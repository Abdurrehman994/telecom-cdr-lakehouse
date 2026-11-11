"""A sink end to end on a real Iceberg table (SQLite catalog, local files); Kafka is faked."""
import json
from datetime import datetime, timezone

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry import Schema
from dataplatform import Encoder, subject_for
from dataplatform.testing import registry_with
from pyiceberg.catalog.sql import SqlCatalog

from catalog import stored_offsets
from conftest import LAKEHOUSE, FakeMessage, FakeProducer, contract, contract_json
from maintenance import compact
from sink_config import load_sink
from writer import LakehouseWriter

CDR_SINK = load_sink(str(LAKEHOUSE / "sinks" / "cdr_events.yaml"))
DECISIONS_SINK = load_sink(str(LAKEHOUSE / "sinks" / "fraud_decisions.yaml"))


class RecordingConsumer:
    def __init__(self):
        self.commits = []
        self.assigned = None

    def commit(self, offsets, asynchronous):
        self.commits.append({tp.partition: tp.offset for tp in offsets})

    def assign(self, partitions):
        self.assigned = [(p.partition, p.offset) for p in partitions]


def cdr(n: int, **overrides):
    return dict({
        "transaction_id": f"TXN-{n}", "customer_id": f"CUST-{n % 3}", "full_name": "A B", "city": "Oulu",
        "mobile_number": "+358401234567", "sim_serial_number": "SIM1", "sim_status": "active",
        "imei": "356938035643809", "risk_score": 12.5, "fraud_flag": False, "type": "call",
        "amount_eur": 3.5, "time_stamp": datetime(2026, 1, 1, 12, n % 60, tzinfo=timezone.utc),
        "processed_timestamp": datetime(2026, 1, 1, 13, tzinfo=timezone.utc),
    }, **overrides)


@pytest.fixture
def catalog(tmp_path):
    return SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/warehouse")


@pytest.fixture
def registry():
    return registry_with({"cdr-events": contract("cdr-events"), "fraud-decisions": contract("fraud-decisions")})


def make_writer(sink, registry, catalog):
    return LakehouseWriter(sink, registry, catalog, RecordingConsumer(), FakeProducer())


def rows(catalog, sink):
    return catalog.load_table(sink.table).scan().to_arrow().sort_by("kafka_offset").to_pylist()


def test_contract_defines_the_table_and_messages_land_exactly_once(registry, catalog):
    w = make_writer(CDR_SINK, registry, catalog)
    encoder = Encoder(registry, "cdr-events", contract("cdr-events"))
    for offset in range(3):
        w.buffer_message(FakeMessage(encoder.encode(cdr(offset)), partition=0, offset=offset))

    w.flush()

    table = catalog.load_table(CDR_SINK.table)
    assert [f.name for f in table.schema().fields] == (
        [f["name"] for f in contract_json("cdr-events")["fields"]] + ["kafka_partition", "kafka_offset", "ingested_at"])
    assert str(table.spec().fields[0].transform) == "day" and table.spec().fields[0].name == "event_day"
    assert [r["transaction_id"] for r in rows(catalog, CDR_SINK)] == ["TXN-0", "TXN-1", "TXN-2"]
    assert rows(catalog, CDR_SINK)[0]["time_stamp"] == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert stored_offsets(table, CDR_SINK.offsets_property) == {"0": 3}
    assert w.consumer.commits == [{0: 3}]


def test_a_new_contract_version_adds_its_fields_as_columns(registry, catalog):
    w = make_writer(CDR_SINK, registry, catalog)
    v1 = Encoder(registry, "cdr-events", contract("cdr-events"))
    w.buffer_message(FakeMessage(v1.encode(cdr(0)), offset=0))
    w.flush()

    v2 = contract_json("cdr-events")
    v2["fields"].append({"name": "roaming", "type": ["null", "boolean"], "default": None})
    registry.register_schema(subject_for("cdr-events"), Schema(json.dumps(v2), "AVRO"))
    w.buffer_message(FakeMessage(Encoder(registry, "cdr-events", json.dumps(v2)).encode(cdr(1, roaming=True)),
                                 offset=1))
    # a producer still on v1 keeps working
    w.buffer_message(FakeMessage(v1.encode(cdr(2)), offset=2))
    w.flush()

    assert "roaming" in [f.name for f in catalog.load_table(CDR_SINK.table).schema().fields]
    assert [r["roaming"] for r in rows(catalog, CDR_SINK)] == [None, True, None]


def test_json_from_before_the_contract_is_converted_to_column_types(registry, catalog):
    w = make_writer(CDR_SINK, registry, catalog)
    legacy = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in cdr(0).items()}
    legacy.update(amount_eur=7, time_stamp="2026-01-01T12:00:00")  # int amount, naive timestamp

    w.buffer_message(FakeMessage(json.dumps(legacy).encode(), offset=0))
    w.flush()

    row = rows(catalog, CDR_SINK)[0]
    assert row["amount_eur"] == 7.0
    assert row["time_stamp"] == datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    assert row["fraud_type"] is None


def test_messages_that_cannot_be_written_go_to_the_dead_letter_topic(registry, catalog):
    w = make_writer(CDR_SINK, registry, catalog)
    good = Encoder(registry, "cdr-events", contract("cdr-events")).encode(cdr(2))

    w.buffer_message(FakeMessage(b"garbage", offset=0))
    w.buffer_message(FakeMessage(json.dumps({"transaction_id": "T", "amount_eur": "lots"}).encode(), offset=1))
    w.buffer_message(FakeMessage(good, offset=2))
    w.flush()

    dead = w.dlq_producer.produced
    assert [m["topic"] for m in dead] == ["cdr-events-dlq", "cdr-events-dlq"]
    assert [m["headers"]["source_offset"] for m in dead] == ["0", "1"]
    assert dead[0]["headers"]["source"] == "lakehouse-writer/cdr_events"
    assert [r["transaction_id"] for r in rows(catalog, CDR_SINK)] == ["TXN-2"]
    assert stored_offsets(catalog.load_table(CDR_SINK.table), CDR_SINK.offsets_property) == {"0": 3}


def test_a_restarted_writer_resumes_after_the_rows_in_iceberg(registry, catalog):
    first = make_writer(CDR_SINK, registry, catalog)
    encoder = Encoder(registry, "cdr-events", contract("cdr-events"))
    for partition, offset in ((0, 0), (0, 1), (2, 7)):
        first.buffer_message(FakeMessage(encoder.encode(cdr(offset)), partition=partition, offset=offset))
    first.flush()

    restarted = make_writer(CDR_SINK, registry, catalog)
    restarted.on_assign(restarted.consumer, [TopicPartition("cdr-events", p) for p in (0, 1, 2)])

    assert restarted.consumer.assigned == [(0, 2), (1, -1001), (2, 8)]


def test_a_second_sink_needs_only_its_contract_and_sink_file(registry, catalog):
    w = make_writer(DECISIONS_SINK, registry, catalog)
    decision = {
        "transaction_id": "TXN-1", "customer_id": "CUST-1", "is_fraud": True, "method": "rules",
        "fraud_type": "sim_swap", "confidence": 1.0, "explanation": "flagged", "amount_eur": 20.0,
        "event_time": datetime(2026, 1, 1, tzinfo=timezone.utc), "decided_at": datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        "detector": "ml-service", "source_partition": 1, "source_offset": 42,
    }

    w.buffer_message(FakeMessage(Encoder(registry, "fraud-decisions", contract("fraud-decisions")).encode(decision)))
    w.flush()

    table = catalog.load_table("telecom.fraud_decisions")
    assert table.spec().fields[0].name == "event_day"
    assert rows(catalog, DECISIONS_SINK)[0]["method"] == "rules"
    assert stored_offsets(table, "kafka.fraud-decisions.offsets") == {"0": 1}


def test_compaction_follows_the_sink_policy_and_keeps_offsets(registry, catalog):
    w = make_writer(CDR_SINK, registry, catalog)
    encoder = Encoder(registry, "cdr-events", contract("cdr-events"))
    for offset in range(6):  # six commits on one day, newest CDR first: six small files
        w.buffer_message(FakeMessage(encoder.encode(cdr(5 - offset)), offset=offset))
        w.flush()

    result = compact(catalog, CDR_SINK)

    table = catalog.load_table(CDR_SINK.table)
    assert result == {"partitions": 1, "files_removed": 6, "files_written": 1}
    assert len(table.inspect.files()) == 1
    timestamps = table.scan().to_arrow()["time_stamp"].to_pylist()
    assert len(timestamps) == 6 and timestamps == sorted(timestamps)  # sorted by the sink's sort_by
    assert stored_offsets(table, CDR_SINK.offsets_property) == {"0": 6}
