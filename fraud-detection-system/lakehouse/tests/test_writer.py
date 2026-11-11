"""Exactly-once behaviour of the lakehouse writer, with Iceberg and Kafka faked."""
import json
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from pyiceberg.exceptions import CommitFailedException

import writer
from catalog import stored_offsets
from conftest import LAKEHOUSE, contract_json
from sink_config import load_sink
from table_schema import RowBuilder, iceberg_schema
from writer import LakehouseWriter, SinkMetrics

SINK = load_sink(str(LAKEHOUSE / "sinks" / "cdr_events.yaml"))
OFFSETS_PROPERTY = SINK.offsets_property
SCHEMA = iceberg_schema(contract_json("cdr-events"))


class FakeTable:
    """Keeps the offsets of the latest snapshot, like the Iceberg snapshot property."""

    def __init__(self, offsets=None, fail_append=False, conflicts=0):
        self.offsets = offsets
        self.fail_append = fail_append
        self.conflicts = conflicts
        self.appends = []

    def refresh(self):
        pass

    def schema(self):
        return SCHEMA

    def current_snapshot(self):
        if self.offsets is None:
            return None
        props = {OFFSETS_PROPERTY: json.dumps(self.offsets)}
        return SimpleNamespace(snapshot_id=len(self.appends), parent_snapshot_id=None,
                               summary=SimpleNamespace(additional_properties=props))

    def append(self, batch, snapshot_properties):
        if self.fail_append:
            raise OSError("S3 unavailable")
        if self.conflicts:
            self.conflicts -= 1
            raise CommitFailedException("Requirement failed: branch main has changed")
        self.appends.append(batch.to_pylist())
        self.offsets = json.loads(snapshot_properties[OFFSETS_PROPERTY])


class FakeConsumer:
    def __init__(self, commit_error=None, during_commit=None):
        self.commit_error = commit_error
        self.during_commit = during_commit
        self.commits = []
        self.assigned = None

    def commit(self, offsets, asynchronous):
        self.commits.append({tp.partition: tp.offset for tp in offsets})
        if self.during_commit is not None:
            # a blocking commit serves callbacks, e.g. on_revoke after a session timeout
            callback, self.during_commit = self.during_commit, None
            callback()
        if self.commit_error is not None:
            raise self.commit_error

    def assign(self, partitions):
        self.assigned = [(p.partition, p.offset) for p in partitions]


def make_writer(table, consumer):
    w = LakehouseWriter.__new__(LakehouseWriter)  # skip __init__: no registry, catalog or broker
    w.sink = SINK
    w.metrics = SinkMetrics(SINK)
    w.table = table
    w.consumer = consumer
    w.rows = RowBuilder(SCHEMA)
    w.buffer = []
    w.pending_offsets = {}
    w.last_flush = time.monotonic()
    w.running = True
    return w


def buffer_message(w, partition, offset):
    """what buffer_message() does for each decoded message"""
    cdr = {"transaction_id": f"T-{partition}-{offset}", "customer_id": "CUST-1",
           "amount_eur": 10, "time_stamp": "2026-01-01T12:00:00Z"}
    w.buffer.append(w.rows.build(cdr, partition, offset, datetime.now(timezone.utc)))
    w.pending_offsets[partition] = offset + 1


def unknown_member():
    return KafkaException(KafkaError(KafkaError.UNKNOWN_MEMBER_ID))


def test_flush_stores_next_offsets_with_the_data():
    table = FakeTable(offsets={"0": 10, "1": 5})
    consumer = FakeConsumer()
    w = make_writer(table, consumer)
    buffer_message(w, 1, 5)
    buffer_message(w, 1, 6)

    w.flush()

    assert [r["transaction_id"] for r in table.appends[0]] == ["T-1-5", "T-1-6"]
    # partition 0 had no new rows and keeps its stored offset
    assert table.offsets == {"0": 10, "1": 7}
    assert consumer.commits == [{1: 7}]
    assert w.buffer == [] and w.pending_offsets == {}


def test_failed_kafka_commit_does_not_crash_or_append_twice():
    """Regression: with the broker down, the blocking commit let the session time out,
    on_revoke flushed the still-full buffer again, and the same rows were appended twice."""
    table = FakeTable(offsets={"2": 227})
    w = make_writer(table, None)
    w.consumer = FakeConsumer(commit_error=unknown_member(), during_commit=lambda: w.on_revoke(w.consumer, []))
    for offset in (227, 228, 229):
        buffer_message(w, 2, offset)

    w.flush()  # must not raise

    assert len(table.appends) == 1
    assert len(table.appends[0]) == 3
    assert table.offsets == {"2": 230}
    assert w.buffer == []


def test_rows_already_in_iceberg_are_not_appended_again():
    table = FakeTable(offsets={"1": 7})
    w = make_writer(table, FakeConsumer())
    for offset in (5, 6, 7, 7):  # 5 and 6 are stored already; 7 is buffered twice
        buffer_message(w, 1, offset)

    w.flush()

    assert [r["kafka_offset"] for r in table.appends[0]] == [7]
    assert table.offsets == {"1": 8}


def test_flush_with_only_stored_rows_appends_nothing():
    table = FakeTable(offsets={"1": 7})
    w = make_writer(table, FakeConsumer())
    buffer_message(w, 1, 6)

    w.flush()

    assert table.appends == []
    assert w.buffer == []


def test_failed_iceberg_append_keeps_the_buffer():
    """If the data did not reach Iceberg, nothing may be dropped or marked as written."""
    table = FakeTable(offsets={"1": 5}, fail_append=True)
    consumer = FakeConsumer()
    w = make_writer(table, consumer)
    buffer_message(w, 1, 5)

    with pytest.raises(OSError):
        w.flush()

    assert len(w.buffer) == 1
    assert w.pending_offsets == {1: 6}
    assert consumer.commits == []


def test_append_is_retried_after_losing_a_race_with_compaction():
    table = FakeTable(offsets={"1": 5}, conflicts=2)
    w = make_writer(table, FakeConsumer())
    buffer_message(w, 1, 5)
    buffer_message(w, 1, 6)

    w.flush()

    assert len(table.appends) == 1
    assert len(table.appends[0]) == 2
    assert table.offsets == {"1": 7}


def test_append_gives_up_after_repeated_conflicts_and_keeps_the_buffer():
    table = FakeTable(offsets={"1": 5}, conflicts=writer.COMMIT_ATTEMPTS)
    w = make_writer(table, FakeConsumer())
    buffer_message(w, 1, 5)

    with pytest.raises(CommitFailedException):
        w.flush()

    assert table.appends == []
    assert len(w.buffer) == 1


def test_stored_offsets_come_from_the_newest_snapshot_that_has_them():
    """a snapshot made by another tool may lack the property; walk back to its parent"""
    with_offsets = SimpleNamespace(snapshot_id=1, parent_snapshot_id=None,
                                   summary=SimpleNamespace(additional_properties={OFFSETS_PROPERTY: '{"0": 42}'}))
    without = SimpleNamespace(snapshot_id=2, parent_snapshot_id=1,
                              summary=SimpleNamespace(additional_properties={"operation": "overwrite"}))
    table = SimpleNamespace(current_snapshot=lambda: without,
                            snapshot_by_id=lambda sid: {1: with_offsets, 2: without}[sid])

    assert stored_offsets(table, OFFSETS_PROPERTY) == {"0": 42}


def test_on_assign_seeks_to_the_offsets_stored_in_iceberg():
    table = FakeTable(offsets={"0": 248, "2": 230})
    consumer = FakeConsumer(commit_error=unknown_member())
    w = make_writer(table, consumer)
    partitions = [TopicPartition(SINK.topic, p) for p in (0, 1, 2)]

    w.on_assign(consumer, partitions)  # a failing lag commit must not stop the assignment

    # partition 1 has nothing in Iceberg yet, so Kafka's own position is used
    assert consumer.assigned[0] == (0, 248)
    assert consumer.assigned[2] == (2, 230)
    assert consumer.commits == [{0: 248, 2: 230}]


def test_env_overrides_the_sink_batch_size(monkeypatch):
    monkeypatch.setenv("FLUSH_MAX_RECORDS", "5000")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "")

    sink = writer.with_env_overrides(SINK)

    assert sink.flush_max_records == 5000
    assert sink.flush_interval_seconds == SINK.flush_interval_seconds
