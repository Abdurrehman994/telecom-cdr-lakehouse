"""At-least-once processing, idempotent alerts and decision events in the fraud-detection consumer."""
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from dataplatform import Decoder, Encoder
from dataplatform.testing import registry_with

import ml_service
from ml_service import AlertNotifier, CDRConsumer, FraudDetectionEngine

CONTRACTS = Path(__file__).resolve().parents[2] / "contracts"
CDR_CONTRACT = (CONTRACTS / "cdr-events-value.avsc").read_text()
DECISION_CONTRACT = (CONTRACTS / "fraud-decisions-value.avsc").read_text()

FRAUD = {"is_fraud": True, "confidence": 1.0, "method": "rules",
         "fraud_type": "SIM_SWAP", "explanation": "Record marked as fraudulent"}
CLEAN = {"is_fraud": False, "confidence": 0.9, "method": "random_forest"}


def cdr(transaction_id="T1", **overrides):
    return {"transaction_id": transaction_id, "customer_id": "CUST-1", "amount_eur": 25.0,
            "time_stamp": "2026-01-01T12:00:00+00:00", **overrides}


class FakeMessage:
    def __init__(self, value, partition=0, offset=42):
        self._value = value if isinstance(value, bytes) else json.dumps(value).encode()
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def key(self):
        return b"CUST-1"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class FakeKafkaConsumer:
    def __init__(self):
        self.stored = []

    def store_offsets(self, message):
        self.stored.append(message.offset())


class FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key, value, headers):
        self.produced.append({"topic": topic, "value": value, "headers": headers})

    def flush(self, timeout):
        return 0


class FakeDecisionsProducer:
    """confirms each decision on flush(), after failing the first `failures` deliveries"""

    def __init__(self, failures=0):
        self.failures = failures
        self.delivered = []
        self._pending = []

    def produce(self, topic, key, value, on_delivery):
        self._pending.append((topic, key, value, on_delivery))

    def flush(self, timeout):
        for topic, key, value, on_delivery in self._pending:
            if self.failures:
                self.failures -= 1
                on_delivery("Broker: Not enough in-sync replicas", None)
            else:
                self.delivered.append({"topic": topic, "key": key, "value": value})
                on_delivery(None, None)
        self._pending = []
        return 0


class FakeAlertStore:
    """save() fails with the given errors first, then reports whether the alert is new."""

    def __init__(self, existing=(), failures=()):
        self.rows = set(existing)
        self.failures = list(failures)
        self.attempts = 0

    def save(self, cdr, result, partition, offset):
        self.attempts += 1
        if self.failures:
            raise self.failures.pop(0)
        is_new = cdr["transaction_id"] not in self.rows
        self.rows.add(cdr["transaction_id"])
        return is_new


@pytest.fixture
def registry():
    return registry_with({"cdr-events": CDR_CONTRACT, "fraud-decisions": DECISION_CONTRACT})


@pytest.fixture
def consumer(monkeypatch, registry):
    c = CDRConsumer(topic="cdr-events", dlq_topic="cdr-events-dlq", decisions_topic="fraud-decisions")
    c.consumer = FakeKafkaConsumer()
    c.dlq_producer = FakeProducer()
    c.decisions_producer = FakeDecisionsProducer()
    c.decoder = Decoder(registry, allow_json=True)
    c.decision_encoder = Encoder(registry, "fraud-decisions", DECISION_CONTRACT)
    c.running = True
    c.alerts_sent = []
    monkeypatch.setattr(ml_service, "notifier", SimpleNamespace(notify=lambda cdr, result: c.alerts_sent.append(cdr)))
    monkeypatch.setattr(ml_service.time, "sleep", lambda seconds: None)
    return c


def use(monkeypatch, store, decision=FRAUD):
    monkeypatch.setattr(ml_service, "alert_store", store)
    monkeypatch.setattr(ml_service.detector, "detect_fraud", lambda cdr: decision)


def decisions(consumer, registry):
    return [Decoder(registry).decode(d["value"]).value for d in consumer.decisions_producer.delivered]


def test_new_fraud_is_stored_alerted_and_marked_done(consumer, monkeypatch):
    store = FakeAlertStore()
    use(monkeypatch, store)

    consumer.process_message(FakeMessage(cdr("T1")))

    assert store.rows == {"T1"}
    assert [a["transaction_id"] for a in consumer.alerts_sent] == ["T1"]
    assert consumer.consumer.stored == [42]


def test_redelivered_cdr_does_not_alert_again(consumer, monkeypatch):
    use(monkeypatch, FakeAlertStore(existing={"T1"}))

    consumer.process_message(FakeMessage(cdr("T1")))

    assert consumer.alerts_sent == []
    assert consumer.consumer.stored == [42]


def test_clean_cdr_is_marked_done_without_an_alert(consumer, monkeypatch):
    store = FakeAlertStore()
    use(monkeypatch, store, decision=CLEAN)

    consumer.process_message(FakeMessage(cdr("T1")))

    assert store.attempts == 0
    assert consumer.consumer.stored == [42]


def test_every_decision_is_published_with_its_source(consumer, monkeypatch, registry):
    use(monkeypatch, FakeAlertStore(), decision=FRAUD)
    consumer.process_message(FakeMessage(cdr("T1"), partition=2, offset=7))
    use(monkeypatch, FakeAlertStore(), decision=CLEAN)
    consumer.process_message(FakeMessage(cdr("T2"), partition=2, offset=8))

    fraud, clean = decisions(consumer, registry)
    assert consumer.decisions_producer.delivered[0]["topic"] == "fraud-decisions"
    assert consumer.decisions_producer.delivered[0]["key"] == b"CUST-1"
    assert (fraud["transaction_id"], fraud["is_fraud"], fraud["method"], fraud["fraud_type"]) == \
        ("T1", True, "rules", "SIM_SWAP")
    assert (fraud["source_partition"], fraud["source_offset"]) == (2, 7)
    assert fraud["event_time"] == datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    assert (clean["is_fraud"], clean["fraud_type"], clean["confidence"]) == (False, None, 0.9)


def test_llm_answers_that_are_not_numbers_are_published_without_confidence(consumer, monkeypatch, registry):
    use(monkeypatch, FakeAlertStore(), decision={"is_fraud": False, "confidence": "high", "method": "llm"})

    consumer.process_message(FakeMessage(cdr("T1")))

    decision = decisions(consumer, registry)[0]
    assert (decision["method"], decision["confidence"]) == ("llm", None)


def test_avro_cdrs_reach_the_detector_as_json_values(consumer, monkeypatch, registry):
    seen = []
    use(monkeypatch, FakeAlertStore(), decision=CLEAN)
    monkeypatch.setattr(ml_service.detector, "detect_fraud", lambda c: seen.append(c) or CLEAN)
    full_cdr = {
        "transaction_id": "T1", "customer_id": "CUST-1", "full_name": "A B", "city": "Oulu",
        "mobile_number": "+358401234567", "sim_serial_number": "SIM1", "sim_status": "active",
        "imei": "356938035643809", "risk_score": 12.5, "fraud_flag": False, "type": "call", "amount_eur": 3.5,
        "time_stamp": datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
        "processed_timestamp": datetime(2026, 1, 1, 13, tzinfo=timezone.utc),
    }

    consumer.process_message(FakeMessage(Encoder(registry, "cdr-events", CDR_CONTRACT).encode(full_cdr)))

    # like the JSON cdr-ingestion used to send: ISO timestamps, no empty fields
    assert seen[0]["time_stamp"] == "2026-01-01T12:00:00+00:00"
    assert "fraud_type" not in seen[0]
    assert consumer.consumer.stored == [42]


def test_broker_failure_on_the_decision_is_retried(consumer, monkeypatch, registry):
    use(monkeypatch, FakeAlertStore(), decision=CLEAN)
    consumer.decisions_producer = FakeDecisionsProducer(failures=2)

    consumer.process_message(FakeMessage(cdr("T1")))

    assert len(decisions(consumer, registry)) == 1
    assert consumer.dlq_producer.produced == []
    assert consumer.consumer.stored == [42]


def test_stopping_before_the_decision_is_stored_leaves_the_offset_unstored(consumer, monkeypatch):
    use(monkeypatch, FakeAlertStore(), decision=CLEAN)
    consumer.decisions_producer = FakeDecisionsProducer(failures=10)
    monkeypatch.setattr(ml_service.time, "sleep", lambda seconds: consumer.stop())

    consumer.process_message(FakeMessage(cdr("T1")))

    assert consumer.consumer.stored == []
    assert consumer.dlq_producer.produced == []


def test_registry_outage_while_decoding_is_retried_not_sent_to_the_dlq(consumer, monkeypatch, registry):
    use(monkeypatch, FakeAlertStore(), decision=CLEAN)
    message = FakeMessage(Encoder(registry, "cdr-events", CDR_CONTRACT).encode({
        "transaction_id": "T1", "customer_id": "CUST-1", "full_name": "A B", "city": "Oulu",
        "mobile_number": "+358401234567", "sim_serial_number": "SIM1", "sim_status": "active",
        "imei": "356938035643809", "risk_score": 12.5, "fraud_flag": False, "type": "call", "amount_eur": 3.5,
        "time_stamp": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "processed_timestamp": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }))
    outages = [SchemaRegistryError(503, 50300, "unavailable")] * 2
    real_get_schema = registry.get_schema

    def get_schema(schema_id):
        if outages:
            raise outages.pop()
        return real_get_schema(schema_id)

    monkeypatch.setattr(registry, "get_schema", get_schema)

    consumer.process_message(message)

    assert outages == []
    assert consumer.dlq_producer.produced == []
    assert consumer.consumer.stored == [42]


def test_postgres_outage_is_retried_not_sent_to_the_dlq(consumer, monkeypatch):
    down = psycopg.OperationalError("connection refused")
    store = FakeAlertStore(failures=[down, down])
    use(monkeypatch, store)

    consumer.process_message(FakeMessage(cdr("T1")))

    assert store.attempts == 3
    assert len(consumer.alerts_sent) == 1
    assert consumer.dlq_producer.produced == []
    assert consumer.consumer.stored == [42]


def test_stopping_during_an_outage_leaves_the_offset_unstored(consumer, monkeypatch):
    """the CDR must be read again after restart, not skipped"""
    store = FakeAlertStore(failures=[psycopg.OperationalError("down")] * 10)
    use(monkeypatch, store)
    monkeypatch.setattr(ml_service.time, "sleep", lambda seconds: consumer.stop())

    consumer.process_message(FakeMessage(cdr("T1")))

    assert consumer.consumer.stored == []
    assert consumer.alerts_sent == []
    assert consumer.dlq_producer.produced == []


def test_malformed_message_goes_to_the_dlq_and_the_partition_moves_on(consumer, monkeypatch):
    use(monkeypatch, FakeAlertStore())

    consumer.process_message(FakeMessage(b"not json", partition=2, offset=7))

    dead = consumer.dlq_producer.produced[0]
    assert dead["topic"] == "cdr-events-dlq"
    assert dead["value"] == b"not json"
    assert dead["headers"]["source_partition"] == "2"
    assert dead["headers"]["source_offset"] == "7"
    assert consumer.consumer.stored == [7]


@pytest.mark.parametrize("cdr, fraud_type", [
    ({"fraud_flag": True, "fraud_type": "SIM_SWAP"}, "SIM_SWAP"),
    ({"risk_score": 0.95}, "high_risk_score"),
    ({"mobile_number": "+1234567890"}, "blacklisted_number"),
    ({"sim_status": "Blocked"}, "suspicious_sim_status"),
    ({"amount_eur": 15000}, "high_value_transaction"),
])
def test_rules_flag_known_fraud_patterns(cdr, fraud_type):
    result = FraudDetectionEngine().apply_rules(cdr)

    assert result["is_fraud"] is True
    assert result["type"] == fraud_type


def test_rules_pass_an_ordinary_cdr():
    cdr = {"risk_score": 0.2, "mobile_number": "+358401234567", "sim_status": "active", "amount_eur": 30}

    assert FraudDetectionEngine().apply_rules(cdr) == {"is_fraud": False}


def test_a_slow_alertmanager_does_not_hold_up_detection():
    """Regression: without alertmanager, waiting on each notification cut throughput 14-fold."""
    release = threading.Event()
    sent = []
    def slow_send(cdr, result):
        release.wait(5)
        sent.append(cdr["transaction_id"])

    notifier = AlertNotifier(send=slow_send)
    notifier.start()

    started = time.monotonic()
    for n in range(20):
        notifier.notify({"transaction_id": f"T{n}"}, FRAUD)
    assert time.monotonic() - started < 0.5

    release.set()
    notifier.queue.join()
    assert sent == [f"T{n}" for n in range(20)]
    notifier.stop()


def test_notifications_beyond_the_queue_are_dropped_and_counted():
    notifier = AlertNotifier(send=lambda cdr, result: None, max_queued=2)  # not started: nothing drains
    dropped = ml_service.alert_notifications.labels(result="dropped")
    before = dropped._value.get()

    for n in range(5):
        notifier.notify({"transaction_id": f"T{n}"}, FRAUD)

    assert notifier.queue.qsize() == 2
    assert dropped._value.get() == before + 3


class FakePool:
    """a psycopg pool that is opened once and becomes ready after some waits"""

    def __init__(self, timeouts):
        self.timeouts = timeouts
        self.opened = 0

    def open(self):
        if self.opened:
            raise psycopg.OperationalError("pool has already been opened/closed and cannot be reused")
        self.opened += 1

    def wait(self, timeout):
        if self.timeouts:
            self.timeouts -= 1
            raise ml_service.PoolTimeout("pool initialization incomplete after 10 sec")


def test_a_late_database_is_waited_for_without_reopening_the_pool():
    """Regression: a second open() raised and silently stopped the consumer when postgres started late."""
    store = ml_service.AlertStore.__new__(ml_service.AlertStore)
    store.pool = FakePool(timeouts=3)

    store.open()

    assert store.pool.opened == 1 and store.pool.timeouts == 0


def test_health_fails_once_the_consumer_thread_has_stopped(monkeypatch):
    from fastapi.testclient import TestClient

    client = TestClient(ml_service.app)
    stopped = threading.Thread(target=lambda: None)
    stopped.start()
    stopped.join()

    monkeypatch.setattr(ml_service, "consumer_thread", None)
    assert client.get("/health").status_code == 200
    monkeypatch.setattr(ml_service, "consumer_thread", stopped)
    assert client.get("/health").status_code == 503

