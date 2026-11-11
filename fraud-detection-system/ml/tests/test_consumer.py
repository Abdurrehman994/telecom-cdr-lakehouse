"""At-least-once processing and idempotent alerts in the fraud-detection consumer."""
import json

import psycopg
import pytest

import ml_service
from ml_service import CDRConsumer, FraudDetectionEngine

FRAUD = {"is_fraud": True, "confidence": 1.0, "method": "rules",
         "fraud_type": "SIM_SWAP", "explanation": "Record marked as fraudulent"}
CLEAN = {"is_fraud": False, "confidence": 0.9}


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
def consumer(monkeypatch):
    c = CDRConsumer(topic="cdr-events", dlq_topic="cdr-events-dlq")
    c.consumer = FakeKafkaConsumer()
    c.dlq_producer = FakeProducer()
    c.running = True
    c.alerts_sent = []
    monkeypatch.setattr(ml_service, "send_alert_to_prometheus", lambda cdr, result: c.alerts_sent.append(cdr))
    monkeypatch.setattr(ml_service.time, "sleep", lambda seconds: None)
    return c


def use(monkeypatch, store, decision=FRAUD):
    monkeypatch.setattr(ml_service, "alert_store", store)
    monkeypatch.setattr(ml_service.detector, "detect_fraud", lambda cdr: decision)


def test_new_fraud_is_stored_alerted_and_marked_done(consumer, monkeypatch):
    store = FakeAlertStore()
    use(monkeypatch, store)

    consumer.process_message(FakeMessage({"transaction_id": "T1"}))

    assert store.rows == {"T1"}
    assert [a["transaction_id"] for a in consumer.alerts_sent] == ["T1"]
    assert consumer.consumer.stored == [42]


def test_redelivered_cdr_does_not_alert_again(consumer, monkeypatch):
    use(monkeypatch, FakeAlertStore(existing={"T1"}))

    consumer.process_message(FakeMessage({"transaction_id": "T1"}))

    assert consumer.alerts_sent == []
    assert consumer.consumer.stored == [42]


def test_clean_cdr_is_marked_done_without_an_alert(consumer, monkeypatch):
    store = FakeAlertStore()
    use(monkeypatch, store, decision=CLEAN)

    consumer.process_message(FakeMessage({"transaction_id": "T1"}))

    assert store.attempts == 0
    assert consumer.consumer.stored == [42]


def test_postgres_outage_is_retried_not_sent_to_the_dlq(consumer, monkeypatch):
    down = psycopg.OperationalError("connection refused")
    store = FakeAlertStore(failures=[down, down])
    use(monkeypatch, store)

    consumer.process_message(FakeMessage({"transaction_id": "T1"}))

    assert store.attempts == 3
    assert len(consumer.alerts_sent) == 1
    assert consumer.dlq_producer.produced == []
    assert consumer.consumer.stored == [42]


def test_stopping_during_an_outage_leaves_the_offset_unstored(consumer, monkeypatch):
    """the CDR must be read again after restart, not skipped"""
    store = FakeAlertStore(failures=[psycopg.OperationalError("down")] * 10)
    use(monkeypatch, store)
    monkeypatch.setattr(ml_service.time, "sleep", lambda seconds: consumer.stop())

    consumer.process_message(FakeMessage({"transaction_id": "T1"}))

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
