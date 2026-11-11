"""KafkaPublisher reports success only once the broker has confirmed a CDR."""
from datetime import datetime, timezone
from pathlib import Path

from dataplatform import Decoder, Encoder
from dataplatform.testing import InMemoryRegistry, registry_with

from app.services import kafka_publisher
from app.services.kafka_publisher import KafkaPublisher

CONTRACT = (Path(__file__).resolve().parents[2] / "contracts" / "cdr-events-value.avsc").read_text()


def cdr(transaction_id="T1", **overrides):
    """an enriched CDR, as the API hands it to the publisher"""
    return {
        "transaction_id": transaction_id, "customer_id": "CUST-1", "full_name": "John Doe", "city": "Helsinki",
        "mobile_number": "+358123456789", "sim_serial_number": "SIM1234567890", "sim_status": "active",
        "imei": "356938035643809", "risk_score": 25.5, "fraud_flag": False, "type": "payment",
        "amount_eur": 150.0, "time_stamp": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "processed_timestamp": datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc), **overrides,
    }


class FakeProducer:
    """Stands in for confluent_kafka.Producer; poll() answers queued deliveries."""

    def __init__(self, delivery_error=None, confirms=True, produce_error=None):
        self.delivery_error = delivery_error
        self.confirms = confirms
        self.produce_error = produce_error
        self.messages = []
        self._callbacks = []

    def produce(self, topic, key, value, on_delivery):
        if self.produce_error is not None:
            raise self.produce_error
        self.messages.append({"topic": topic, "key": key, "value": value})
        self._callbacks.append(on_delivery)

    def poll(self, timeout):
        if self.confirms:
            while self._callbacks:
                self._callbacks.pop(0)(self.delivery_error, None)
        return 0


def make_publisher(producer, registry=None):
    registry = registry or registry_with({"cdr-events": CONTRACT})
    publisher = KafkaPublisher(topic="cdr-events", bootstrap_servers="unused:9092",
                               encoder=Encoder(registry, "cdr-events", CONTRACT))
    publisher.producer = producer
    return publisher


def test_publish_sends_avro_with_the_registered_contract():
    registry = registry_with({"cdr-events": CONTRACT})
    producer = FakeProducer()

    assert make_publisher(producer, registry).publish(cdr()) is None

    sent = producer.messages[0]
    assert sent["topic"] == "cdr-events"
    # keyed by customer so one customer's CDRs stay in one partition, in order
    assert sent["key"] == b"CUST-1"
    decoded = Decoder(registry).decode(sent["value"])
    assert decoded.schema_id == 1
    assert decoded.value["time_stamp"] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert decoded.value["fraud_type"] is None


def test_what_the_api_publishes_matches_the_contract():
    """the request model and the Avro contract must not drift apart"""
    from app.processors.cdr_transformer import enrich_cdr
    from app.schemas import CDRRecord

    request = CDRRecord(**{k: v.isoformat() if isinstance(v, datetime) else v
                           for k, v in cdr().items() if k != "processed_timestamp"})

    assert make_publisher(FakeProducer()).publish(enrich_cdr(request)) is None


def test_a_cdr_that_breaks_the_contract_is_not_sent():
    producer = FakeProducer()

    error = make_publisher(producer).publish(cdr(amount_eur="a lot"))

    assert "does not match the cdr-events contract" in error
    assert producer.messages == []


def test_nothing_is_sent_while_the_contract_is_not_registered():
    producer = FakeProducer()

    error = make_publisher(producer, registry=InMemoryRegistry()).publish(cdr())

    assert error.startswith("Could not encode CDR")
    assert producer.messages == []


def test_publish_returns_the_broker_error():
    producer = FakeProducer(delivery_error="Broker: Not enough in-sync replicas")

    assert make_publisher(producer).publish(cdr()) == "Broker: Not enough in-sync replicas"


def test_publish_returns_error_when_local_queue_is_full():
    producer = FakeProducer(produce_error=BufferError("Local: Queue full"))

    assert "Queue full" in make_publisher(producer).publish(cdr())


def test_publish_times_out_when_the_broker_never_answers(monkeypatch):
    monkeypatch.setattr(kafka_publisher, "DELIVERY_TIMEOUT_MS", 0)
    monkeypatch.setattr(kafka_publisher, "DELIVERY_WAIT_GRACE_SECONDS", 0.2)

    error = make_publisher(FakeProducer(confirms=False)).publish(cdr())

    assert error == "Kafka did not confirm delivery in time"


def test_publish_batch_reports_each_message():
    class OneFailure(FakeProducer):
        def poll(self, timeout):
            for i, callback in enumerate(self._callbacks):
                callback("Broker: Message too large" if i == 1 else None, None)
            self._callbacks = []
            return 0

    errors = make_publisher(OneFailure()).publish_batch([cdr("T1"), cdr("T2"), cdr("T3")])

    assert errors == [None, "Broker: Message too large", None]
