"""KafkaPublisher reports success only once the broker has confirmed a CDR."""
import json
from datetime import datetime, timezone

from app.services import kafka_publisher
from app.services.kafka_publisher import KafkaPublisher


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


def make_publisher(producer):
    publisher = KafkaPublisher(topic="cdr-events", bootstrap_servers="unused:9092")
    publisher.producer = producer
    return publisher


def test_publish_returns_none_once_delivered():
    producer = FakeProducer()
    cdr = {"transaction_id": "T1", "customer_id": "CUST-1",
           "time_stamp": datetime(2026, 1, 1, tzinfo=timezone.utc)}

    assert make_publisher(producer).publish(cdr) is None

    sent = producer.messages[0]
    assert sent["topic"] == "cdr-events"
    # keyed by customer so one customer's CDRs stay in one partition, in order
    assert sent["key"] == b"CUST-1"
    assert json.loads(sent["value"])["time_stamp"] == "2026-01-01T00:00:00+00:00"


def test_publish_returns_the_broker_error():
    producer = FakeProducer(delivery_error="Broker: Not enough in-sync replicas")

    assert make_publisher(producer).publish({"transaction_id": "T1"}) == "Broker: Not enough in-sync replicas"


def test_publish_returns_error_when_local_queue_is_full():
    producer = FakeProducer(produce_error=BufferError("Local: Queue full"))

    assert "Queue full" in make_publisher(producer).publish({"transaction_id": "T1"})


def test_publish_times_out_when_the_broker_never_answers(monkeypatch):
    monkeypatch.setattr(kafka_publisher, "DELIVERY_TIMEOUT_MS", 0)
    monkeypatch.setattr(kafka_publisher, "DELIVERY_WAIT_GRACE_SECONDS", 0.2)

    error = make_publisher(FakeProducer(confirms=False)).publish({"transaction_id": "T1"})

    assert error == "Kafka did not confirm delivery in time"


def test_publish_batch_reports_each_message():
    class OneFailure(FakeProducer):
        def poll(self, timeout):
            for i, callback in enumerate(self._callbacks):
                callback("Broker: Message too large" if i == 1 else None, None)
            self._callbacks = []
            return 0

    errors = make_publisher(OneFailure()).publish_batch(
        [{"transaction_id": "T1"}, {"transaction_id": "T2"}, {"transaction_id": "T3"}]
    )

    assert errors == [None, "Broker: Message too large", None]
