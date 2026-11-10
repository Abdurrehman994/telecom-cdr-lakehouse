"""Kafka Publisher Service for CDR ingestion"""
import json
import logging
import threading
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from confluent_kafka import Producer, KafkaException
from app.config import settings


logger = logging.getLogger(__name__)

# How long the producer keeps retrying a message before reporting it as failed
DELIVERY_TIMEOUT_MS = 10000


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class KafkaPublisher:
    """Handles publishing of CDR records to a Kafka topic"""

    def __init__(self, topic: str = None, bootstrap_servers: str = None):
        """Initialize Kafka Publisher"""
        self.topic = topic or settings.kafka_topic
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.producer: Optional[Producer] = None

        logger.info(f"Kafka Publisher initialized for topic: {self.topic}")

    def init_connection(self) -> bool:
        """Create the producer and check that the broker and topic are reachable"""
        try:
            self.producer = Producer({
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": settings.service_name,
                # Wait for the broker to persist the write before confirming it
                "acks": "all",
                # Retries cannot create duplicates or reorder messages
                "enable.idempotence": True,
                # Wait up to 10 ms to batch messages together
                "linger.ms": 10,
                # Fail fast so the API can answer 503 instead of hanging for the 5 minute default
                "delivery.timeout.ms": DELIVERY_TIMEOUT_MS,
            })
            metadata = self.producer.list_topics(topic=self.topic, timeout=5)
            if metadata.topics[self.topic].error is not None:
                logger.error(f"Kafka topic {self.topic} is not available: {metadata.topics[self.topic].error}")
                return False

            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            return True

        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            return False

    def _produce(self, message: Dict[str, Any], outcome: Dict[str, Optional[str]]) -> threading.Event:
        """Queue a CDR for sending; the returned event is set once the broker confirms or rejects it"""
        done = threading.Event()

        def on_delivery(err, msg):
            outcome["error"] = str(err) if err is not None else None
            done.set()

        # Same key -> same partition, so each customer's CDRs stay in order
        key = str(message.get("customer_id", ""))
        self.producer.produce(
            self.topic,
            key=key.encode("utf-8"),
            value=json.dumps(message, cls=DateTimeEncoder).encode("utf-8"),
            on_delivery=on_delivery,
        )
        return done

    def publish_batch(self, messages: List[Dict[str, Any]]) -> List[Optional[str]]:
        """Publish CDRs and wait until the broker has stored each one.

        Returns one entry per message: None if it was delivered, otherwise the error.
        Blocks for up to the delivery timeout, so call it from a worker thread.
        """
        if self.producer is None and not self.init_connection():
            return ["Kafka is unavailable"] * len(messages)

        outcomes = [{"error": "Kafka did not confirm delivery in time"} for _ in messages]
        pending = []
        for message, outcome in zip(messages, outcomes):
            try:
                pending.append(self._produce(message, outcome))
            except (KafkaException, BufferError) as e:
                outcome["error"] = str(e)

        # The producer gives up after delivery.timeout.ms; the extra seconds cover callback scheduling
        deadline = time.monotonic() + DELIVERY_TIMEOUT_MS / 1000 + 5
        for done in pending:
            while not done.is_set() and time.monotonic() < deadline:
                self.producer.poll(0.1)

        errors = [outcome["error"] for outcome in outcomes]
        for message, error in zip(messages, errors):
            if error is None:
                logger.info(f"Published CDR {message.get('transaction_id')} to {self.topic}")
            else:
                logger.error(f"Failed to publish CDR {message.get('transaction_id')} to Kafka: {error}")
        return errors

    def publish(self, message: Dict[str, Any]) -> Optional[str]:
        """Publish one CDR and wait for the broker; returns None on success, otherwise the error"""
        return self.publish_batch([message])[0]

    def close_connection(self) -> None:
        """Send any buffered messages before shutting down"""
        if self.producer is not None:
            remaining = self.producer.flush(10)
            if remaining:
                logger.warning(f"{remaining} Kafka messages were not delivered before shutdown")
            logger.info("Kafka producer closed")

    def health_check(self) -> bool:
        """Check if the Kafka broker is reachable"""
        try:
            return self.producer is not None and bool(self.producer.list_topics(timeout=2).brokers)
        except KafkaException:
            return False
