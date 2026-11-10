"""Kafka Publisher Service for CDR ingestion"""
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from confluent_kafka import Producer, KafkaException
from app.config import settings


logger = logging.getLogger(__name__)


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

    def _on_delivery(self, err, msg) -> None:
        """Called by the producer once the broker confirms or rejects a message"""
        if err is not None:
            logger.error(f"Kafka delivery failed for key {msg.key()}: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} partition {msg.partition()} offset {msg.offset()}")

    def publish(self, message: Dict[str, Any]) -> bool:
        """Publish a CDR record to Kafka, keyed by customer_id"""
        if self.producer is None and not self.init_connection():
            return False

        try:
            # Same key -> same partition, so each customer's CDRs stay in order
            key = str(message.get("customer_id", ""))
            self.producer.produce(
                self.topic,
                key=key.encode("utf-8"),
                value=json.dumps(message, cls=DateTimeEncoder).encode("utf-8"),
                on_delivery=self._on_delivery,
            )
            # Serve delivery callbacks for earlier messages without blocking
            self.producer.poll(0)

            logger.info(f"Published CDR {message.get('transaction_id')} to {self.topic}")
            return True

        except (KafkaException, BufferError) as e:
            logger.error(f"Failed to publish message to Kafka: {str(e)}")
            return False

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
