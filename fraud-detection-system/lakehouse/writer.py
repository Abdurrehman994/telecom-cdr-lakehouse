"""Lakehouse writer: streams CDRs from Kafka into an Apache Iceberg table.

Reads the cdr-events topic as its own consumer group, so it gets every CDR
independently of fraud detection. CDRs are buffered and appended to Iceberg
in batches, because each append creates a snapshot and new Parquet files;
appending one row at a time would leave thousands of tiny files.

Exactly-once: the Kafka offsets of each batch are written into the Iceberg
snapshot properties in the same commit as the data. On startup and on every
partition assignment the writer seeks to the offsets stored in the latest
snapshot, so a crash between the Iceberg commit and the Kafka offset commit
cannot produce duplicate rows.
"""
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pyarrow as pa
from pyarrow import fs
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("lakehouse-writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cdr-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "lakehouse-writer")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "cdr-events-dlq")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "telecom.cdr_events")
FLUSH_MAX_RECORDS = int(os.getenv("FLUSH_MAX_RECORDS", "500"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("FLUSH_INTERVAL_SECONDS", "10"))

# snapshot property that holds the next Kafka offset to read, per partition
OFFSETS_PROPERTY = f"kafka.{KAFKA_TOPIC}.offsets"

CDR_SCHEMA = Schema(
    NestedField(1, "transaction_id", StringType(), required=False),
    NestedField(2, "customer_id", StringType(), required=False),
    NestedField(3, "full_name", StringType(), required=False),
    NestedField(4, "city", StringType(), required=False),
    NestedField(5, "mobile_number", StringType(), required=False),
    NestedField(6, "sim_serial_number", StringType(), required=False),
    NestedField(7, "sim_status", StringType(), required=False),
    NestedField(8, "imei", StringType(), required=False),
    NestedField(9, "risk_score", DoubleType(), required=False),
    NestedField(10, "fraud_flag", BooleanType(), required=False),
    NestedField(11, "fraud_type", StringType(), required=False),
    NestedField(12, "type", StringType(), required=False),
    NestedField(13, "amount_eur", DoubleType(), required=False),
    NestedField(14, "time_stamp", TimestamptzType(), required=False),
    NestedField(15, "processed_timestamp", TimestamptzType(), required=False),
    # lineage: where in Kafka each row came from
    NestedField(16, "kafka_partition", IntegerType(), required=False),
    NestedField(17, "kafka_offset", LongType(), required=False),
    NestedField(18, "ingested_at", TimestamptzType(), required=False),
)

# one folder of files per day, so date-filtered queries skip other days
CDR_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=14, field_id=1000, transform=DayTransform(), name="event_day")
)


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "lakehouse")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "lakehouse-secret")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")


def ensure_bucket():
    """create the warehouse bucket if it does not exist yet"""
    s3 = fs.S3FileSystem(
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY_ID,
        secret_key=S3_SECRET_ACCESS_KEY,
        region=S3_REGION,
        allow_bucket_creation=True,
    )
    if s3.get_file_info(S3_BUCKET).type == fs.FileType.NotFound:
        s3.create_dir(S3_BUCKET)
        logger.info(f"Created bucket {S3_BUCKET}")


def load_iceberg_catalog():
    """connect to the Iceberg REST catalog, retrying until it is up"""
    for attempt in range(30):
        try:
            catalog = load_catalog(
                "lakehouse",
                **{
                    "type": "rest",
                    "uri": os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
                    "s3.endpoint": S3_ENDPOINT,
                    "s3.access-key-id": S3_ACCESS_KEY_ID,
                    "s3.secret-access-key": S3_SECRET_ACCESS_KEY,
                    "s3.region": S3_REGION,
                    "s3.path-style-access": "true",
                },
            )
            catalog.list_namespaces()
            return catalog
        except Exception as e:
            logger.warning(f"Iceberg catalog not ready ({e}), retrying in 2s")
            time.sleep(2)
    raise RuntimeError("Could not connect to the Iceberg catalog")


def get_or_create_table(catalog) -> Table:
    namespace = ICEBERG_TABLE.split(".")[0]
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table_if_not_exists(
        ICEBERG_TABLE,
        schema=CDR_SCHEMA,
        partition_spec=CDR_PARTITION_SPEC,
    )
    logger.info(f"Using Iceberg table {ICEBERG_TABLE} at {table.location()}")
    return table


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """parse an ISO timestamp; values without a timezone are treated as UTC"""
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def to_row(cdr: Dict, partition: int, offset: int) -> Dict:
    """map one CDR message to a row of the Iceberg table"""
    return {
        "transaction_id": cdr.get("transaction_id"),
        "customer_id": cdr.get("customer_id"),
        "full_name": cdr.get("full_name"),
        "city": cdr.get("city"),
        "mobile_number": cdr.get("mobile_number"),
        "sim_serial_number": cdr.get("sim_serial_number"),
        "sim_status": cdr.get("sim_status"),
        "imei": cdr.get("imei"),
        "risk_score": float(cdr["risk_score"]) if cdr.get("risk_score") is not None else None,
        "fraud_flag": cdr.get("fraud_flag"),
        "fraud_type": cdr.get("fraud_type"),
        "type": cdr.get("type"),
        "amount_eur": float(cdr["amount_eur"]) if cdr.get("amount_eur") is not None else None,
        "time_stamp": parse_timestamp(cdr.get("time_stamp")),
        "processed_timestamp": parse_timestamp(cdr.get("processed_timestamp")),
        "kafka_partition": partition,
        "kafka_offset": offset,
        "ingested_at": datetime.now(timezone.utc),
    }


class LakehouseWriter:
    def __init__(self):
        ensure_bucket()
        self.catalog = load_iceberg_catalog()
        self.table = get_or_create_table(self.catalog)
        self.arrow_schema = CDR_SCHEMA.as_arrow()
        self.buffer: List[Dict] = []
        # next offset to read per partition, for the rows in self.buffer
        self.pending_offsets: Dict[int, int] = {}
        self.last_flush = time.monotonic()
        self.running = True

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            # offsets are committed manually, only after the data is in Iceberg
            "enable.auto.commit": False,
        })
        self.dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})

    def stored_offsets(self) -> Dict[str, int]:
        """offsets saved with the data in the latest Iceberg snapshot"""
        self.table.refresh()
        snapshot = self.table.current_snapshot()
        if snapshot is None or snapshot.summary is None:
            return {}
        raw = snapshot.summary.additional_properties.get(OFFSETS_PROPERTY)
        return json.loads(raw) if raw else {}

    def on_assign(self, consumer, partitions):
        """start each partition right after the last row already in Iceberg"""
        stored = self.stored_offsets()
        for p in partitions:
            if str(p.partition) in stored:
                p.offset = stored[str(p.partition)]
        logger.info(f"Assigned partitions: {[(p.partition, p.offset) for p in partitions]}")
        consumer.assign(partitions)
        # bring Kafka's committed offsets in line with Iceberg, so consumer lag
        # is correct even for partitions that get no new messages
        resumed = [TopicPartition(KAFKA_TOPIC, p.partition, p.offset) for p in partitions if p.offset >= 0]
        if resumed:
            try:
                consumer.commit(offsets=resumed, asynchronous=False)
            except KafkaException as e:
                logger.warning(f"Kafka offset commit failed, consumer lag may be stale: {e}")

    def on_revoke(self, consumer, partitions):
        """write out what we have before another consumer takes over"""
        self.flush()
        logger.info(f"Revoked partitions: {[p.partition for p in partitions]}")

    def send_to_dlq(self, msg, error: Exception):
        self.dlq_producer.produce(
            KAFKA_DLQ_TOPIC,
            key=msg.key(),
            value=msg.value(),
            headers={
                "error": str(error),
                "source": "lakehouse-writer",
                "source_partition": str(msg.partition()),
                "source_offset": str(msg.offset()),
            },
        )
        self.dlq_producer.flush(10)
        logger.warning(f"Sent partition {msg.partition()} offset {msg.offset()} to {KAFKA_DLQ_TOPIC}")

    def flush(self):
        """append the buffered rows to Iceberg, then commit the Kafka offsets"""
        if not self.pending_offsets:
            return

        # merge with the stored offsets so partitions without new rows keep theirs
        stored = self.stored_offsets()
        offsets = dict(stored)
        for p, o in self.pending_offsets.items():
            offsets[str(p)] = max(o, stored.get(str(p), 0))

        # Append only rows Iceberg does not have yet, each once. Normally that is the whole
        # buffer; this guards against writing a batch twice after a failure or a rebalance.
        rows, seen = [], set()
        for row in self.buffer:
            position = (row["kafka_partition"], row["kafka_offset"])
            if row["kafka_offset"] >= stored.get(str(row["kafka_partition"]), 0) and position not in seen:
                seen.add(position)
                rows.append(row)
        if len(rows) < len(self.buffer):
            logger.warning(f"Skipped {len(self.buffer) - len(rows)} rows already in {ICEBERG_TABLE}")

        if rows:
            started = time.monotonic()
            batch = pa.Table.from_pylist(rows, schema=self.arrow_schema)
            self.table.append(batch, snapshot_properties={OFFSETS_PROPERTY: json.dumps(offsets)})
            logger.info(
                f"Appended {len(rows)} rows to {ICEBERG_TABLE} in {time.monotonic() - started:.2f}s "
                f"(snapshot {self.table.current_snapshot().snapshot_id}, offsets {offsets})"
            )

        # The rows are durable now, so clear the buffer before anything else can fail or
        # call back into flush(): a blocking Kafka commit can trigger on_revoke -> flush()
        committed = self.pending_offsets
        self.buffer = []
        self.pending_offsets = {}
        self.last_flush = time.monotonic()

        # Iceberg is the source of truth; this commit only keeps consumer lag accurate,
        # so a failure (e.g. the broker restarting) is logged, not fatal
        try:
            self.consumer.commit(
                offsets=[TopicPartition(KAFKA_TOPIC, p, o) for p, o in committed.items()],
                asynchronous=False,
            )
        except KafkaException as e:
            logger.warning(f"Kafka offset commit failed, consumer lag may be stale: {e}")

    def run(self):
        self.consumer.subscribe([KAFKA_TOPIC], on_assign=self.on_assign, on_revoke=self.on_revoke)
        logger.info(f"Writing {KAFKA_TOPIC} to {ICEBERG_TABLE} (group: {KAFKA_GROUP_ID})")
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is not None:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error(f"Consumer error: {msg.error()}")
                    else:
                        try:
                            cdr = json.loads(msg.value())
                            self.buffer.append(to_row(cdr, msg.partition(), msg.offset()))
                        except (ValueError, TypeError, KeyError) as e:
                            self.send_to_dlq(msg, e)
                        self.pending_offsets[msg.partition()] = msg.offset() + 1

                if (len(self.buffer) >= FLUSH_MAX_RECORDS
                        or time.monotonic() - self.last_flush >= FLUSH_INTERVAL_SECONDS):
                    self.flush()
        finally:
            self.flush()
            self.consumer.close()
            logger.info("Lakehouse writer stopped")

    def stop(self, *_):
        self.running = False


if __name__ == "__main__":
    writer = LakehouseWriter()
    signal.signal(signal.SIGTERM, writer.stop)
    signal.signal(signal.SIGINT, writer.stop)
    writer.run()
