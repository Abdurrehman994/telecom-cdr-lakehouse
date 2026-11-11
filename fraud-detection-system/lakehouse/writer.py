"""Lakehouse sink: streams one Kafka topic into one Apache Iceberg table.

What lands where is configuration, not code. A sink file in sinks/ names the topic, the
table, its partitioning, batching and maintenance policy; the table's columns come from
the topic's data contract in the schema registry. Run one writer per sink:

    python writer.py sinks/cdr_events.yaml

Each sink reads its topic as its own consumer group, independently of other consumers.
Messages are buffered and appended in batches, because each append creates a snapshot
and new Parquet files; appending one row at a time would leave thousands of tiny files.

Exactly-once: the Kafka offsets of each batch are written into the Iceberg snapshot
properties in the same commit as the data. On startup and on every partition assignment
the writer seeks to the offsets stored in the latest snapshot, so a crash between the
Iceberg commit and the Kafka offset commit cannot produce duplicate rows.

Schema evolution: messages carry the id of the contract version they were written with.
When one arrives with a version the writer has not seen, its new fields are added to the
table as columns before the message is buffered.
"""
import argparse
import dataclasses
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from dataplatform import Decoder, registry_client, subject_for
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table

from catalog import ensure_bucket, load_iceberg_catalog, stored_offsets
from sink_config import SinkConfig, SinkConfigError, load_sink
from table_schema import RowBuilder, describe_spec, iceberg_schema, partition_spec, sync_table_schema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("lakehouse-writer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# an append can lose a race with a maintenance commit; it is retried on the new snapshot
COMMIT_ATTEMPTS = int(os.getenv("COMMIT_ATTEMPTS", "5"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))

rows_appended = Counter("lakehouse_rows_appended_total", "Rows appended to the Iceberg table", ["table"])
rows_skipped = Counter("lakehouse_rows_skipped_total", "Buffered rows skipped because Iceberg already had them",
                       ["table"])
commit_seconds = Histogram("lakehouse_commit_duration_seconds", "Time to append one batch to Iceberg", ["table"],
                           buckets=(0.25, 0.5, 1, 2, 4, 8, 16, 32, 64))
commit_conflicts = Counter("lakehouse_commit_conflicts_total", "Appends retried after a concurrent Iceberg commit",
                           ["table"])
kafka_commit_failures = Counter("lakehouse_kafka_commit_failures_total",
                                "Failed Kafka offset commits (lag reporting only)", ["table"])
dlq_messages = Counter("lakehouse_dlq_messages_total", "Messages the writer sent to the dead-letter topic", ["table"])
schema_changes = Counter("lakehouse_schema_changes_total", "Columns added or widened from a new contract version",
                         ["table"])
buffered_rows = Gauge("lakehouse_buffered_rows", "Rows read from Kafka but not yet in Iceberg", ["table"])
# consumergroup and topic match kafka-exporter's labels, so alerts can join on consumer lag
last_commit = Gauge("lakehouse_last_commit_timestamp_seconds",
                    "Unix time of the last successful append (or of startup)", ["table", "consumergroup", "topic"])


class SinkMetrics:
    """the writer metrics, labelled with this sink's table"""

    def __init__(self, sink: SinkConfig):
        self.rows_appended = rows_appended.labels(sink.table)
        self.rows_skipped = rows_skipped.labels(sink.table)
        self.commit_seconds = commit_seconds.labels(sink.table)
        self.commit_conflicts = commit_conflicts.labels(sink.table)
        self.kafka_commit_failures = kafka_commit_failures.labels(sink.table)
        self.dlq_messages = dlq_messages.labels(sink.table)
        self.schema_changes = schema_changes.labels(sink.table)
        self.buffered_rows = buffered_rows.labels(sink.table)
        self.last_commit = last_commit.labels(sink.table, sink.consumer_group, sink.topic)


def with_env_overrides(sink: SinkConfig) -> SinkConfig:
    """FLUSH_MAX_RECORDS and FLUSH_INTERVAL_SECONDS override the sink file, e.g. for a load test"""
    overrides = {}
    if os.getenv("FLUSH_MAX_RECORDS"):
        overrides["flush_max_records"] = int(os.environ["FLUSH_MAX_RECORDS"])
    if os.getenv("FLUSH_INTERVAL_SECONDS"):
        overrides["flush_interval_seconds"] = float(os.environ["FLUSH_INTERVAL_SECONDS"])
    return dataclasses.replace(sink, **overrides)


def latest_contract(registry, topic: str) -> Tuple[int, Dict]:
    """(schema id, Avro schema) of the newest contract version for the topic, retrying until it is registered"""
    subject = subject_for(topic)
    for _ in range(30):
        try:
            registered = registry.get_latest_version(subject)
            return registered.schema_id, json.loads(registered.schema.schema_str)
        except Exception as e:
            logger.warning(f"No contract for {topic} yet ({e}), retrying in 2s")
            time.sleep(2)
    raise RuntimeError(f"No contract is registered for {topic} (subject {subject})")


def with_commit_retries(table: Table, action, on_conflict=None):
    """run action(), re-reading the table and retrying if another commit landed first"""
    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        try:
            return action()
        except CommitFailedException as e:
            if on_conflict is not None:
                on_conflict()
            if attempt == COMMIT_ATTEMPTS:
                raise
            logger.warning(f"Iceberg commit conflict, retrying ({attempt}/{COMMIT_ATTEMPTS}): {e}")
            table.refresh()


def prepare_table(catalog, sink: SinkConfig, avro_schema: Dict) -> Table:
    """create the sink's table, or add the contract's new fields to an existing one"""
    schema = iceberg_schema(avro_schema)
    spec = partition_spec(schema, sink.partition_by)
    catalog.create_namespace_if_not_exists(sink.namespace)
    table = catalog.create_table_if_not_exists(sink.table, schema=schema, partition_spec=spec)

    changed = with_commit_retries(table, lambda: sync_table_schema(table, avro_schema))
    if changed:
        logger.info(f"Added or widened columns {changed} in {sink.table} to match the contract")
    if describe_spec(schema, spec) != describe_spec(table.schema(), table.spec()):
        # Iceberg can evolve partitioning, but changing it is a deliberate migration, not a startup side effect
        logger.warning(f"{sink.table} is partitioned as {describe_spec(table.schema(), table.spec())}, "
                       f"not as the sink file says; keeping the table's partitioning")
    for column in filter(None, [sink.sort_by] + [p.column for p in sink.partition_by]):
        if column not in {f.name for f in table.schema().fields}:
            raise SinkConfigError(f"{sink.name}: column {column} is not in {sink.table}")
    logger.info(f"Using Iceberg table {sink.table} at {table.location()}")
    return table


class LakehouseWriter:
    def __init__(self, sink: SinkConfig, registry, catalog, consumer, dlq_producer):
        self.sink = sink
        self.metrics = SinkMetrics(sink)
        # JSON is accepted for messages from before the topic had a contract
        self.decoder = Decoder(registry, allow_json=True)
        schema_id, avro_schema = latest_contract(registry, sink.topic)
        self.table = prepare_table(catalog, sink, avro_schema)
        self.known_schema_ids = {schema_id}
        self.rows = RowBuilder(self.table.schema())
        self.buffer: List[Dict] = []
        # next offset to read per partition, for the rows in self.buffer
        self.pending_offsets: Dict[int, int] = {}
        self.last_flush = time.monotonic()
        self.running = True
        self.consumer = consumer
        self.dlq_producer = dlq_producer
        # "last commit" starts now, so an alert fires if the first commit never comes
        self.metrics.last_commit.set_to_current_time()

    @classmethod
    def connect(cls, sink: SinkConfig) -> "LakehouseWriter":
        """a writer for the sink, connected to the registry, the catalog and Kafka"""
        ensure_bucket()
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": sink.consumer_group,
            "auto.offset.reset": "earliest",
            # offsets are committed manually, only after the data is in Iceberg
            "enable.auto.commit": False,
        })
        dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})
        return cls(sink, registry_client(), load_iceberg_catalog(), consumer, dlq_producer)

    def stored_offsets(self) -> Dict[str, int]:
        """offsets saved with the data in the latest Iceberg snapshot"""
        self.table.refresh()
        return stored_offsets(self.table, self.sink.offsets_property)

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
        resumed = [TopicPartition(self.sink.topic, p.partition, p.offset) for p in partitions if p.offset >= 0]
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
            self.sink.dead_letter_topic,
            key=msg.key(),
            value=msg.value(),
            headers={
                "error": str(error),
                "source": f"lakehouse-writer/{self.sink.name}",
                "source_partition": str(msg.partition()),
                "source_offset": str(msg.offset()),
            },
        )
        self.dlq_producer.flush(10)
        self.metrics.dlq_messages.inc()
        logger.warning(f"Sent partition {msg.partition()} offset {msg.offset()} to {self.sink.dead_letter_topic}")

    def evolve(self, schema_id: int):
        """add the fields of a contract version seen for the first time to the table"""
        avro_schema = self.decoder.writer_schema(schema_id)
        changed = with_commit_retries(self.table, lambda: sync_table_schema(self.table, avro_schema))
        if changed:
            self.metrics.schema_changes.inc(len(changed))
            logger.info(f"Contract version {schema_id} of {self.sink.topic}: "
                        f"added or widened columns {changed} in {self.sink.table}")
        self.known_schema_ids.add(schema_id)
        # rows already buffered lack the new columns, which are written as null
        self.rows = RowBuilder(self.table.schema())

    def buffer_message(self, msg):
        """decode one message into a row; one that can never be written goes to the dead-letter topic.

        Other errors, such as the registry being unreachable, propagate and stop the writer:
        it restarts from the offsets in Iceberg, so nothing is lost or written twice.
        """
        try:
            decoded = self.decoder.decode(msg.value())
            if decoded.schema_id is not None and decoded.schema_id not in self.known_schema_ids:
                self.evolve(decoded.schema_id)
            self.buffer.append(self.rows.build(decoded.value, msg.partition(), msg.offset(),
                                               datetime.now(timezone.utc)))
        except (ValueError, TypeError) as e:
            self.send_to_dlq(msg, e)
        self.pending_offsets[msg.partition()] = msg.offset() + 1

    def flush(self):
        """append the buffered rows to Iceberg, then commit the Kafka offsets"""
        if not self.pending_offsets:
            return

        # another commit (e.g. compaction) may land first; re-read the table and retry
        with_commit_retries(self.table, self._append_new_rows, on_conflict=self.metrics.commit_conflicts.inc)

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
                offsets=[TopicPartition(self.sink.topic, p, o) for p, o in committed.items()],
                asynchronous=False,
            )
        except KafkaException as e:
            self.metrics.kafka_commit_failures.inc()
            logger.warning(f"Kafka offset commit failed, consumer lag may be stale: {e}")

    def _append_new_rows(self):
        """append the buffered rows Iceberg does not have yet, with the next offsets"""
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
            self.metrics.rows_skipped.inc(len(self.buffer) - len(rows))
            logger.warning(f"Skipped {len(self.buffer) - len(rows)} rows already in {self.sink.table}")
        if not rows:
            return

        started = time.monotonic()
        batch = pa.Table.from_pylist(rows, schema=self.table.schema().as_arrow())
        self.table.append(batch, snapshot_properties={self.sink.offsets_property: json.dumps(offsets)})
        elapsed = time.monotonic() - started
        self.metrics.commit_seconds.observe(elapsed)
        self.metrics.rows_appended.inc(len(rows))
        self.metrics.last_commit.set_to_current_time()
        logger.info(
            f"Appended {len(rows)} rows to {self.sink.table} in {elapsed:.2f}s "
            f"(snapshot {self.table.current_snapshot().snapshot_id}, offsets {offsets})"
        )

    def run(self):
        self.consumer.subscribe([self.sink.topic], on_assign=self.on_assign, on_revoke=self.on_revoke)
        logger.info(f"Writing {self.sink.topic} to {self.sink.table} (group: {self.sink.consumer_group})")
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is not None:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error(f"Consumer error: {msg.error()}")
                    else:
                        self.buffer_message(msg)
                self.metrics.buffered_rows.set(len(self.buffer))

                if (len(self.buffer) >= self.sink.flush_max_records
                        or time.monotonic() - self.last_flush >= self.sink.flush_interval_seconds):
                    self.flush()
        finally:
            self.flush()
            self.consumer.close()
            logger.info("Lakehouse writer stopped")

    def stop(self, *_):
        self.running = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream a Kafka topic into an Iceberg table")
    parser.add_argument("sink", help="sink file, e.g. sinks/cdr_events.yaml")
    sink = with_env_overrides(load_sink(parser.parse_args().sink))
    start_http_server(METRICS_PORT)
    writer = LakehouseWriter.connect(sink)
    signal.signal(signal.SIGTERM, writer.stop)
    signal.signal(signal.SIGINT, writer.stop)
    writer.run()
