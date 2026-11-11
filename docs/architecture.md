# Architecture: Telecom CDR Streaming Lakehouse

This document describes how call detail records (CDRs) move through the system, why it is built this way, and what would change to run it in production.

The project started as a University of Oulu group project that used RabbitMQ for fraud detection. Version 2 replaces the queue with Kafka and adds an Apache Iceberg lakehouse, so the same event stream serves real-time fraud detection and historical analytics.

## Contents

- [System overview](#system-overview)
- [Components](#components)
- [Data flow](#data-flow)
- [Design decisions](#design-decisions)
- [Delivery guarantees](#delivery-guarantees)
- [Failure scenarios](#failure-scenarios)
- [Performance](#performance)
- [Data model](#data-model)
- [Known limitations](#known-limitations)
- [Path to production](#path-to-production)
- [Running and verifying](#running-and-verifying)

## System overview

```mermaid
flowchart LR
    SRC[CDR sources<br/>ingest-auto.py] -->|HTTP POST| ING[cdr-ingestion<br/>FastAPI]
    ING -->|key = customer_id| T[(Kafka topic<br/>cdr-events<br/>3 partitions)]

    T -->|group: fraud-detection| ML1[ml-service]
    T -->|group: fraud-detection| ML2[ml-service2]
    T -->|group: lakehouse-writer| LW[lakehouse-writer]

    ML1 & ML2 -->|store alert| PG[(Postgres<br/>fraud_alerts)]
    ML1 & ML2 -->|new alerts only| AM[Alertmanager]
    ML1 & ML2 -.->|uncertain cases| OL[Ollama LLM]
    ML1 & ML2 & LW -->|unprocessable| DLQ[(cdr-events-dlq)]

    LW -->|commit snapshot| CAT[Iceberg REST catalog]
    LW -->|Parquet files| S3[(SeaweedFS<br/>S3 API)]
    CAT -->|metadata files| S3

    Q[query.py<br/>DuckDB] --> CAT
    Q --> S3
```

One topic, two independent consumer groups. Fraud detection and the lakehouse each read every CDR at their own pace, and neither can slow down or block the other.

## Components

| Component | Technology | Role |
|---|---|---|
| `cdr-ingestion` | Python, FastAPI, `confluent-kafka` | Validates and enriches CDRs received over HTTP, publishes them to Kafka |
| `kafka` | Apache Kafka 3.9.1, KRaft mode | Durable, ordered, replayable event log |
| `kafka-init` | One-shot container | Creates `cdr-events` (3 partitions) and `cdr-events-dlq` (1 partition); automatic topic creation is disabled |
| `ml-service`, `ml-service2` | Python, scikit-learn, Ollama | Fraud detection: rules, then a random forest, then an LLM for uncertain cases. Also serves the alerts API (`/alerts`) |
| `postgres` | PostgreSQL 15 | OLTP store for fraud alerts and their case status; schema in `postgres/init/` |
| `lakehouse-writer` | Python, PyIceberg, PyArrow | Streams CDRs into the Iceberg table `telecom.cdr_events` |
| `iceberg-rest` | Apache Iceberg REST catalog 1.9.1 | Tracks the current metadata file of each table; state kept in SQLite on a volume |
| `seaweedfs` | SeaweedFS 4.51 | S3-compatible object storage for Parquet data and Iceberg metadata |
| `query.py` | DuckDB | Analytical (OLAP) queries and snapshot time travel |
| Prometheus, Grafana, Alertmanager | | Metrics, dashboards and fraud alerts |

## Data flow

1. **Ingest.** A client posts a CDR to `/ingest` or `/ingest/batch`. The service validates it, adds `processed_timestamp` and simple risk flags, and produces it to `cdr-events` with `customer_id` as the key. It answers only after the broker confirms the write.
2. **Partition.** Kafka hashes the key, so all CDRs of one customer land in the same partition, in the order they were produced.
3. **Detect fraud.** The `fraud-detection` group splits the 3 partitions between the two `ml-service` replicas. Each CDR goes through rules, a random forest score, and the LLM if the score is uncertain. A fraud decision is stored in Postgres; if the row is new, Alertmanager is notified. Analysts list alerts and record decisions (`confirmed`, `dismissed`) through the alerts API.
4. **Write to the lakehouse.** The `lakehouse-writer` group reads the same partitions independently. It buffers rows and appends a batch to Iceberg every 500 rows or 10 seconds. Each append creates a new snapshot.
5. **Analyse.** `query.py` loads the current snapshot (or an earlier one) and runs SQL with DuckDB.

## Design decisions

### Kafka instead of RabbitMQ

RabbitMQ deletes a message once it is acknowledged. That fits a task queue with a single consumer, which is what the original system was. Version 2 needs the same events to feed two systems, and to be re-read after a fraud rule changes or a model is retrained. Kafka keeps messages after they are read, so any number of consumer groups can read the same data, and a group can rewind to replay history.

The migration ran in stages, so the system kept working at every commit:

1. Run Kafka next to RabbitMQ.
2. Dual-write: ingestion publishes to both.
3. Move fraud detection to Kafka.
4. Remove RabbitMQ.

### Partition key: `customer_id`

Fraud patterns are per customer: a SIM swap followed by large payments, or many transactions in a short window. Keying by `customer_id` keeps each customer's CDRs in order inside one partition and sends them to the same consumer.

Trade-off: **key skew**. Customers with more activity make some partitions bigger. In the test data, partitions hold 248, 328 and 224 messages. With real traffic, a few very active subscribers could overload one partition; the usual answers are more partitions or a composite key.

### KRaft, and why there is one broker

KRaft lets Kafka manage its own cluster metadata, without a separate ZooKeeper cluster. One broker with replication factor 1 keeps the local stack light. It is not fault tolerant; see [Path to production](#path-to-production).

### Explicit topic creation

`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, and `kafka-init` creates topics with chosen partition counts. A typo in a topic name then fails loudly instead of silently creating a new, empty topic.

### At-least-once for fraud detection

The ML consumer uses `enable.auto.offset.store=false` and stores a message's offset only after processing finishes. If a replica crashes mid-message, the message is processed again rather than skipped. Missing a fraud case is worse than checking a CDR twice.

### Exactly-once into Iceberg

Kafka offsets and Iceberg data live in different systems, so committing them separately leaves a gap: a crash after the Iceberg commit but before the Kafka commit would write the same rows twice on restart.

The writer closes that gap by storing the offsets **inside the Iceberg commit**, as the snapshot property `kafka.cdr-events.offsets`. Data and position are committed atomically. When partitions are assigned, the writer reads that property and seeks there, ignoring Kafka's own committed offsets. It then commits the same offsets back to Kafka, only so that consumer lag reports correctly.

This was tested by rewinding the writer's Kafka offsets to 0 and restarting it: it resumed at offsets 248, 328 and 225 from Iceberg and wrote no duplicates.

A later test found a gap anyway. With the broker stopped, the writer appended a batch to Iceberg and then blocked on the Kafka offset commit. While it waited, the consumer session timed out, the revoke callback ran `flush()` again, and the batch was still in the buffer, so the same 3 rows were appended twice. The commit then failed and the writer crashed. Two changes closed it:

- The buffer is cleared as soon as the Iceberg append succeeds, before the Kafka commit, and a failed Kafka commit is logged instead of raised. The Kafka commit only feeds lag reporting, so it should never stop the writer.
- Before each append the writer drops rows whose offset is below what Iceberg already stores for that partition. An append is then idempotent against the table's own state, whatever the bug that re-sends it.

Repeating the same broker outage after the fix: the commit failure was logged, the partitions were revoked and reassigned, the writer did not restart, and no new duplicates were written.

### Batched appends

Every Iceberg append writes new Parquet files and a new snapshot. Writing one row per append would create thousands of tiny files and slow down every query. The writer batches up to 500 rows or 10 seconds, trading a little latency for far fewer files.

### Postgres for alerts, Iceberg for history (OLTP and OLAP)

The two stores answer different questions:

| | Postgres (`fraud_alerts`) | Iceberg (`telecom.cdr_events`) |
|---|---|---|
| Workload | OLTP: read and update one alert at a time | OLAP: scan and aggregate millions of CDRs |
| Typical query | "Open alerts, newest first"; "mark this alert confirmed" | "Fraud amount per type per day this year" |
| Access pattern | Primary key and index lookups, row updates | Columnar scans of Parquet files, partition pruning |
| Data | Only fraud decisions, mutable (status changes) | Every CDR, append-only, with snapshots |

Putting alerts in Iceberg would turn every status change into a new snapshot and file rewrite. Putting CDR history in Postgres would make large scans slow and storage expensive. Each store does what it is built for.

### Idempotent alerts

At-least-once delivery means a CDR can be processed twice: after a crash, a rebalance or a deliberate replay. The `fraud_alerts` table uses `transaction_id` as its primary key, and alerts are inserted with `ON CONFLICT DO NOTHING`. The insert reports whether the row is new, and only new alerts reach Alertmanager. Replaying the whole topic therefore produces no duplicate rows and no repeated notifications.

Tested by replaying all CDRs twice: the second replay recognised 648 alerts as already stored and created no duplicate rows.

A Postgres outage is treated differently from a bad message. The CDR is valid, so instead of going to the DLQ, the consumer retries with backoff and the partition waits. Tested by stopping Postgres, sending a fraud CDR, and restarting Postgres: the alert was stored about 3 seconds after the database came back.

### Dead-letter topic

A message that cannot be parsed or processed goes to `cdr-events-dlq`, with headers for the error, the source partition and the source offset. The consumer then moves on, so one bad message cannot block a whole partition, and nothing is silently dropped.

### Iceberg as the table format

Iceberg gives the lakehouse database-like guarantees on plain files in object storage: atomic commits, schema evolution, hidden partitioning, and time travel to earlier snapshots. It is an open format, so the same table can be read by DuckDB, Trino, Spark or Flink without copying data.

The table is partitioned by `day(time_stamp)`, so queries filtered by date skip files for other days.

### SeaweedFS for object storage

Any S3-compatible store works, because Iceberg only talks to the S3 API. MinIO was the first choice, but its Docker images are no longer published. SeaweedFS is Apache-licensed, runs as a single container, and is a drop-in replacement for local development. In the cloud this would be Amazon S3 or an equivalent.

## Delivery guarantees

| Path | Guarantee | How |
|---|---|---|
| Producer → Kafka | No duplicates or reordering from retries | `enable.idempotence=true`, `acks=all` |
| Kafka → fraud detection | At least once | Offset stored only after processing |
| Fraud decision → alert | Effectively once per transaction | `transaction_id` primary key, `ON CONFLICT DO NOTHING`, notify only on insert |
| Kafka → Iceberg | Exactly once | Offsets committed in the same Iceberg snapshot as the data |
| HTTP client → Kafka | At least once, if the client retries | The API waits for the broker's delivery report: `202` only after the CDR is stored, `503` (or a per-record error in a batch) if it is not |

## Failure scenarios

| Failure | What happens |
|---|---|
| An `ml-service` replica stops | It leaves the group; Kafka moves its partitions to the other replica within seconds. Tested: one replica held all 3 partitions until the second joined, then they were rebalanced. |
| A malformed message | Sent to `cdr-events-dlq` with the error; processing continues. Tested with a non-JSON message. |
| `lakehouse-writer` crashes mid-batch | Unflushed rows were never committed to Iceberg, so on restart it reads them again from the last stored offsets. No loss, no duplicates. |
| Kafka offsets lost or rewound | The writer ignores them and resumes from Iceberg. Tested by resetting the group to offset 0. |
| Ollama unavailable | Uncertain cases fall back to the random forest score. After one failure the LLM is skipped for 30 seconds (circuit breaker), so the partition is not held up waiting for the same failure on every CDR. Tested under load; see [Performance](#performance). |
| Postgres unavailable | The ML consumer retries the alert insert with backoff; its partitions wait. Nothing is skipped or sent to the DLQ. Tested. |
| A CDR is redelivered | The alert insert is a no-op and no second notification is sent. Tested with two full replays. |
| The Kafka broker stops | Ingestion answers `503` after about 10 seconds, so clients know to retry; consumers wait and rejoin their groups when it returns. Tested: no messages lost, no duplicate rows, no writer restart. With one broker this is an outage; replication would prevent it. |

## Performance

Measured with [`loadtest/loadtest.py`](../fraud-detection-system/loadtest/loadtest.py): 10,000 CDRs (copies of the sample data with unique transaction ids) sent as 20 batches of 500 over 4 concurrent connections. Setup: a laptop with an 8 GB Docker VM, one Kafka broker, 3 partitions, 2 `ml-service` replicas, Ollama not running. These numbers describe this setup, not a production cluster.

| Stage | Throughput | Notes |
|---|---|---|
| HTTP → Kafka (confirmed) | 1,594 CDRs/s | Batch latency p50 1.2 s, p95 2.1 s for 500 CDRs, including the wait for broker confirmation |
| Fraud detection | 46 CDRs/s | 2 replicas; one Postgres insert and one Alertmanager call per fraud alert, synchronously |
| Lakehouse writer | 86 CDRs/s | 21 commits of about 476 rows; Iceberg commit p50 4.8 s, p95 6.0 s |

### What the load test found

**The LLM path stalled fraud detection.** A 2,000-CDR run processed about 0.5 CDRs/s. With Ollama not running, every uncertain CDR spent about 4 seconds failing to resolve the `ollama` hostname before falling back to the score. A slow or missing dependency held up the whole partition. Fix: a circuit breaker that skips the LLM for 30 seconds after a failure, plus a 2-second connect timeout. The remaining backlog of 1,557 CDRs then cleared in under 40 seconds.

**The random forest retrained on every CDR.** Training was triggered when the buffer held 50 CDRs, but the buffer is a sliding window that never empties, so after the first 50 every clean CDR refit 100 trees on up to 1,000 rows. It now retrains once per 50 new CDRs.

**Iceberg small files.** After the 10,000-CDR run the table had 6,052 data files averaging 2 rows and 6.8 KB. The sample data's timestamps span 301 days and the table is partitioned by day, so each 500-row commit wrote about 240 tiny files. That explains most of the 4.8-second commit time, and it would slow every query. Live CDRs mostly fall into today's partition, so day partitioning suits real-time traffic; backfills and late data spread across many days are what it handles badly. The options are larger commits, coarser partitions (Iceberg can change the partition spec without rewriting data), and scheduled compaction.

**Where the next limits are.** The fraud consumer does a synchronous Postgres insert and an HTTP call per alert; batching inserts and sending alerts asynchronously would raise it. With 3 partitions, at most 3 replicas can share the work. With the LLM enabled, its latency (seconds per call on CPU) bounds the uncertain cases; it belongs off the hot path, for example as a separate review topic.

## Data model

Iceberg table `telecom.cdr_events`, partitioned by `day(time_stamp)`:

| Column | Type | Notes |
|---|---|---|
| `transaction_id` | string | Unique per CDR |
| `customer_id` | string | Kafka key |
| `full_name`, `city`, `mobile_number` | string | |
| `sim_serial_number`, `sim_status`, `imei` | string | |
| `risk_score` | double | 0.0 to 1.0 |
| `fraud_flag` | boolean | |
| `fraud_type` | string | e.g. `SIM_SWAP`, `IMEI_CLONE`, `ROAMING_ABUSE` |
| `type` | string | Transaction type |
| `amount_eur` | double | |
| `time_stamp` | timestamptz | Event time, used for partitioning |
| `processed_timestamp` | timestamptz | Set by ingestion |
| `kafka_partition`, `kafka_offset` | int, long | Lineage: where each row came from |
| `ingested_at` | timestamptz | When the writer read it |

## Known limitations

- **A client retry can duplicate a CDR in Kafka.** The API now waits for the broker before answering, so a CDR is no longer lost silently. But if the broker stores a CDR after the API has given up waiting (10 seconds), the client sees an error, retries, and the CDR is produced twice. Fraud alerts are deduplicated by `transaction_id`; the Iceberg table is not, so analytical queries should count distinct `transaction_id`, or a deduplicated table should be built from it.
- **The random forest is trained per replica, in memory.** Each replica only sees its own partitions, and the model is lost on restart. As a result, replaying the same CDRs can give different decisions: in testing, one CDR was clean on the first replay and fraud on the second. A shared model registry, offline training from the lakehouse, and storing the model version with each alert would fix this.
- **The first decision wins.** If a replay produces a different decision for a CDR that already has an alert, the stored alert is not updated. With model versions recorded, a re-scoring job could compare and update them deliberately.
- **One lakehouse writer.** The offset merge assumes a single writer. Two replicas would need to handle Iceberg commit conflicts, or write per-partition.
- **No table maintenance.** Small files and old snapshots accumulate; the load test produced files of 2 rows on average (see [Performance](#performance)). Compaction and snapshot expiry need to run on a schedule.
- **Test-grade catalog.** `iceberg-rest-fixture` is the reference implementation, with SQLite storage and no authentication.
- **No schema contract.** Producers and consumers agree on JSON fields informally; a breaking change would only show up as DLQ messages.
- **DuckDB loads the snapshot into memory.** Fine for this data volume, not for large tables.

## Path to production

| Area | Local setup | Production |
|---|---|---|
| Kafka | 1 broker, replication factor 1 | 3+ brokers, replication factor 3, `min.insync.replicas=2`, rack awareness |
| Schemas | Informal JSON | Avro or Protobuf with a schema registry and compatibility rules |
| Catalog | `iceberg-rest-fixture`, SQLite | Apache Polaris, Lakekeeper or Nessie, backed by Postgres, with access control |
| Storage | SeaweedFS, one container | S3 or equivalent, with lifecycle rules |
| Query engine | DuckDB, in memory | Trino or Spark for large scans; DuckDB for local analysis |
| Lakehouse writes | Python writer | Kafka Connect Iceberg sink or Flink, for higher throughput |
| Table maintenance | None | Scheduled compaction, snapshot expiry and orphan file cleanup |
| OLTP | One Postgres container, schema from an init script | Managed Postgres with replicas and backups; schema migrations with a tool such as Alembic or Flyway |
| Monitoring | Prometheus, Alertmanager | Plus consumer lag alerts per group, DLQ rate, and writer commit latency |
| Deployment | Docker Compose | Kubernetes, with Kafka run by an operator such as Strimzi |
| Secrets | Defaults in Compose | A secret manager; no defaults outside local development |

## Running and verifying

From `fraud-detection-system/`:

```bash
# Start the pipeline
docker compose -f docker-compose.system.yml up -d --build cdr-ingestion ml-service ml-service2 lakehouse-writer

# Send the sample dataset (800 CDRs)
python ingest-auto/ingest-auto.py --batch-size 50

# Consumer lag for both groups (LAG should reach 0)
docker exec kafka_1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups

# Query the lakehouse: row counts, duplicates, fraud by type, time travel
docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer python query.py

# Load test (adds synthetic CDRs; best run in a separate Compose project, e.g. -p cdr-loadtest)
python loadtest/loadtest.py --records 10000
```
