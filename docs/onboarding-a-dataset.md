# Onboarding a dataset

How a team gets its events into Kafka with a data contract and into the lakehouse as an Iceberg table. It takes three files and no new code on the platform side. `fraud-decisions` was onboarded this way; its files are the example below.

## 1. Write the contract

Add an Avro schema to [`fraud-detection-system/contracts/`](../fraud-detection-system/contracts/) and list it in `contracts.yaml`:

```yaml
  - topic: fraud-decisions
    schema: fraud-decisions-value.avsc
    compatibility: FULL_TRANSITIVE
    owner: fraud-detection
```

The schema must be a record. Each field becomes a table column, so stick to the supported types: primitives, enums, `timestamp-micros`/`-millis`, `local-timestamp-*`, `date`, `decimal`, `uuid`, and unions of `null` with one of these. Nested records, arrays and maps are rejected for now. Give optional fields `"type": ["null", ...]` and `"default": null`.

Check it locally against a running registry:

```bash
pip install fraud-detection-system/libs/dataplatform
python -m dataplatform.contracts check fraud-detection-system/contracts --registry-url http://localhost:8081
```

The `schema-init` job registers contracts when the stack starts. If a contract breaks its compatibility rule, the job fails, and the services that depend on it do not start.

## 2. Create the topic

Add the topic and its dead-letter topic to `kafka-init` in `docker-compose.system.yml`:

```sh
kafka-topics.sh ... --create --if-not-exists --topic fraud-decisions --partitions 3 --replication-factor 1
kafka-topics.sh ... --create --if-not-exists --topic fraud-decisions-dlq --partitions 1 --replication-factor 1
```

## 3. Produce with the contract

Use the platform library, so messages are Avro in the wire format every consumer understands:

```python
from dataplatform import Encoder, registry_client

encoder = Encoder.from_file(registry_client(), "fraud-decisions", "contracts/fraud-decisions-value.avsc")
producer.produce("fraud-decisions", key=customer_id.encode(), value=encoder.encode(decision))
```

`encode` raises `ContractViolation` for a record that does not match the schema, and fails while the schema is not registered. A producer can never put data on the topic that its consumers cannot read.

Consumers decode with `Decoder(registry_client()).decode(msg.value())`, which reads each message with the schema it was written with. Treat `InvalidMessage` as a dead letter. Any other error, such as the registry being unreachable, is temporary: retry it rather than dead-lettering a valid message.

For tests, `dataplatform.testing.InMemoryRegistry` stands in for the registry.

## 4. Add a sink

Add a file to [`fraud-detection-system/lakehouse/sinks/`](../fraud-detection-system/lakehouse/sinks/):

```yaml
name: fraud_decisions
source:
  topic: fraud-decisions
  consumer_group: lakehouse-fraud-decisions
  dead_letter_topic: fraud-decisions-dlq
table:
  identifier: telecom.fraud_decisions
  partition_by:
    - {column: event_time, transform: day, name: event_day}
  sort_by: event_time
flush:
  max_records: 500
  interval_seconds: 10
maintenance:
  compact_min_files: 5
  snapshot_retain_last: 10
  snapshot_max_age_hours: 24
  orphan_min_age_minutes: 1440
```

| Setting | Meaning |
|---|---|
| `partition_by` | Iceberg partition transforms: `identity`, `year`, `month`, `day`, `hour`, `bucket[N]`, `truncate[W]` |
| `sort_by` | Column compaction sorts each rewritten file by |
| `flush` | A batch is appended when it reaches `max_records` or `interval_seconds` has passed; bigger batches mean fewer files but later data |
| `maintenance.compact_min_files` | Compact a day partition once it holds this many files. Needs a `day` partition; `0` turns compaction off |
| `maintenance.snapshot_*` | Expire snapshots older than `max_age_hours`, always keeping the newest `retain_last` (the time travel window) |
| `maintenance.orphan_min_age_minutes` | Only delete unreferenced files older than this (never under 15 minutes) |

Unknown keys, a column the contract does not have, or a transform that does not fit the column type stop the writer at startup with an error naming the problem.

Then add a writer for it to `docker-compose.system.yml`, a copy of `lakehouse-writer-fraud-decisions` with the sink file changed, and add it to the `lakehouse-writer` scrape job in `monitoring/prometheus/prometheus.yml`. `lakehouse-maintenance` picks up every sink file by itself.

## What you get

- An Iceberg table with a column per contract field, plus `kafka_partition`, `kafka_offset` and `ingested_at` for lineage.
- Exactly-once delivery from the topic into the table, the same as `cdr_events`.
- Schema evolution: register a new contract version with an extra optional field, and the sink adds the column when the first message with it arrives. Older rows read it as null.
- Compaction, snapshot expiry and orphan cleanup with your policy.
- Metrics labelled with your table, the table picker on the CDR Pipeline dashboard, and the lakehouse alerts.
- Messages that cannot be written go to your dead-letter topic, with the error, source partition and offset as headers.

## Changing a contract

With `FULL_TRANSITIVE`, every version must read data written by every other version, so producers and consumers can upgrade in any order and the lakehouse can read all of its history:

| Change | Allowed |
|---|---|
| Add a field with a default | Yes; it becomes a new column |
| Remove a field that has a default | Yes; the column stays and new rows hold null |
| Add a field without a default | No |
| Remove a required field | No |
| Change a field's type | No (create a new field instead) |
| Rename a field | No (add the new name with a default, stop writing the old one) |
