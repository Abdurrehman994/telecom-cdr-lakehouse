-- Example analytics on the lakehouse with Trino. Run with:
--   docker exec trino trino --catalog lakehouse --schema telecom -f /etc/trino/queries.sql

-- Fraud by type, from the CDRs
SELECT coalesce(fraud_type, 'none') AS fraud_type, count(*) AS cdrs, round(sum(amount_eur), 2) AS total_eur
FROM cdr_events
GROUP BY 1
ORDER BY cdrs DESC;

-- How each detection method did, joining the two sinks' tables through the
-- deduplicated views (trino/views.sql), one row per CDR and its latest decision
SELECT d.method, count(*) AS decisions, count_if(d.is_fraud) AS flagged,
       count_if(d.is_fraud AND c.fraud_flag) AS flagged_and_labelled_fraud
FROM fraud_decisions_latest d JOIN cdr_events_unique c ON c.transaction_id = d.transaction_id
GROUP BY 1
ORDER BY decisions DESC;

-- CDRs that reached Kafka more than once (client retries); the views hide them
SELECT count(*) - count(DISTINCT transaction_id) AS extra_copies FROM cdr_events;

-- Partition pruning: only the files of one day partition are read
SELECT count(*) AS cdrs_on_2025_01_01
FROM cdr_events
WHERE time_stamp >= TIMESTAMP '2025-01-01 00:00:00 UTC' AND time_stamp < TIMESTAMP '2025-01-02 00:00:00 UTC';

-- Iceberg metadata: the sink's commits, with the Kafka position stored in each
-- (element_at gives null for a key a snapshot does not have)
SELECT committed_at, operation, element_at(summary, 'added-records') AS added_rows,
       element_at(summary, 'kafka.cdr-events.offsets') AS kafka_offsets
FROM "cdr_events$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- Table health: files per partition
SELECT count(*) AS partitions, sum(file_count) AS files, sum(record_count) AS rows
FROM "cdr_events$partitions";
