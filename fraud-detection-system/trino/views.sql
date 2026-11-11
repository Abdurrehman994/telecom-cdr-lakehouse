-- Deduplicated views over the sinks' tables, applied by the trino-init job at startup.
--
-- The sinks write every Kafka message exactly once, but a message can be in Kafka twice:
-- a client retries a CDR the API reported as failed after the broker had in fact stored it
-- (measured: 16 of 2,250 CDRs with two of three brokers down), and fraud decisions are
-- published at least once. These views keep one row per transaction_id.
--
-- A view's column list is fixed when it is created; trino-init recreates the views on
-- every start, so columns added by a new contract version appear after a restart.

-- the first copy of each CDR (ties on position across recreated topics are broken by
-- ingestion time; exact copies of one row collapse into one)
CREATE OR REPLACE VIEW cdr_events_unique AS
SELECT DISTINCT c.*
FROM cdr_events c
JOIN (
    SELECT transaction_id, min(ROW(ingested_at, kafka_partition, kafka_offset)) AS first_copy
    FROM cdr_events
    GROUP BY transaction_id
) f ON c.transaction_id = f.transaction_id
   AND ROW(c.ingested_at, c.kafka_partition, c.kafka_offset) = f.first_copy;

-- the latest decision for each CDR
CREATE OR REPLACE VIEW fraud_decisions_latest AS
SELECT DISTINCT d.*
FROM fraud_decisions d
JOIN (
    SELECT transaction_id, max(ROW(decided_at, kafka_partition, kafka_offset)) AS last_decision
    FROM fraud_decisions
    GROUP BY transaction_id
) l ON d.transaction_id = l.transaction_id
   AND ROW(d.decided_at, d.kafka_partition, d.kafka_offset) = l.last_decision;
