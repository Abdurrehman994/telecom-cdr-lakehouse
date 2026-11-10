-- Fraud alerts and their case status (OLTP).
-- Runs once, when the Postgres data volume is first created.
--
-- transaction_id is the primary key, so storing an alert is idempotent:
-- when Kafka redelivers a CDR (at-least-once), the second insert is a no-op
-- and no duplicate alert is raised.

CREATE TABLE IF NOT EXISTS fraud_alerts (
    transaction_id  TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    mobile_number   TEXT,
    fraud_type      TEXT,
    method          TEXT NOT NULL,          -- rules, random_forest, llm, scoring_fallback
    confidence      DOUBLE PRECISION NOT NULL,
    explanation     TEXT,
    amount_eur      DOUBLE PRECISION,
    event_time      TIMESTAMPTZ,            -- when the transaction happened
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'open'
                    CHECK (status IN ('open', 'confirmed', 'dismissed')),
    analyst_note    TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    kafka_partition INTEGER,
    kafka_offset    BIGINT
);

-- analysts work through open alerts, newest first
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_status_detected
    ON fraud_alerts (status, detected_at DESC);

-- all alerts for one customer
CREATE INDEX IF NOT EXISTS idx_fraud_alerts_customer
    ON fraud_alerts (customer_id);
