CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.raw_trials (
    nct_id      VARCHAR(20) PRIMARY KEY,
    raw_json    JSONB       NOT NULL,
    ingested_at TIMESTAMP   NOT NULL DEFAULT NOW()
);
