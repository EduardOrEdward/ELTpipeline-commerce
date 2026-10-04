-- Initialize database infrastructure for the ELT pipeline.
-- PostgreSQL executes this file only when the database volume is initialized.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS mart;

-- Bronze stores raw source data without cleaning or business transformations.

CREATE TABLE IF NOT EXISTS bronze.orders (
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_object_key TEXT,
    raw_payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.deliveries (
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_object_key TEXT,
    raw_payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.inventory (
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_object_key TEXT,
    raw_payload JSONB NOT NULL
);

-- Silver and Mart relations are managed by dbt.
