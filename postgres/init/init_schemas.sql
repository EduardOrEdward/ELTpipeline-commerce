-- Initialize database infrastructure for the ELT pipeline.
-- PostgreSQL executes this file only when the database volume is initialized.

CREATE DATABASE metabase;

\connect elt_pipeline

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS mart;

-- Bronze stores raw source data without cleaning or business transformations.
-- record_index gives each record a stable position inside its immutable S3 object.

CREATE TABLE IF NOT EXISTS bronze.orders (
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_object_key TEXT,
    record_index INTEGER,
    raw_payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.deliveries (
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_object_key TEXT,
    record_index INTEGER,
    raw_payload JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.inventory (
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_object_key TEXT,
    record_index INTEGER,
    raw_payload JSONB NOT NULL
);

-- A row exists here only after the corresponding completed snapshot and all its
-- Bronze records have committed in the same transaction.
CREATE TABLE IF NOT EXISTS bronze.loaded_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    manifest_key TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS orders_source_record_uidx
    ON bronze.orders (source_object_key, record_index);
CREATE UNIQUE INDEX IF NOT EXISTS deliveries_source_record_uidx
    ON bronze.deliveries (source_object_key, record_index);
CREATE UNIQUE INDEX IF NOT EXISTS inventory_source_record_uidx
    ON bronze.inventory (source_object_key, record_index);

-- Silver and Mart relations are managed by dbt.
