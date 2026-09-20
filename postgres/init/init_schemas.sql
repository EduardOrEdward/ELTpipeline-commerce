-- Initialize the database schemas used by the ELT pipeline.
-- PostgreSQL executes files in /docker-entrypoint-initdb.d
-- only when the database volume is initialized for the first time.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS mart;

-- Bronze keeps the data as received from the source.
-- No deduplication, cleaning, or business rules are applied here.
-- The raw JSON payload is preserved so later layers can reprocess it.

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
