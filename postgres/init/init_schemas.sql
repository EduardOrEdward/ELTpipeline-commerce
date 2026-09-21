-- Initialize the database schemas used by the ELT pipeline.
-- PostgreSQL executes files in /docker-entrypoint.d only when the database
-- volume is initialized for the first time.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS mart;

-- ============================================================================
-- BRONZE: raw source data
-- ============================================================================
-- Bronze deliberately does not clean, deduplicate, or apply business rules.

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

-- ============================================================================
-- SILVER: cleaned and typed data
-- ============================================================================
-- Silver extracts fields from raw JSON, validates basic domain rules, and
-- keeps one current record per source identifier.

CREATE OR REPLACE VIEW silver.orders AS
SELECT DISTINCT ON (order_id)
    order_id,
    product_id,
    supplier_id,
    planned_quantity,
    order_date,
    expected_delivery_date,
    ingested_at
FROM (
    SELECT
        raw_payload->>'order_id' AS order_id,
        (raw_payload->>'product_id')::INTEGER AS product_id,
        (raw_payload->>'supplier_id')::INTEGER AS supplier_id,
        (raw_payload->>'planned_quantity')::INTEGER AS planned_quantity,
        (raw_payload->>'order_date')::DATE AS order_date,
        (raw_payload->>'expected_delivery_date')::DATE AS expected_delivery_date,
        ingested_at
    FROM bronze.orders
) AS source
WHERE order_id IS NOT NULL
  AND product_id > 0
  AND supplier_id > 0
  AND planned_quantity > 0
ORDER BY order_id, ingested_at DESC;

CREATE OR REPLACE VIEW silver.deliveries AS
SELECT DISTINCT ON (delivery_id)
    delivery_id,
    order_id,
    actual_quantity,
    actual_delivery_date,
    ingested_at
FROM (
    SELECT
        raw_payload->>'delivery_id' AS delivery_id,
        raw_payload->>'order_id' AS order_id,
        (raw_payload->>'actual_quantity')::INTEGER AS actual_quantity,
        (raw_payload->>'actual_delivery_date')::DATE AS actual_delivery_date,
        ingested_at
    FROM bronze.deliveries
) AS source
WHERE delivery_id IS NOT NULL
  AND order_id IS NOT NULL
  AND actual_quantity > 0
ORDER BY delivery_id, ingested_at DESC;

CREATE OR REPLACE VIEW silver.inventory AS
SELECT
    product_id,
    quantity,
    snapshot_date,
    ingested_at
FROM (
    SELECT
        (raw_payload->>'product_id')::INTEGER AS product_id,
        (raw_payload->>'quantity')::INTEGER AS quantity,
        (raw_payload->>'snapshot_date')::DATE AS snapshot_date,
        ingested_at
    FROM bronze.inventory
) AS source
WHERE product_id > 0
  AND quantity >= 0;

-- ============================================================================
-- MART: business-facing analytical models
-- ============================================================================

CREATE OR REPLACE VIEW mart.supplier_reliability AS
SELECT
    o.supplier_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT d.delivery_id) AS delivered_orders,
    COALESCE(SUM(o.planned_quantity), 0) AS planned_quantity,
    COALESCE(SUM(d.actual_quantity), 0) AS delivered_quantity,
    ROUND(
        COALESCE(
            SUM(d.actual_quantity)::NUMERIC / NULLIF(SUM(o.planned_quantity), 0),
            0
        ),
        4
    ) AS fulfillment_rate,
    ROUND(
        AVG(
            CASE
                WHEN d.delivery_id IS NOT NULL
                THEN GREATEST(
                    0,
                    d.actual_delivery_date - o.expected_delivery_date
                )
            END
        ),
        2
    ) AS avg_delivery_delay_days
FROM silver.orders AS o
LEFT JOIN silver.deliveries AS d
    ON d.order_id = o.order_id
GROUP BY o.supplier_id;

CREATE OR REPLACE VIEW mart.inventory AS
SELECT DISTINCT ON (product_id)
    product_id,
    quantity,
    snapshot_date,
    CASE
        WHEN quantity = 0 THEN TRUE
        ELSE FALSE
    END AS is_out_of_stock
FROM silver.inventory
ORDER BY product_id, snapshot_date DESC, ingested_at DESC;
