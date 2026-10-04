{{ config(materialized='view') }}

with source as (
    select
        raw_payload->>'order_id' as order_id,
        (raw_payload->>'product_id')::integer as product_id,
        (raw_payload->>'supplier_id')::integer as supplier_id,
        (raw_payload->>'planned_quantity')::integer as planned_quantity,
        (raw_payload->>'order_date')::date as order_date,
        (raw_payload->>'expected_delivery_date')::date as expected_delivery_date,
        ingested_at
    from {{ source('bronze', 'orders') }}
),

deduplicated as (
    select distinct on (order_id)
        order_id,
        product_id,
        supplier_id,
        planned_quantity,
        order_date,
        expected_delivery_date,
        ingested_at
    from source
    where order_id is not null
      and product_id > 0
      and supplier_id > 0
      and planned_quantity > 0
    order by order_id, ingested_at desc
)

select *
from deduplicated
