{{ config(materialized='view') }}

with source as (
    select
        raw_payload->>'delivery_id' as delivery_id,
        raw_payload->>'order_id' as order_id,
        (raw_payload->>'actual_quantity')::integer as actual_quantity,
        (raw_payload->>'actual_delivery_date')::date as actual_delivery_date,
        ingested_at
    from {{ source('bronze', 'deliveries') }}
),

deduplicated as (
    select distinct on (delivery_id)
        delivery_id,
        order_id,
        actual_quantity,
        actual_delivery_date,
        ingested_at
    from source
    where delivery_id is not null
      and order_id is not null
      and actual_quantity >= 0
    order by delivery_id, ingested_at desc
)

select *
from deduplicated
