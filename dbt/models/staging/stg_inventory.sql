{{ config(materialized='view') }}

with source as (
    select
        (raw_payload->>'product_id')::integer as product_id,
        (raw_payload->>'quantity')::integer as quantity,
        (raw_payload->>'snapshot_date')::date as snapshot_date,
        ingested_at,
        source_object_key,
        record_index
    from {{ source('bronze', 'inventory') }}
    where (raw_payload->>'product_id')::integer > 0
      and (raw_payload->>'quantity')::integer >= 0
      and (raw_payload->>'snapshot_date') is not null
),

deduplicated as (
    select distinct on (product_id, snapshot_date)
        product_id,
        quantity,
        snapshot_date,
        ingested_at
    from source
    order by
        product_id,
        snapshot_date,
        ingested_at desc,
        source_object_key desc,
        record_index desc
)

select *
from deduplicated
