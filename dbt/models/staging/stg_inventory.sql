{{ config(materialized='view') }}

select
    (raw_payload->>'product_id')::integer as product_id,
    (raw_payload->>'quantity')::integer as quantity,
    (raw_payload->>'snapshot_date')::date as snapshot_date,
    ingested_at
from {{ source('bronze', 'inventory') }}
where (raw_payload->>'product_id')::integer > 0
  and (raw_payload->>'quantity')::integer >= 0
