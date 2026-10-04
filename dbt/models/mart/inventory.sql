{{ config(materialized='table') }}

select distinct on (product_id)
    product_id,
    quantity,
    snapshot_date,
    quantity = 0 as is_out_of_stock
from {{ ref('stg_inventory') }}
order by product_id, snapshot_date desc, ingested_at desc
