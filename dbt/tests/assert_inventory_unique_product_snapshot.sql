-- Silver inventory grain: one row per product and business snapshot date.
select
    product_id,
    snapshot_date,
    count(*) as row_count
from {{ ref('stg_inventory') }}
group by product_id, snapshot_date
having count(*) > 1
