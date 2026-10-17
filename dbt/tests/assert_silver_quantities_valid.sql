-- Silver exposes only quantities that satisfy the source model contract.
select
    'orders' as dataset,
    order_id as record_id,
    planned_quantity::bigint as quantity
from {{ ref('stg_orders') }}
where planned_quantity <= 0

union all

select
    'deliveries' as dataset,
    delivery_id as record_id,
    actual_quantity::bigint as quantity
from {{ ref('stg_deliveries') }}
where actual_quantity < 0

union all

select
    'inventory' as dataset,
    product_id::text as record_id,
    quantity::bigint as quantity
from {{ ref('stg_inventory') }}
where quantity < 0
