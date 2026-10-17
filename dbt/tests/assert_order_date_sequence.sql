-- An order's expected delivery date must not precede the order date.
select
    order_id,
    order_date,
    expected_delivery_date
from {{ ref('stg_orders') }}
where expected_delivery_date < order_date
