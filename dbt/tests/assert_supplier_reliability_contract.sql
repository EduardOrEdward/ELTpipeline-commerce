-- Gold rows must satisfy the supplier_reliability output contract.
-- Negative average delays are valid: they mean deliveries arrived early.
-- fulfillment_rate may exceed 1 when actual delivered quantity exceeds the plan.
select
    supplier_id,
    total_orders,
    delivered_orders,
    planned_quantity,
    delivered_quantity,
    fulfillment_rate,
    avg_delivery_delay_days
from {{ ref('supplier_reliability') }}
where supplier_id is null
   or total_orders is null
   or total_orders <= 0
   or delivered_orders is null
   or delivered_orders < 0
   or delivered_orders > total_orders
   or planned_quantity is null
   or planned_quantity <= 0
   or delivered_quantity is null
   or delivered_quantity < 0
   or fulfillment_rate is null
   or fulfillment_rate < 0
   or fulfillment_rate is distinct from round(
       delivered_quantity::numeric / nullif(planned_quantity, 0),
       4
   )
