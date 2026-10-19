{{ config(materialized='table') }}

with order_metrics as (
    -- Aggregate orders before joining deliveries so planned quantities are
    -- counted once per order.
    select
        supplier_id,
        count(*)::bigint as total_orders,
        sum(planned_quantity)::bigint as planned_quantity
    from {{ ref('stg_orders') }}
    group by supplier_id
),

completed_delivery_metrics as (
    -- A delivery counts as completed only when its actual delivery date exists.
    -- The current API always generates this date; keeping the condition explicit
    -- makes the business definition clear and avoids counting future pending rows.
    select
        o.supplier_id,
        count(distinct d.order_id)::bigint as delivered_orders,
        sum(d.actual_quantity)::bigint as delivered_quantity,
        round(
            avg(d.actual_delivery_date - o.expected_delivery_date)::numeric,
            2
        ) as avg_delivery_delay_days
    from {{ ref('stg_deliveries') }} as d
    inner join {{ ref('stg_orders') }} as o
        on d.order_id = o.order_id
    where d.actual_delivery_date is not null
    group by o.supplier_id
)

select
    o.supplier_id,
    o.total_orders,
    coalesce(d.delivered_orders, 0)::bigint as delivered_orders,
    o.planned_quantity,
    coalesce(d.delivered_quantity, 0)::bigint as delivered_quantity,
    -- Delivery fulfillment is actual delivered quantity / planned quantity.
    -- Values above 1 are valid when the supplier delivers more than planned.
    round(
        coalesce(
            coalesce(d.delivered_quantity, 0)::numeric
                / nullif(o.planned_quantity, 0),
            0
        ),
        4
    ) as fulfillment_rate,
    d.avg_delivery_delay_days
from order_metrics as o
left join completed_delivery_metrics as d
    on d.supplier_id = o.supplier_id
