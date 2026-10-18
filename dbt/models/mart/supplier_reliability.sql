{{ config(materialized='table') }}

with order_metrics as (
    -- Aggregate orders before joining deliveries so one order is never counted
    -- more than once in planned quantities.
    select
        supplier_id,
        count(*)::bigint as total_orders,
        sum(planned_quantity)::bigint as planned_quantity
    from {{ ref('stg_orders') }}
    group by supplier_id
),

delivery_metrics as (
    -- Delivery metrics are aggregated independently at supplier grain.
    select
        o.supplier_id,
        count(distinct d.order_id)::bigint as delivered_orders,
        sum(d.actual_quantity)::bigint as delivered_quantity,
        round(
            avg(
                greatest(
                    0,
                    d.actual_delivery_date - o.expected_delivery_date
                )
            )::numeric,
            2
        ) as avg_delivery_delay_days
    from {{ ref('stg_deliveries') }} as d
    inner join {{ ref('stg_orders') }} as o
        on d.order_id = o.order_id
    group by o.supplier_id
)

select
    o.supplier_id,
    o.total_orders,
    coalesce(d.delivered_orders, 0)::bigint as delivered_orders,
    o.planned_quantity,
    coalesce(d.delivered_quantity, 0)::bigint as delivered_quantity,
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
left join delivery_metrics as d
    on d.supplier_id = o.supplier_id
