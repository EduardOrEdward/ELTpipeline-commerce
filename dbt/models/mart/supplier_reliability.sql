{{ config(materialized='table') }}

select
    o.supplier_id,
    count(distinct o.order_id) as total_orders,
    count(distinct d.delivery_id) as delivered_orders,
    coalesce(sum(o.planned_quantity), 0) as planned_quantity,
    coalesce(sum(d.actual_quantity), 0) as delivered_quantity,
    round(
        coalesce(
            sum(d.actual_quantity)::numeric / nullif(sum(o.planned_quantity), 0),
            0
        ),
        4
    ) as fulfillment_rate,
    round(
        avg(
            case
                when d.delivery_id is not null
                then greatest(
                    0,
                    d.actual_delivery_date - o.expected_delivery_date
                )
            end
        ),
        2
    ) as avg_delivery_delay_days
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_deliveries') }} as d
    on d.order_id = o.order_id
group by o.supplier_id
