-- Reconcile Gold metrics against independently aggregated Silver inputs.
-- This is a broad reconciliation check; fixed-input unit tests separately verify
-- known expected values without reproducing this aggregation.
with expected_orders as (
    select
        supplier_id,
        count(*)::bigint as total_orders,
        sum(planned_quantity)::bigint as planned_quantity
    from {{ ref('stg_orders') }}
    group by supplier_id
),

expected_completed_deliveries as (
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
),

expected as (
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
    from expected_orders as o
    left join expected_completed_deliveries as d
        on d.supplier_id = o.supplier_id
)

select
    coalesce(g.supplier_id, e.supplier_id) as supplier_id,
    g.total_orders as gold_total_orders,
    e.total_orders as expected_total_orders,
    g.delivered_orders as gold_delivered_orders,
    e.delivered_orders as expected_delivered_orders,
    g.planned_quantity as gold_planned_quantity,
    e.planned_quantity as expected_planned_quantity,
    g.delivered_quantity as gold_delivered_quantity,
    e.delivered_quantity as expected_delivered_quantity,
    g.fulfillment_rate as gold_fulfillment_rate,
    e.fulfillment_rate as expected_fulfillment_rate,
    g.avg_delivery_delay_days as gold_avg_delivery_delay_days,
    e.avg_delivery_delay_days as expected_avg_delivery_delay_days
from {{ ref('supplier_reliability') }} as g
full outer join expected as e
    on g.supplier_id = e.supplier_id
where g.supplier_id is null
   or e.supplier_id is null
   or g.total_orders is distinct from e.total_orders
   or g.delivered_orders is distinct from e.delivered_orders
   or g.planned_quantity is distinct from e.planned_quantity
   or g.delivered_quantity is distinct from e.delivered_quantity
   or g.fulfillment_rate is distinct from e.fulfillment_rate
   or g.avg_delivery_delay_days is distinct from e.avg_delivery_delay_days
