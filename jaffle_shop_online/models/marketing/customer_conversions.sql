{{
    config(
        materialized = "view",
    )
}}

with sessions as (
    select *
    from {{ ref("sessions") }}
),

orders as (
    select *
    from {{ ref("total_orders") }}
),

session_order_pairs as (
    select 
        sessions.customer_id,
        sessions.started_at as session_date,
        sessions.utm_source,
        sessions.utm_medium,
        orders.order_date,
        orders.amount,
        orders.order_id,
        datediff('day', sessions.started_at, orders.order_date) as days_to_order,
        datediff('hour', sessions.started_at, orders.order_date) as hours_to_order,
        -- Pick the closest (most recent) eligible session for each order
        row_number() over (
            partition by orders.order_id 
            order by sessions.started_at desc
        ) as session_rank
    from sessions
    inner join orders 
        on sessions.customer_id = orders.customer_id 
        and orders.order_date >= sessions.started_at
        and datediff('day', sessions.started_at, orders.order_date) <= {{ var('conversion_window_days', 7) }}
),

conversions as (
    select 
        customer_id,
        session_date,
        utm_source,
        utm_medium,
        order_date as converted_at,
        amount as revenue,
        order_id,
        days_to_order,
        hours_to_order
    from session_order_pairs 
    where session_rank = 1
)

select
    customer_id,
    converted_at,
    revenue,
    session_date,
    utm_source,
    utm_medium,
    order_id,
    days_to_order,
    hours_to_order
from conversions
