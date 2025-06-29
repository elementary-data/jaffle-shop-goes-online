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
    from {{ ref("orders") }}
),

session_order_pairs as (
    -- For each session, find the next order from that customer
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
        row_number() over (
            partition by sessions.customer_id, sessions.started_at 
            order by orders.order_date
        ) as order_rank
    from sessions
    inner join orders 
        on sessions.customer_id = orders.customer_id 
        and orders.order_date >= sessions.started_at  -- Order must be after session
        and datediff('day', sessions.started_at, orders.order_date) <= 7  -- Within 1 week max (generous for jaffle shop)
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
    where order_rank = 1
    and days_to_order <= 2  -- Realistic timeframe: max 2 days for jaffle shop decision
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
