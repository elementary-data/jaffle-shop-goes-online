{{
    config(
        materialized = "view",
    )
}}

with customer as (
    select *
    from {{ ref("customers") }}
),

orders as (
    select *
    from {{ ref("orders") }}
),

customer_orders as (
        select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders,
        sum(amount) as revenue
    from orders
    group by customer_id
)

select
    customer.customer_id,
    customer_orders.first_order as converted_at,
    case when customer_orders.revenue is not null then customer_orders.revenue else 0 end as revenue 
from customer left join customer_orders on customer.customer_id = customer_orders.customer_id
