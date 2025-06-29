with orders as(
    select * from {{ ref('orders')}}
),

last_order_date as (
    select max(order_date) as last_order_date from orders
)

select * from orders
where order_date = (select last_order_date from last_order_date)

