{{
  config(
    materialized = 'view',
    )
}}

with orders as (
    select *
    from {{ ref('total_orders') }}
)

select *
from orders
where status = 'return_pending' or status = 'returned'
