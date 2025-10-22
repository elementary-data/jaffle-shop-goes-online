{{
  config(
    materialized = 'view',
    )
}}

with orders as (
    select *
    from {{ ref('orders') }}
)

select 
  {# This if case is used to fail the test schema_changes_from_baseline for the lost_orders model #}
  {% if elementary.get_config_var('validation') %}
    CONCAT('ID-', order_id) as order_id,
  {% else %}
    order_id,
  {% endif %}
  customer_id,
  order_date,
  status,
  amount,
  credit_card_amount,
  coupon_amount,
  bank_transfer_amount
from orders
where status = 'return_pending' or status = 'shipped'
