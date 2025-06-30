{{
  config(materialized='view')
}}

{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments as (
    select
        order_id,
        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}
        sum(amount) as total_amount
    from payments
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        {% for payment_method in payment_methods -%}
        op.{{ payment_method }}_amount,
        {% endfor -%}
        op.total_amount    as amount_cents
    from orders o
    left join order_payments op on o.order_id = op.order_id
)

select 
    order_id,
    customer_id,
    order_date,
    status,
    {{ cents_to_dollars('amount_cents') }} as amount,
    {{ cents_to_dollars('bank_transfer_amount') }} as bank_transfer_amount,
    {{ cents_to_dollars('coupon_amount') }} as coupon_amount,
    {{ cents_to_dollars('credit_card_amount') }} as credit_card_amount,
    {{ cents_to_dollars('gift_card_amount') }} as gift_card_amount
from final
where date(order_date) = (
    select date(max(order_date)) from final
) 