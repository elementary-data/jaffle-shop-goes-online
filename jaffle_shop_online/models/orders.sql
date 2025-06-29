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
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        order_payments.{{ payment_method }}_amount,

        {% endfor -%}

        order_payments.total_amount as amount

    from orders


    left join order_payments
        on orders.order_id = order_payments.order_id

),

recent_dates as (
    select distinct date(order_date) as order_day
    from final
    order by order_day desc
    limit 2
),

past_orders as (
    select
        order_id,
        customer_id,
        order_date,
        status,
        {{ cents_to_dollars('amount') }} as amount,
        {{ cents_to_dollars('bank_transfer_amount') }} as bank_transfer_amount,
        {{ cents_to_dollars('coupon_amount') }} as coupon_amount,
        {{ cents_to_dollars('credit_card_amount') }} as credit_card_amount,
        {{ cents_to_dollars('gift_card_amount') }} as gift_card_amount
    from final
    where date(order_date) not in (select order_day from recent_dates)
),

real_time_orders as (
    select 
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        bank_transfer_amount,
        coupon_amount,
        credit_card_amount,
        gift_card_amount
    from final
    where date(order_date) in (select order_day from recent_dates)
)

select * from past_orders
union all
select * from real_time_orders
