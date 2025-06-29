with orders as(
    select 
        customer_id,
        order_id,
        {{ cents_to_dollars('amount') }} as amount,
        {{ cents_to_dollars('bank_transfer_amount') }} as bank_transfer_amount,
        {{ cents_to_dollars('coupon_amount') }} as coupon_amount,
        {{ cents_to_dollars('credit_card_amount') }} as credit_card_amount,
        {{ cents_to_dollars('gift_card_amount') }} as gift_card_amount,
        order_date
     from {{ ref('orders')}}
),

last_order_date as (
    select max(order_date) as last_order_date from orders
)

select * from orders
where order_date < (select last_order_date from last_order_date)

