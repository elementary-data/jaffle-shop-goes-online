-- depends_on: {{ ref('raw_payments_validation') }}

{% if elementary.get_config_var('validation') %}
    with source as (
        select * from {{ ref('raw_payments_validation') }}
    ),

{% else %}
    with source as (
        select * from {{ ref('raw_payments_training') }}
    ),
{% endif %}

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed
