-- depends_on: {{ ref('raw_customers_validation') }}

{% if elementary.get_config_var('validation') %}
    with source as (
        select * from {{ ref('raw_customers_validation') }}
    ),

{% else %}
    with source as (
        select * from {{ ref('raw_customers_training') }}
    ),
{% endif %}

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
