-- depends_on: {{ ref('raw_signups_validation') }}

{% if elementary.get_config_var('validation') %}
    with source as (
        select * from {{ ref('raw_signups_validation') }}
    ),

{% else %}
    with source as (
        select * from {{ ref('raw_signups_training') }}
    ),
{% endif %}

renamed as (

    select
        id as signup_id,
        user_id as customer_id,
        user_email as customer_email,
        hashed_password,
        signup_date

    from source

)

select * from renamed
