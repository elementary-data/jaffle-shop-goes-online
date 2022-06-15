-- depends_on: {{ ref('raw_signups_validation') }}

{% if elementary.table_exists_in_target('raw_signups_validation') %}
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
