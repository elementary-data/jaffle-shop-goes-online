with source as (

    select * from {{ source('jaffle_app', 'raw_signups') }}

),

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
