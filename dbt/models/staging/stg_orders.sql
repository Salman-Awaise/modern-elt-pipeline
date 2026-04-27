with source as (
    select * from {{ source('raw', 'orders') }}
),

typed as (
    select
        cast(order_id as integer) as order_id,
        cast(customer_id as integer) as customer_id,
        cast(order_date as date) as order_date,
        lower(trim(status)) as status,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(12, 2)) as unit_price,
        upper(trim(country)) as country,
        cast(quantity as integer) * cast(unit_price as numeric(12, 2)) as gross_revenue
    from source
)

select * from typed
