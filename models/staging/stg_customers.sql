with source as (

    select *
    from {{ source('thelook_ecommerce', 'users') }}

),

renamed as (

    select
        cast(id as int64)                    as customer_id,
        cast(first_name as string)           as first_name,
        cast(last_name as string)            as last_name,
        cast(email as string)                as email,
        cast(age as int64)                   as age,
        cast(gender as string)               as gender,
        cast(state as string)                as state,
        cast(street_address as string)       as street_address,
        cast(postal_code as string)          as postal_code,
        cast(city as string)                 as city,
        cast(country as string)              as country,
        cast(latitude as float64)            as latitude,
        cast(longitude as float64)           as longitude,
        cast(traffic_source as string)       as traffic_source,
        cast(created_at as timestamp)        as created_at,
        user_geom                            as user_geom
    from source

)

select *
from renamed
