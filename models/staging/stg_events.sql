with source as (

    select *
    from {{ source('thelook_ecommerce', 'events') }}

),

renamed as (

    select
        -- primary key
        cast(id as int64)                as event_id,

        -- foreign keys
        cast(user_id as int64)           as customer_id,

        -- session metadata
        cast(sequence_number as int64)   as session_sequence_number,
        cast(session_id as string)       as session_id,

        -- timestamps
        cast(created_at as timestamp)    as created_at,

        -- location data
        cast(ip_address as string)       as ip_address,
        cast(city as string)             as city,
        cast(state as string)            as state,
        cast(postal_code as string)      as postal_code,

        -- device / acquisition
        cast(browser as string)          as browser,
        cast(traffic_source as string)   as traffic_source,

        -- behavioral data
        cast(uri as string)              as uri,
        cast(event_type as string)       as event_type

    from source

)

select *
from renamed