with source as (

    select *
    from {{ source('thelook_ecommerce', 'distribution_centers') }}

),

renamed as (

    select
        cast(id as int64)                               as distribution_center_id,
        cast(name as string)                            as distribution_center_name,
        cast(latitude as float64)                       as distribution_center_latitude,
        cast(longitude as float64)                      as distribution_center_longitude,
        distribution_center_geom                        as distribution_center_geom

    from source

)

select *
from renamed
