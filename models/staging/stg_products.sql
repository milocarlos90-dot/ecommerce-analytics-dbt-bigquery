{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('thelook_ecommerce', 'products') }}

),

renamed as (

    select
        cast(id as int64)                       as product_id,
        cast(cost as numeric)                   as cost,
        cast(category as string)                as category,
        cast(name as string)                    as product_name,
        cast(brand as string)                   as brand,
        cast(retail_price as numeric)           as retail_price,
        cast(department as string)              as department,
        cast(sku as string)                     as sku,
        cast(distribution_center_id as int64)   as distribution_center_id
    from source

)

select *
from renamed

