{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('thelook_ecommerce', 'products') }}

),

renamed as (

    select
        cast(id as int64)                       as product_id,
        round(cast(cost as numeric), 2)         as cost,
        cast(category as string)                as category,
        cast(name as string)                    as product_name,
        cast(brand as string)                   as brand,
        round(cast(retail_price as numeric), 2) as retail_price,
        cast(department as string)              as department,
        cast(sku as string)                     as sku,
        cast(distribution_center_id as int64)   as distribution_center_id
    from source

)

select *
from renamed

