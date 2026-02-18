{{ config(materialized='view') }}

select
    id,
    cost,
    category,
    name,
    brand,
    retail_price,
    department,
    sku,
    distribution_center_id
from {{ source('thelook_ecommerce', 'products') }}
