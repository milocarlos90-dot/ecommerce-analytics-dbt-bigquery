with products as (

    select
        product_id,
        cost,
        category,
        product_name,
        brand,
        retail_price,
        department,
        sku

    from {{ ref('stg_products') }}

),

final as (

    select
        -- Keys
        product_id,
        sku,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,


        -- product type
        category,
        product_name,
        brand,

        -- pricing
        cost,
        retail_price,

        -- internal
        department


    from products

)

select *
from final