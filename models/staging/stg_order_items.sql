with source as (

    select *
    from {{ source('thelook_ecommerce', 'order_items') }}

),

renamed as (

    select
        cast(id as int64)                     as order_item_id,
        cast(order_id as int64)               as order_id,
        cast(user_id as int64)                as customer_id,
        cast(product_id as int64)             as product_id,
        cast(inventory_item_id as int64)      as inventory_item_id,
        cast(status as string)                as status,
        cast(created_at as timestamp)         as created_at,
        cast(shipped_at as timestamp)         as shipped_at,
        cast(delivered_at as timestamp)       as delivered_at,
        cast(returned_at as timestamp)        as returned_at,
        round(cast(sale_price as numeric), 2) as sale_price
    from source

)

select *
from renamed
