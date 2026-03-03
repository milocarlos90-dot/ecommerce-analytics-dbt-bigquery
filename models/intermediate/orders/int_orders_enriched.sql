with orders as (

    select *
    from {{ ref('stg_orders') }}

),

normalized as (

    select
        o.*,

        -- normalize raw status
        case
            when lower(status) = 'complete' then 'completed'
            when lower(status) = 'shipped' then 'shipped'
            when lower(status) = 'delivered' then 'delivered'
            when lower(status) = 'returned' then 'returned'
            when lower(status) = 'cancelled' then 'cancelled'
            when lower(status) = 'processing' then 'processing'
            when lower(status) = 'pending' then 'pending'
            else 'unknown'
        end as status_standardized

    from orders o

)

select
    n.*,

    created_at as order_created_at,
    date(created_at) as order_created_date,

    -- lifecycle flags derived from CLEAN status
    status_standardized = 'completed'  as is_completed_order,
    status_standardized = 'cancelled'  as is_cancelled_order,
    status_standardized = 'returned'   as is_returned_order,
    status_standardized = 'shipped'    as is_shipped_order,
    status_standardized = 'processing' as is_processing_order

from normalized n