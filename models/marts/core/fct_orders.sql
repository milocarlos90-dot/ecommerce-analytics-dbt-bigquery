with orders as (

    select *
    from {{ ref('int_orders_enriched') }}

),

financials as (

    select *
    from {{ ref('int_orders_financials') }}

),

customers as (

    select
        customer_id,
        customer_sk
    from {{ ref('dim_customers') }}

),

final as (

    select

        -- FACT PRIMARY KEY
        o.order_id,

        -- DIMENSION KEYS
        c.customer_sk,

        -- DATES
        o.order_created_at,
        o.order_created_date,

        -- ORDER FLAGS
        o.is_completed_order,
        o.is_cancelled_order,
        o.is_returned_order,
        o.is_shipped_order,
        o.is_processing_order,

        -- MEASURES
        f.item_count,
        f.distinct_products,
        f.gross_revenue,
        f.completed_revenue,
        f.cancelled_revenue,
        f.returned_revenue,
        f.shipped_revenue,
        f.processing_revenue,
        current_timestamp() as dbt_updated_at

    from orders o
    left join financials f
    on  o.order_id = f.order_id

    left join customers c
    on o.customer_id = c.customer_id

)

select *
from final