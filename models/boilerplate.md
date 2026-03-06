# 📘 SQL MILO -- Time saving Boilerplate

------------------------------------------------------------------------

## 🧠 Core Rule Before Writing Any Query

Always define: - What is the grain? - Will this join fan out? - Is this
metric event, session, order, or customer level? - Should row count be
preserved?

------------------------------------------------------------------------

# 1️⃣ Basic SELECT

``` sql
select column_1, column_2
from table_name;
```

``` sql
select distinct column_name
from table_name;
```

------------------------------------------------------------------------

# 2️⃣ Filtering (WHERE)

``` sql
select *
from table_name
where column = 'value'
  and numeric_column > 10
  and date_column >= '2024-01-01';
```

Common conditions:

    where column is null
    where column is not null
    where column in ('A','B')
    where column between 1 and 10
    where lower(column) like '%test%'

------------------------------------------------------------------------

# 3️⃣ CASE WHEN

``` sql
select
    case
        when status = 'completed' then 'success'
        when status = 'cancelled' then 'failed'
        else 'other'
    end as status_group
from orders;
```

Conditional metric:

``` sql
select
    sum(case when status = 'completed' then revenue else 0 end)
        as completed_revenue
from orders;
```

------------------------------------------------------------------------

# 4️⃣ Aggregations

``` sql
select
    category,
    count(*) as total_rows,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from table_name
group by category;
```

Common helpers:

    count(*)
    count(column)
    count(distinct column)
    countif(condition)          -- BigQuery
    sum(column)
    avg(column)
    min(column)
    max(column)

------------------------------------------------------------------------

# 5️⃣ HAVING

``` sql
select customer_id, count(*) as order_count
from orders
group by customer_id
having count(*) > 5;

``` 
select
    journey_session_id,

    countif(customer_id is null) as anonymous_events,
    countif(customer_id is not null) as known_events,

    min(event_ts) as session_start,
    max(event_ts) as session_end

from `ecommerce-analytics-training.dbt_dev_mart.fct_events`
group by journey_session_id

having
    countif(customer_id is null) > 0
    and countif(customer_id is not null) > 0

order by session_start
--- check for anonymous_events
```
select
    journey_session_id,
    count(distinct customer_id) as distinct_customer_ids
from `ecommerce-analytics-training.dbt_dev_mart.fct_events`
where customer_id is not null
group by journey_session_id
having distinct_customer_ids > 1
--- No single journey_session_id has multiple different customers.
```

------------------------------------------------------------------------

# 6️⃣ CTE (Common Table Expression)

``` sql
with base as (
    select * from orders
),
filtered as (
    select * from base where status = 'completed'
),
aggregated as (
    select customer_id, count(*) as order_count
    from filtered
    group by customer_id
)
select * from aggregated;
```

------------------------------------------------------------------------

# 7️⃣ Subqueries

Scalar:

``` sql
select (select count(*) from orders) as total_orders;
```

IN subquery:

``` sql
select *
from customers
where customer_id in (
    select customer_id from orders
);
```

------------------------------------------------------------------------

# 8️⃣ Joins

``` sql
select *
from orders o
join customers c
    on o.customer_id = c.customer_id;
```

``` sql
select *
from orders o
left join customers c
    on o.customer_id = c.customer_id;
```

Join fan-out check:

``` sql
select
    count(*) as total_rows,
    count(distinct primary_key) as distinct_keys
from joined_table;
```

------------------------------------------------------------------------

# 9️⃣ Set Operations

``` sql
select column from table_a
union all
select column from table_b;
```

``` sql
select column from table_a
union
select column from table_b;
```

``` sql
select id from table_a
except distinct
select id from table_b;
```

------------------------------------------------------------------------

# 🔟 Window Functions (BigQuery / Snowflake)

Row number dedupe:

``` sql
select *
from customers
qualify row_number() over (
    partition by customer_id
    order by updated_at desc
) = 1;
```

Rank:

``` sql
select
    customer_id,
    revenue,
    rank() over (order by revenue desc) as revenue_rank
from customers;
```

Running total:

``` sql
select
    order_date,
    sum(revenue) over (
        order by order_date
        rows between unbounded preceding and current row
    ) as running_revenue
from orders;
```

Lag:

``` sql
select
    customer_id,
    event_ts,
    lag(event_ts) over (
        partition by customer_id
        order by event_ts
    ) as previous_event_ts
from events;
```

Filter window output:

``` sql
select *
from events
qualify lag(event_ts) over (
    partition by customer_id
    order by event_ts
) is null;
```

------------------------------------------------------------------------

# 1️⃣1️⃣ Date & Time (BigQuery)

``` sql
date(timestamp_column)
date_trunc(date_column, month)
extract(year from date_column)
timestamp_diff(end_ts, start_ts, second)
current_date()
current_timestamp()
```

------------------------------------------------------------------------

# 1️⃣2️⃣ Null Handling & Safe Math

``` sql
coalesce(column, 0)
ifnull(column, 0)
safe_divide(numerator, denominator)
```

------------------------------------------------------------------------

# 1️⃣3️⃣ Data Quality / QA Validation

Grain validation:

``` sql
select count(*) as total_rows,
       count(distinct primary_key) as distinct_keys
from table_name;
```

Row count comparison:

``` sql
select
  (select count(*) from table_a) as a_count,
  (select count(*) from table_b) as b_count;
```

Missing keys:

``` sql
select primary_key from table_a
except distinct
select primary_key from table_b;
```

Null drift:

``` sql
select
    countif(column is null) as null_count,
    count(*) as total_rows,
    safe_divide(countif(column is null), count(*)) as null_rate
from table_name;
```

------------------------------------------------------------------------

# 1️⃣4️⃣ Analytics Patterns

Session conversion rate:

``` sql
select
    count(*) as total_sessions,
    countif(has_conversion = 1) as converted_sessions,
    safe_divide(countif(has_conversion = 1), count(*)) as conversion_rate
from fct_customer_sessions;
```

Funnel step counts:

``` sql
select
    count(distinct session_id) as sessions,
    count(distinct case
        when event_category = 'cart_interaction'
        then session_id end) as cart_sessions
from fct_events;
```

Cohort example:

``` sql
with first_purchase as (
    select customer_id, min(order_date) as cohort_date
    from orders
    group by customer_id
)
select
    cohort_date,
    order_date,
    count(distinct o.customer_id) as active_customers
from orders o
join first_purchase f
    on o.customer_id = f.customer_id
group by 1,2;
```

------------------------------------------------------------------------

General checks:

``` sql

SELECT
  DATE_TRUNC(DATE(order_created_date), WEEK) AS order_week,
  COUNT(*) AS orders,
  ROUND(SUM(gross_revenue) ,0) AS rev,
  ROUND(SUM(gross_revenue) /  COUNT(*),0)  AS avg_rev,
   COUNT(DISTINCT customer_sk) AS customers,
     ROUND(AVG(item_count),2) AS avg_items_per_order
FROM `ecommerce-analytics-training.dbt_prod_marts.fct_orders`
WHERE DATE(order_created_date) BETWEEN '2025-02-15' AND '2026-03-03'
GROUP BY order_week
ORDER BY order_week;

```

SELECT
journey_session_id
FROM `ecommerce-analytics-training.dbt_prod_marts.fct_events` 
GROUP BY journey_session_id
HAVING SUM(CASE WHEN event_type = 'product' THEN 1 ELSE 0 END) = 0
 
```
------------------------------------------------------------------------

# ⚡ Final Checklist

1.  What is the grain?
2.  Can this join duplicate rows?
3.  Is aggregation correct?
4.  Should row count match upstream?
5.  Does this metric belong at event, session, order, or customer level?
