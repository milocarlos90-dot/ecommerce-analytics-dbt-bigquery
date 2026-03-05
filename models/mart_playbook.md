# 🚀 Mart Playbook

## dbt • Dimensional Modeling • Grain Design • Fan-Out Prevention

------------------------------------------------------------------------

# 🧠 1. Grain Design (Most Important Concept)

Before building any model:

-   What does **one row represent**?
-   Is it event-level, session-level, order-level, or customer-level?
-   Should row count match upstream?

Golden Rule: If you cannot clearly state the grain in one sentence, the
model is not ready.

Example:

fct_events → 1 row = 1 event\
fct_sessions → 1 row = 1 customer session\
fct_orders → 1 row = 1 order

------------------------------------------------------------------------

# 🏗 2. Layering Strategy in dbt

## Staging (stg\_)

Purpose: - Rename - Cast types - Light cleaning - No business logic

Should NOT: - Aggregate - Join unrelated tables - Create metrics

------------------------------------------------------------------------

## Intermediate (int\_)

Purpose: - Business transformations - Window functions -
Sessionization - Derived fields - Aggregations at new grain

Example: - int_orders_financials (order-level aggregation) -
int_events_sessionized (session logic)

------------------------------------------------------------------------

## Marts (dim\_ / fct\_)

Purpose: - Conformed dimensions - Fact tables at clear business grain -
Ready for BI

Facts: Contain measures and foreign keys.

Dimensions: Contain descriptive attributes.

------------------------------------------------------------------------

# 🔁 3. When to Create an Intermediate Model

Create shared int\_ model if:

-   Complex joins
-   Window functions
-   Reusable enrichment logic
-   Shared logic across multiple fact tables

Avoid: "God models" with unrelated transformations.

------------------------------------------------------------------------

# ⚠️ 4. Fan-Out Prevention

Fan-out happens when a join increases row count unintentionally.

QA Check:

``` sql
select
    count(*) as total_rows,
    count(distinct primary_key) as distinct_keys
from model;
```

If total_rows \> distinct_keys → duplication occurred.

Prevention:

-   Aggregate before join
-   Join on correct keys
-   Validate grain before and after join

------------------------------------------------------------------------

# 🧮 5. Fact Table Design

Fact tables should:

-   Have a clearly defined grain
-   Contain only foreign keys + measures
-   Avoid descriptive text columns when possible
-   Have contract enforcement

Bad Practice: Mixing event-level and session-level metrics in one fact
table.

------------------------------------------------------------------------

# 🗂 6. Dimension Design

Dimensions:

-   One row per business entity
-   Stable keys (surrogate keys preferred)
-   Avoid volatile metrics
-   Have contract enforcement

dim_customers: - customer_sk (PK) - customer_id (natural key) -
descriptive attributes only

------------------------------------------------------------------------

# 🔗 7. Relationship Testing Strategy

Relationship tests should validate business relationships, not pipeline
chaining.

Correct:

-   fct_orders.customer_sk → dim_customers.customer_sk
-   int_orders_enriched.customer_id → int_customers.customer_id

Avoid: Testing direct upstream dependency if equal_rowcount already
guarantees it.

------------------------------------------------------------------------

# 📊 8. Event vs Session Modeling

Events roll up to sessions.

-   Events = atomic behavior
-   Sessions = grouped events
-   Orders = grouped order items

Never aggregate sessions into events.

------------------------------------------------------------------------

# 🎯 9. Funnel Modeling Strategy

Funnels are usually:

Session-based: - % sessions with cart - % sessions with conversion

Event-based: - Step counts - Drop-off counts

Always define: Is the funnel session-based or event-based?

------------------------------------------------------------------------

# 🧪 10. Testing Philosophy in dbt

Use:

-   not_null
-   unique
-   relationships
-   equal_rowcount
-   accepted_range
-   expression_is_true

Do NOT duplicate the same test across every layer unnecessarily.

Principle: Test where logic is created.

------------------------------------------------------------------------

# 🧱 11. Surrogate Keys

Generate using deterministic logic:

-   dbt_utils.generate_surrogate_key()
-   Use stable natural keys

Never base surrogate keys on non-deterministic values.

------------------------------------------------------------------------

# ⚡ 12. Performance Patterns

-   Filter early in CTE chains
-   Avoid select \*
-   Avoid unnecessary FULL JOIN
-   Use QUALIFY instead of subqueries (BigQuery/Snowflake)
-   Prefer UNION ALL over UNION

------------------------------------------------------------------------

# 🛠 13. Common Modeling Patterns

## Slowly Changing Dimension (Type 1)

Overwrite attributes. No history tracking.

## Slowly Changing Dimension (Type 2)

Track history with: - valid_from - valid_to - is_current

------------------------------------------------------------------------

# 🔍 14. Debugging Data Drift

Check:

1.  Row count parity
2.  Missing keys (EXCEPT DISTINCT)
3.  Null drift
4.  Join fan-out
5.  Grain mismatch

------------------------------------------------------------------------

# 🧠 15. Mental Checklist

Before merging a PR:

-   Is grain clearly documented?
-   Could this join duplicate rows?
-   Is this metric calculated at correct level?
-   Is business logic centralized?
-   Are tests aligned with where logic lives?

------------------------------------------------------------------------

# 🏁 Final Philosophy

Clean modeling \> clever SQL.

Stable grain \> clever joins.

Clear separation of layers \> convenience.

Your future self will thank you.
