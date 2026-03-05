
# Behaviour Mart Layer

## Overview

The Behaviour mart layer contains event-, session-, and behavioural-analysis fact tables used for website analytics, product interaction analysis, and conversion measurement.

This layer consumes cleaned and enriched event data from the intermediate layer and aggregates where appropriate.

In addition to core event and session models, the mart includes derived behavioural models that support funnel analysis, session journey analysis, and conversion timing insights.

------------------------------------------------------------------------

# fct_events

## Grain

1 row per event.

## Purpose

Provides atomic behavioural telemetry for detailed user interaction analysis.

## Key Characteristics

- Fact primary key: `event_id`
- References:
  - `customer_sk`
  - `product_sk`
- Preserves natural keys (`customer_id`, `product_id`) for traceability
- Maintains session linkage via `journey_session_id`

## Core Fields

### Time

- `event_ts`
- `event_date`
- `seconds_since_session_start`
- `seconds_since_previous_event`

### Behaviour

- `event_type`
- `event_category`
- `page_category`
- `is_conversion_event`
- `is_session_entry_event`

### Context

- `browser`
- `traffic_source`
- `session_sequence_number`

## Analytical Use Cases

- Funnel step analysis
- Product browsing behaviour
- UX interaction timing
- Feature usage monitoring
- Event-level attribution
- Delay detection between steps
- Behavioural micro-pattern analysis

------------------------------------------------------------------------

# fct_sessions

## Grain

1 row per `journey_session_id`.

## Purpose

Aggregates event-level behaviour into session-level metrics for visit and engagement analysis.

## Key Characteristics

- Fact primary key: `journey_session_id`
- References:
  - `customer_sk`
- Built directly from `int_events_cleaned`
- Preserves source session definition

## Core Metrics

### Timing

- `session_start_ts`
- `session_end_ts`
- `session_duration_seconds`
- `session_duration_minutes`
- `session_duration_hours`
- `time_to_purchase_seconds`

### Volume & Engagement

- `event_count`
- `distinct_products_viewed`
- `product_view_count`
- `cart_event_count`
- `purchase_event_count`
- `is_bounce`
- `is_no_product_engagement`
- `session_type`
- `session_outcome`

### Behaviour & Attribution

- `has_conversion`
- `traffic_source`
- `browser`
- `funnel_depth`

## Analytical Use Cases

- Session-based conversion rate
- Bounce rate measurement
- Engagement depth analysis
- Time-to-purchase analysis
- Acquisition channel performance
- Research vs single-product behaviour
- Website visit measurement
- Cart abandonment analysis
- Funnel progression analysis

------------------------------------------------------------------------

# fct_session_funnel_steps

## Grain

1 row per `journey_session_id` × `funnel_step`.

## Purpose

Provides a normalized representation of the ecommerce funnel, enabling flexible funnel analysis and BI-friendly aggregation.

Each session is expanded into one row per funnel stage, with a flag indicating whether the session reached that stage.

## Key Characteristics

- Derived from `fct_sessions`
- Supports BI funnel visualizations
- Enables flexible segmentation by session attributes

## Core Fields

- `journey_session_id`
- `funnel_step`
- `funnel_order`
- `reached_step`
- `reached_session_id`

## Analytical Use Cases

- Funnel conversion analysis
- Step-level drop-off measurement
- Conversion rate by acquisition channel
- Conversion analysis by device or browser
- Multi-step behavioural segmentation

------------------------------------------------------------------------

# fct_funnel_time_to_step

## Grain

1 row per `journey_session_id`.

## Purpose

Measures the time elapsed between key funnel milestones within a session, enabling analysis of conversion speed and user decision latency.

## Core Fields

### Step Timestamps

- `home_ts`
- `department_ts`
- `product_ts`
- `cart_ts`
- `purchase_ts`

### Timing Metrics

- `seconds_home_to_department`
- `seconds_department_to_product`
- `seconds_product_to_cart`
- `seconds_cart_to_purchase`

## Analytical Use Cases

- Conversion latency analysis
- UX friction detection
- Checkout efficiency analysis
- Funnel velocity analysis
- Device or browser performance comparisons

------------------------------------------------------------------------

# fct_session_path_analysis

## Grain

1 row per `journey_session_id`.

## Purpose

Captures the ordered sequence of user interactions within a session to enable navigation path analysis and behavioural journey analysis.

## Core Fields

- `journey_session_id`
- `session_path`
- `event_count`

## Analytical Use Cases

- Common user journey discovery
- Navigation pattern analysis
- Path-to-purchase analysis
- Behavioural loop detection
- Drop-off path identification

------------------------------------------------------------------------

# Model Relationships

```
int_events_cleaned
        │
        ▼
     fct_events
        │
        ▼
     fct_sessions
        │
        ├── fct_session_funnel_steps
        ├── fct_funnel_time_to_step
        └── fct_session_path_analysis
```

This structure maintains a clear hierarchy of behavioural aggregation:

- Events → Sessions → Analytical behavioural models

------------------------------------------------------------------------

# Design Principles

- Clear separation of event and session grain
- Deterministic sessionization logic
- Behavioural metrics centralized in mart layer
- BI-friendly funnel modelling
- Explicit grain definition for every model
- Reusable behavioural flags in session layer
- Star-schema alignment with dimension tables
