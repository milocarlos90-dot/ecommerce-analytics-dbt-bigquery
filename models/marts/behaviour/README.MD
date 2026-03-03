# Behaviour Mart Layer

## Overview

The Behaviour mart layer contains session- and event-level fact tables
used for website analytics, product interaction analysis, and conversion
measurement.

This layer consumes cleaned and enriched event data from the
intermediate layer and aggregates where appropriate.

------------------------------------------------------------------------

# fct_events

## Grain

1 row per event.

## Purpose

Provides atomic behavioural telemetry for detailed user interaction
analysis.

## Key Characteristics

-   Fact primary key: `event_id`
-   References:
    -   `customer_sk`
    -   `product_sk`
-   Preserves natural keys (`customer_id`, `product_id`) for
    traceability
-   Maintains session linkage via `journey_session_id`

## Core Fields

### Time

-   `event_ts`
-   `event_date`
-   `seconds_since_session_start`
-   `seconds_since_previous_event`

### Behaviour

-   `event_type`
-   `event_category`
-   `page_category`
-   `is_conversion_event`
-   `is_session_entry_event`

### Context

-   `browser`
-   `traffic_source`
-   `session_sequence_number`

## Analytical Use Cases

-   Funnel step analysis
-   Product browsing behaviour
-   UX interaction timing
-   Feature usage monitoring
-   Event-level attribution
-   Delay detection between steps
-   Behavioural micro-pattern analysis

------------------------------------------------------------------------

# fct_sessions

## Grain

1 row per `journey_session_id`.

## Purpose

Aggregates event-level behaviour into session-level metrics for visit
and engagement analysis.

## Key Characteristics

-   Fact primary key: `journey_session_id`
-   References:
    -   `customer_sk`
-   Built directly from `int_events_cleaned`
-   Preserves source session definition

## Core Metrics

### Timing

-   `session_start_ts`
-   `session_end_ts`
-   `session_duration_seconds`
-   `session_duration_minutes`
-   `session_duration_hours`
-   `time_to_purchase_seconds`

### Volume & Engagement

-   `event_count`
-   `distinct_products_viewed`
-   `is_bounce`
-   `session_type`

### Behaviour & Attribution

-   `has_conversion`
-   `traffic_source`
-   `browser`

## Analytical Use Cases

-   Session-based conversion rate
-   Bounce rate measurement
-   Engagement depth analysis
-   Time-to-purchase analysis
-   Acquisition channel performance
-   Research vs single-product behaviour
-   Website visit measurement

------------------------------------------------------------------------

# Design Principles

-   Clear separation of event and session grain
-   No aggregation in intermediate layer
-   Deterministic session timing logic
-   Star-schema alignment with dimension tables
-   Behavioural metrics centralized in mart layer
