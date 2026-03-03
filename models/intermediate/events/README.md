# Events Intermediate Layer

## Purpose

This layer standardizes, cleans, and enriches raw event data before it
is consumed by mart-level fact tables.

It is responsible for:

-   Data quality enforcement\
-   Timestamp normalization\
-   Traffic filtering\
-   URI metadata parsing\
-   Event semantic normalization\
-   Session-level structural helpers

This layer **does not aggregate data**.\
Grain remains **1 row per event**.

------------------------------------------------------------------------

## Models

### `int_events_cleaned`

**Grain:** 1 row per event

### Responsibilities

-   Removes bot and test traffic
-   Standardizes timestamps (`event_ts`, `event_date`)
-   Normalizes application session identifier (`journey_session_id`)
-   Parses URI metadata:
    -   `page_category`
    -   `product_id`
-   Normalizes behavioral event semantics:
    -   browse
    -   cart_interaction
    -   conversion
    -   order_management
    -   other
-   Adds structural behavioral flags:
    -   `is_conversion_event`
    -   `is_session_entry_event`
-   Adds session timing helpers:
    -   `seconds_since_session_start`
    -   `seconds_since_previous_event`

------------------------------------------------------------------------

## Session Strategy

This project uses:

-   `journey_session_id` → application-defined browsing journey

The intermediate layer **does not redefine session boundaries**.\
Session aggregation occurs in the mart layer.

------------------------------------------------------------------------

## Product Handling Strategy

-   `product_id` is parsed from product page URIs.
-   Dimensional attributes (brand, name, category, etc.) are sourced
    from `dim_products`.
-   No product attributes are derived from URL strings beyond identifier
    extraction.

------------------------------------------------------------------------

## Design Principles

-   Preserve event grain\
-   Avoid aggregation in intermediate layer\
-   Separate structural logic from analytical logic\
-   Keep session and conversion aggregation in mart layer\
-   Maintain clean star-schema compatibility downstream

------------------------------------------------------------------------

## Not Included Here

The following are defined in the mart layer:

-   `fct_events`
-   `fct_sessions`
-   Session-level aggregation logic
-   Conversion timing metrics
-   Session classification (bounce, research, etc.)

A separate README documents the mart layer.
