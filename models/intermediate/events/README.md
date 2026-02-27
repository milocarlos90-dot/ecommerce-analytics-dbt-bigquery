# Events Intermediate Layer

## Purpose
This layer standardizes event data and introduces behavioral sessionization.

## Models

### int_events_cleaned
- Removes bot/test traffic
- Standardizes timestamps
- Normalizes event semantics
- Parses URI metadata
- Grain: 1 row per event

### int_events_sessionized
- Assigns analytics sessions using 30-minute inactivity rule
- Preserves journey sessions from source
- Creates analytics_session_id
- Grain: 1 row per event

## Session Strategy
Two session concepts exist:

- journey_session_id → application-defined browsing journey
- analytics_session_id → behavior-based analytics visit

### fct_events
Supports event-level analytics including:
- user behavior analysis
- page interaction tracking
- feature usage monitoring
- funnel step analysis
- product browsing behavior
- marketing traffic performance
- event-level attribution analysis

### fct_sessions
Supports session-level analytics including:
- website visit measurement
- engagement analysis (events per session)
- session duration analysis
- conversion rate tracking
- funnel conversion analysis
- customer journey analysis
- acquisition channel performance

---