---
name: window-aggregation
description: Generate Flink SQL windowed aggregation queries from natural language descriptions
---

# Window Aggregation Skill

Translates natural language aggregation requirements into Flink SQL windowed queries for real-time stream processing.

## Supported Window Types

### 1. Tumbling Window (Fixed, Non-Overlapping)
> "Aggregate orders by region every 5 minutes"

```sql
SELECT
    region,
    TUMBLE_START(created_at, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(created_at, INTERVAL '5' MINUTE) AS window_end,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value
FROM orders_stream
GROUP BY
    region,
    TUMBLE(created_at, INTERVAL '5' MINUTE);
```

### 2. Sliding Window (Overlapping)
> "Show the running average order value over 10 minutes, updated every 2 minutes"

```sql
SELECT
    region,
    HOP_START(created_at, INTERVAL '2' MINUTE, INTERVAL '10' MINUTE) AS window_start,
    HOP_END(created_at, INTERVAL '2' MINUTE, INTERVAL '10' MINUTE) AS window_end,
    AVG(total_amount) AS avg_order_value,
    COUNT(*) AS order_count
FROM orders_stream
GROUP BY
    region,
    HOP(created_at, INTERVAL '2' MINUTE, INTERVAL '10' MINUTE);
```

### 3. Session Window (Gap-Based)
> "Group user clicks into sessions with 30 minute timeout"

```sql
SELECT
    user_id,
    SESSION_START(event_time, INTERVAL '30' MINUTE) AS session_start,
    SESSION_END(event_time, INTERVAL '30' MINUTE) AS session_end,
    COUNT(*) AS click_count,
    COUNT(DISTINCT page) AS pages_viewed
FROM clickstream
GROUP BY
    user_id,
    SESSION(event_time, INTERVAL '30' MINUTE);
```

## NL Pattern → Window Type Mapping

| Natural Language Pattern | Window Type | Key Params |
|------------------------|-------------|-----------|
| "every X minutes/hours" | TUMBLE | size = X |
| "running/moving average over X" | HOP | size = X, slide = X/5 |
| "updated every X" | HOP | slide = X |
| "session" / "user activity" | SESSION | gap = 30min default |
| "last X minutes" | HOP | size = X, slide = 1min |
| "daily/hourly summary" | TUMBLE | size = 1 DAY/HOUR |

## Common Aggregation Functions
- `COUNT(*)` — event count
- `SUM(amount)` — total value
- `AVG(amount)` — average value
- `MIN(amount)`, `MAX(amount)` — range
- `COUNT(DISTINCT field)` — unique count
- `LISTAGG(field, ',')` — concatenate values

## Sink to Iceberg
Always pair aggregation with an Iceberg sink:

```sql
INSERT INTO iceberg_catalog.ecommerce.orders_by_region
SELECT
    region,
    TUMBLE_START(created_at, INTERVAL '5' MINUTE) AS window_start,
    TUMBLE_END(created_at, INTERVAL '5' MINUTE) AS window_end,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
FROM orders_cdc
GROUP BY
    region,
    TUMBLE(created_at, INTERVAL '5' MINUTE);
```
