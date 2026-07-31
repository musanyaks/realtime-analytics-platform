{{ config(materialized='table') }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE event_timestamp >= DATEADD(day, -{{ var('lookback_days', 30) }}, CURRENT_DATE())
),

hourly AS (
    SELECT
        event_date,
        event_hour,
        COUNT(*) AS total_events,
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT session_id) AS unique_sessions,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
        SUM(CASE WHEN is_revenue_event THEN amount ELSE 0 END) AS revenue,
        AVG(CASE WHEN is_revenue_event THEN amount END) AS avg_order_value,
        COUNT(DISTINCT CASE WHEN device_type = 'mobile' THEN session_id END)::FLOAT / 
            NULLIF(COUNT(DISTINCT session_id), 0) AS mobile_share,
        COUNT(DISTINCT country) AS countries_active
    FROM events
    GROUP BY event_date, event_hour
)

SELECT
    *,
    revenue / NULLIF(unique_users, 0) AS revenue_per_user,
    purchases::FLOAT / NULLIF(unique_sessions, 0) AS conversion_rate
FROM hourly
ORDER BY event_date DESC, event_hour DESC
