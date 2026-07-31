{{ config(materialized='view') }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

session_metrics AS (
    SELECT
        session_id,
        user_id,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        DATEDIFF(minute, MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_minutes,
        COUNT(*) AS total_events,
        COUNT(DISTINCT page_url) AS unique_pages,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        SUM(CASE WHEN is_revenue_event THEN amount ELSE 0 END) AS session_revenue,
        MAX(device_type) AS device_type,
        MAX(country) AS country,
        DATE(MIN(event_timestamp)) AS session_date
    FROM events
    WHERE session_id IS NOT NULL
    GROUP BY session_id, user_id
)

SELECT * FROM session_metrics
