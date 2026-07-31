{{ config(materialized='incremental', unique_key='user_id') }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(last_seen_at) FROM {{ this }})
    {% endif %}
),

user_stats AS (
    SELECT
        user_id,
        MIN(event_timestamp) AS first_seen_at,
        MAX(event_timestamp) AS last_seen_at,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(*) AS total_events,
        SUM(CASE WHEN is_revenue_event THEN amount ELSE 0 END) AS lifetime_value,
        MAX(country) AS most_common_country,
        MAX(device_type) AS most_common_device,
        COUNT(DISTINCT DATE(event_timestamp)) AS active_days
    FROM events
    WHERE user_id != 'unknown'
    GROUP BY user_id
)

SELECT
    user_id,
    first_seen_at,
    last_seen_at,
    total_sessions,
    total_events,
    lifetime_value,
    most_common_country,
    most_common_device,
    active_days,
    CASE 
        WHEN lifetime_value > 100 THEN 'high_value'
        WHEN lifetime_value > 0 THEN 'purchaser'
        WHEN total_sessions > 5 THEN 'engaged'
        ELSE 'new'
    END AS user_segment,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM user_stats
