{{ config(materialized='incremental', unique_key='event_id') }}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    {% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT
    event_id,
    event_timestamp,
    event_date,
    event_hour,
    user_id,
    event_type,
    session_id,
    page_url,
    device_type,
    country,
    amount,
    is_revenue_event,
    _ingestion_timestamp
FROM events
