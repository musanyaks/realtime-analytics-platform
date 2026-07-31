{{ config(materialized='view') }}

WITH source AS (
    SELECT
        event_id,
        event_timestamp,
        user_id,
        event_type,
        session_id,
        page_url,
        device_type,
        country,
        amount,
        _ingestion_timestamp
    FROM {{ source('raw_events', 'kafka_events') }}
    WHERE event_timestamp >= DATEADD(hour, -48, CURRENT_TIMESTAMP())
),

cleaned AS (
    SELECT
        event_id,
        TRY_TO_TIMESTAMP_NTZ(event_timestamp) AS event_timestamp,
        COALESCE(user_id, 'unknown') AS user_id,
        LOWER(TRIM(event_type)) AS event_type,
        session_id,
        page_url,
        COALESCE(device_type, 'unknown') AS device_type,
        COALESCE(UPPER(country), 'UNKNOWN') AS country,
        TRY_TO_DECIMAL(amount, 18, 2) AS amount,
        _ingestion_timestamp,
        -- Derived columns
        DATE(event_timestamp) AS event_date,
        HOUR(event_timestamp) AS event_hour,
        CASE 
            WHEN amount IS NOT NULL AND amount > 0 THEN TRUE 
            ELSE FALSE 
        END AS is_revenue_event
    FROM source
    WHERE event_id IS NOT NULL
      AND event_timestamp IS NOT NULL
)

SELECT * FROM cleaned
