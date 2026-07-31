-- =============================================================================
-- Snowflake Setup for Real-Time Analytics Platform
-- =============================================================================
-- Run this script in your Snowflake console to initialize the database,
-- schemas, tables, and warehouse.
-- =============================================================================

-- Create database
CREATE DATABASE IF NOT EXISTS ANALYTICS;
USE DATABASE ANALYTICS;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS PROD;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- Create warehouse (if not exists)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE;

USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- RAW Layer: Landing zone for ingested data
-- =============================================================================

CREATE TABLE IF NOT EXISTS RAW.KAFKA_EVENTS (
    event_id              STRING,
    event_timestamp       TIMESTAMP_NTZ,
    user_id               STRING,
    event_type            STRING,
    session_id            STRING,
    page_url              STRING,
    device_type           STRING,
    country               STRING,
    amount                DECIMAL(18,2),
    _ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.BATCH_AGGREGATES (
    hour_window           TIMESTAMP_NTZ,
    total_events          INTEGER,
    unique_users          INTEGER,
    total_revenue         DECIMAL(18,2),
    _processed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STAGING Layer: Cleaned and typed data (managed by dbt)
-- =============================================================================
-- dbt will create these views, but we define the expected structure here
-- for documentation purposes.

-- =============================================================================
-- PROD/MARTS Layer: Business-ready tables (managed by dbt)
-- =============================================================================
-- dbt will create:
--   - PROD.FCT_USER_EVENTS (incremental)
--   - PROD.FCT_HOURLY_METRICS (table)
--   - PROD.DIM_USERS (incremental)

-- =============================================================================
-- Batch Aggregate Tables (for Spark batch output)
-- =============================================================================

CREATE TABLE IF NOT EXISTS PROD.BATCH_AGGREGATES_HOURLY (
    event_date            DATE,
    event_hour            INTEGER,
    total_events          BIGINT,
    unique_users          BIGINT,
    unique_sessions       BIGINT,
    purchases             BIGINT,
    revenue               DECIMAL(18,2),
    avg_order_value       DECIMAL(18,2),
    mobile_sessions       BIGINT,
    countries_active      BIGINT,
    revenue_per_user      DECIMAL(18,2),
    conversion_rate       DECIMAL(10,4),
    mobile_share          DECIMAL(10,4),
    _processed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PROD.BATCH_PAGE_METRICS (
    page_url              STRING,
    page_views            BIGINT,
    unique_users          BIGINT,
    avg_time_on_page      DECIMAL(10,2),
    bounce_rate           DECIMAL(10,4),
    _processed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PROD.BATCH_COHORT_METRICS (
    country               STRING,
    device_type           STRING,
    users                 BIGINT,
    sessions              BIGINT,
    events                BIGINT,
    revenue               DECIMAL(18,2),
    conversion_rate       DECIMAL(10,4),
    _processed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- File Format for bulk loads
-- =============================================================================

CREATE FILE FORMAT IF NOT EXISTS RAW.JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

CREATE FILE FORMAT IF NOT EXISTS RAW.PARQUET_FORMAT
  TYPE = 'PARQUET';

-- =============================================================================
-- Stage for external data loading
-- =============================================================================

CREATE STAGE IF NOT EXISTS RAW.DATA_STAGE
  FILE_FORMAT = RAW.JSON_FORMAT;

-- =============================================================================
-- Monitoring Views
-- =============================================================================

CREATE OR REPLACE VIEW RAW.PIPELINE_MONITORING AS
SELECT
    'KAFKA_EVENTS' AS table_name,
    COUNT(*) AS record_count,
    MAX(_ingestion_timestamp) AS last_update,
    DATEDIFF(minute, MAX(_ingestion_timestamp), CURRENT_TIMESTAMP()) AS lag_minutes
FROM RAW.KAFKA_EVENTS
UNION ALL
SELECT
    'BATCH_AGGREGATES_HOURLY' AS table_name,
    COUNT(*) AS record_count,
    MAX(_processed_at) AS last_update,
    DATEDIFF(minute, MAX(_processed_at), CURRENT_TIMESTAMP()) AS lag_minutes
FROM PROD.BATCH_AGGREGATES_HOURLY;

-- =============================================================================
-- Grants (adjust roles as needed)
-- =============================================================================

GRANT USAGE ON DATABASE ANALYTICS TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA RAW TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA STAGING TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA PROD TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA MARTS TO ROLE ACCOUNTADMIN;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA RAW TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA PROD TO ROLE ACCOUNTADMIN;

GRANT SELECT ON VIEW RAW.PIPELINE_MONITORING TO ROLE ACCOUNTADMIN;

-- =============================================================================
-- Sample data for testing (optional)
-- =============================================================================

INSERT INTO RAW.KAFKA_EVENTS (event_id, event_timestamp, user_id, event_type, session_id, page_url, device_type, country, amount)
SELECT
    'evt_' || UNIFORM(1, 1000000, RANDOM()) AS event_id,
    DATEADD(minute, -UNIFORM(1, 1440, RANDOM()), CURRENT_TIMESTAMP()) AS event_timestamp,
    'usr_' || UNIFORM(1, 10000, RANDOM()) AS user_id,
    ['view', 'click', 'add_to_cart', 'checkout', 'purchase'][UNIFORM(1, 5, RANDOM())] AS event_type,
    'sess_' || UNIFORM(1, 500000, RANDOM()) AS session_id,
    ['/home', '/products', '/cart', '/checkout'][UNIFORM(1, 4, RANDOM())] AS page_url,
    ['desktop', 'mobile', 'tablet'][UNIFORM(1, 3, RANDOM())] AS device_type,
    ['US', 'GB', 'DE', 'FR', 'CA'][UNIFORM(1, 5, RANDOM())] AS country,
    CASE WHEN event_type = 'purchase' THEN UNIFORM(1000, 50000, RANDOM()) / 100.0 ELSE NULL END AS amount
FROM TABLE(GENERATOR(ROWCOUNT => 1000));
