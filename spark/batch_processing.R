#!/usr/bin/env Rscript
# =============================================================================
# Spark Batch Processing (sparklyr)
# =============================================================================
# Performs hourly batch aggregations on event data and writes to Snowflake.
#
# Usage:
#   spark-submit batch_processing.R --run-date=2024-01-01
# =============================================================================

library(sparklyr)
library(dplyr)
library(DBI)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(name, default = NULL) {
  arg <- grep(paste0("^--", name, "="), args, value = TRUE)
  if (length(arg) == 0) return(default)
  sub(paste0("^--", name, "="), "", arg)
}

RUN_DATE    <- parse_arg("run-date", format(Sys.Date(), "%Y-%m-%d"))
MASTER      <- parse_arg("master", Sys.getenv("SPARK_MASTER", "local[*]"))
APP_NAME    <- parse_arg("app-name", "BatchAnalytics")

SNOWFLAKE_ACCOUNT   <- Sys.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      <- Sys.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  <- Sys.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  <- Sys.getenv("SNOWFLAKE_DATABASE", "ANALYTICS")
SNOWFLAKE_SCHEMA    <- Sys.getenv("SNOWFLAKE_SCHEMA", "PROD")
SNOWFLAKE_WAREHOUSE <- Sys.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
message("Initializing Spark session...")

config <- spark_config()
config$`sparklyr.shell.packages` <- paste(
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
  "net.snowflake:snowflake-jdbc:3.14.3",
  "net.snowflake:spark-snowflake_2.12:2.12.0",
  sep = ","
)
config$`spark.sql.adaptive.enabled` <- "true"
config$`spark.sql.adaptive.coalescePartitions.enabled` <- "true"
config$`spark.serializer` <- "org.apache.spark.serializer.KryoSerializer"

sc <- spark_connect(master = MASTER, app_name = APP_NAME, config = config)

# ---------------------------------------------------------------------------
# Snowflake Options
# ---------------------------------------------------------------------------
sf_options <- list(
  sfURL        = paste0(SNOWFLAKE_ACCOUNT, ".snowflakecomputing.com"),
  sfUser       = SNOWFLAKE_USER,
  sfPassword   = SNOWFLAKE_PASSWORD,
  sfDatabase   = SNOWFLAKE_DATABASE,
  sfSchema     = SNOWFLAKE_SCHEMA,
  sfWarehouse  = SNOWFLAKE_WAREHOUSE,
  sfRole       = "ACCOUNTADMIN"
)

# ---------------------------------------------------------------------------
# Read Raw Events
# ---------------------------------------------------------------------------
message("Reading raw events from Snowflake...")

raw_events <- spark_read_jdbc(
  sc,
  name = "raw_events",
  options = list(
    url      = paste0("jdbc:snowflake://", sf_options$sfURL, "/?db=", sf_options$sfDatabase, "&schema=", sf_options$sfSchema),
    dbtable  = paste0(SNOWFLAKE_DATABASE, ".", SNOWFLAKE_SCHEMA, ".stg_events"),
    user     = sf_options$sfUser,
    password = sf_options$sfPassword
  )
)

# ---------------------------------------------------------------------------
# Batch Aggregations
# ---------------------------------------------------------------------------
message("Computing batch aggregations...")

hourly_metrics <- raw_events %>%
  filter(event_date == RUN_DATE) %>%
  group_by(event_date, event_hour) %>%
  summarise(
    total_events      = n(),
    unique_users      = n_distinct(user_id),
    unique_sessions   = n_distinct(session_id),
    purchases         = sum(ifelse(event_type == "purchase", 1, 0)),
    revenue           = sum(ifelse(is_revenue_event, amount, 0), na.rm = TRUE),
    avg_order_value   = mean(ifelse(is_revenue_event, amount, NA), na.rm = TRUE),
    mobile_sessions   = n_distinct(ifelse(device_type == "mobile", session_id, NA)),
    countries_active  = n_distinct(country),
    .groups = "drop"
  ) %>%
  mutate(
    revenue_per_user = revenue / unique_users,
    conversion_rate  = purchases / unique_sessions,
    mobile_share     = mobile_sessions / unique_sessions,
    _processed_at    = current_timestamp()
  )

# Top products/pages
page_metrics <- raw_events %>%
  filter(event_date == RUN_DATE) %>%
  group_by(page_url) %>%
  summarise(
    page_views       = n(),
    unique_users     = n_distinct(user_id),
    avg_time_on_page = 0,  # Would need session timing data
    bounce_rate      = 0,  # Would need session-level analysis
    .groups = "drop"
  ) %>%
  arrange(desc(page_views)) %>%
  head(100)

# User cohort analysis
cohort_metrics <- raw_events %>%
  filter(event_date == RUN_DATE) %>%
  group_by(country, device_type) %>%
  summarise(
    users           = n_distinct(user_id),
    sessions        = n_distinct(session_id),
    events          = n(),
    revenue         = sum(ifelse(is_revenue_event, amount, 0), na.rm = TRUE),
    conversion_rate = sum(ifelse(event_type == "purchase", 1, 0)) / n_distinct(session_id),
    .groups = "drop"
  )

# ---------------------------------------------------------------------------
# Write to Snowflake
# ---------------------------------------------------------------------------
message("Writing results to Snowflake...")

spark_write_jdbc(
  hourly_metrics,
  name = "hourly_metrics",
  mode = "overwrite",
  options = list(
    url      = paste0("jdbc:snowflake://", sf_options$sfURL, "/?db=", sf_options$sfDatabase, "&schema=", sf_options$sfSchema),
    dbtable  = paste0(SNOWFLAKE_DATABASE, ".", SNOWFLAKE_SCHEMA, ".batch_aggregates_hourly"),
    user     = sf_options$sfUser,
    password = sf_options$sfPassword
  )
)

spark_write_jdbc(
  page_metrics,
  name = "page_metrics",
  mode = "overwrite",
  options = list(
    url      = paste0("jdbc:snowflake://", sf_options$sfURL, "/?db=", sf_options$sfDatabase, "&schema=", sf_options$sfSchema),
    dbtable  = paste0(SNOWFLAKE_DATABASE, ".", SNOWFLAKE_SCHEMA, ".batch_page_metrics"),
    user     = sf_options$sfUser,
    password = sf_options$sfPassword
  )
)

spark_write_jdbc(
  cohort_metrics,
  name = "cohort_metrics",
  mode = "overwrite",
  options = list(
    url      = paste0("jdbc:snowflake://", sf_options$sfURL, "/?db=", sf_options$sfDatabase, "&schema=", sf_options$sfSchema),
    dbtable  = paste0(SNOWFLAKE_DATABASE, ".", SNOWFLAKE_SCHEMA, ".batch_cohort_metrics"),
    user     = sf_options$sfUser,
    password = sf_options$sfPassword
  )
)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
message(sprintf("Batch processing complete for %s", RUN_DATE))
spark_disconnect(sc)
message("Spark session disconnected.")
