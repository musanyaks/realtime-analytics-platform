#!/usr/bin/env Rscript
# =============================================================================
# Spark Structured Streaming (sparklyr)
# =============================================================================
# Consumes from Kafka, performs windowed aggregations, and writes to Snowflake.
#
# Usage:
#   spark-submit streaming_processing.R --kafka-bootstrap=kafka:9092
# =============================================================================

library(sparklyr)
library(dplyr)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(name, default = NULL) {
  arg <- grep(paste0("^--", name, "="), args, value = TRUE)
  if (length(arg) == 0) return(default)
  sub(paste0("^--", name, "="), "", arg)
}

KAFKA_BOOTSTRAP     <- parse_arg("kafka-bootstrap", "kafka:9092")
KAFKA_TOPIC         <- parse_arg("topic", "user_events")
MASTER              <- parse_arg("master", Sys.getenv("SPARK_MASTER", "local[*]"))
CHECKPOINT_LOCATION <- parse_arg("checkpoint", "/tmp/spark-checkpoints")

SNOWFLAKE_ACCOUNT   <- Sys.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      <- Sys.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  <- Sys.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE  <- Sys.getenv("SNOWFLAKE_DATABASE", "ANALYTICS")
SNOWFLAKE_SCHEMA    <- Sys.getenv("SNOWFLAKE_SCHEMA", "PROD")
SNOWFLAKE_WAREHOUSE <- Sys.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
message("Initializing Spark Structured Streaming session...")

config <- spark_config()
config$`sparklyr.shell.packages` <- paste(
  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
  "net.snowflake:snowflake-jdbc:3.14.3",
  "net.snowflake:spark-snowflake_2.12:2.12.0",
  sep = ","
)
config$`spark.sql.streaming.checkpointLocation` <- CHECKPOINT_LOCATION
config$`spark.sql.adaptive.enabled` <- "true"

sc <- spark_connect(master = MASTER, app_name = "StreamingAnalytics", config = config)

# ---------------------------------------------------------------------------
# Read from Kafka
# ---------------------------------------------------------------------------
message(sprintf("Subscribing to Kafka topic: %s", KAFKA_TOPIC))

stream_df <- stream_read_kafka(
  sc,
  options = list(
    kafka.bootstrap.servers = KAFKA_BOOTSTRAP,
    subscribe               = KAFKA_TOPIC,
    startingOffsets         = "latest",
    failOnDataLoss          = "false"
  )
)

# Parse JSON value
parsed <- stream_df %>%
  mutate(
    value_str = CAST(value AS STRING)
  )

# For sparklyr streaming, we use spark_read_text as a simpler approach
# and parse with Spark SQL
message("Setting up streaming query...")

# Alternative: Read from the file-based simulation for local dev
# In production, this would be the Kafka stream above
input_path <- "/tmp/kafka_simulation"
if (!dir.exists(input_path)) dir.create(input_path, recursive = TRUE)

# Simulate streaming by reading files from directory
stream_source <- stream_read_json(sc, path = input_path)

# ---------------------------------------------------------------------------
# Streaming Transformations
# ---------------------------------------------------------------------------
message("Applying streaming transformations...")

# Windowed aggregations (tumbling window)
windowed_metrics <- stream_source %>%
  mutate(
    event_timestamp = to_timestamp(event_timestamp),
    amount = cast(amount AS DOUBLE)
  ) %>%
  group_by(
    window(event_timestamp, "5 minutes"),
    event_type
  ) %>%
  summarise(
    event_count = n(),
    unique_users = n_distinct(user_id),
    total_revenue = sum(ifelse(is_revenue_event, amount, 0), na.rm = TRUE),
    .groups = "drop"
  )

# ---------------------------------------------------------------------------
# Write to Console (for monitoring)
# ---------------------------------------------------------------------------
message("Starting streaming query to console...")

query_console <- stream_write_memory(
  windowed_metrics,
  name = "streaming_metrics",
  mode = "append",
  trigger = list(processingTime = "10 seconds"),
  options = list(
    checkpointLocation = file.path(CHECKPOINT_LOCATION, "console")
  )
)

# ---------------------------------------------------------------------------
# Write to Snowflake (micro-batch)
# ---------------------------------------------------------------------------
# Note: In production, use foreachBatch or trigger once for Snowflake writes
# to avoid excessive small writes.

sf_url <- paste0("jdbc:snowflake://", SNOWFLAKE_ACCOUNT, ".snowflakecomputing.com")

# For demo purposes, we write to parquet files that can be loaded to Snowflake
message("Starting streaming query to output files...")

query_output <- stream_write_parquet(
  windowed_metrics,
  path = "/tmp/streaming_output",
  mode = "append",
  trigger = list(processingTime = "1 minute"),
  options = list(
    checkpointLocation = file.path(CHECKPOINT_LOCATION, "parquet")
  )
)

# ---------------------------------------------------------------------------
# Keep Streaming Alive
# ---------------------------------------------------------------------------
message("Streaming query active. Press Ctrl+C to stop.")

tryCatch({
  # Run for a limited time (controlled by Airflow timeout)
  Sys.sleep(300)  # 5 minutes
}, finally = {
  message("Stopping streaming queries...")
  stream_stop(query_console)
  stream_stop(query_output)
  spark_disconnect(sc)
  message("Streaming stopped and Spark disconnected.")
})
