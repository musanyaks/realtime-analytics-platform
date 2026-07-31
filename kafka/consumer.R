#!/usr/bin/env Rscript
# =============================================================================
# Kafka Event Consumer (R)
# =============================================================================
# Consumes events from Kafka and writes to Snowflake or local storage.
#
# Usage:
#   Rscript consumer.R --topic=user_events --group=analytics-consumer
# =============================================================================

library(jsonlite)
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

TOPIC       <- parse_arg("topic", "user_events")
GROUP       <- parse_arg("group", "r-analytics-consumer")
BOOTSTRAP   <- parse_arg("bootstrap-servers", Sys.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
BATCH_SIZE  <- as.integer(parse_arg("batch-size", "500"))
OUTPUT_MODE <- parse_arg("output", "console")  # console, snowflake, file

# ---------------------------------------------------------------------------
# Snowflake Connection (if output=snowflake)
# ---------------------------------------------------------------------------
connect_snowflake <- function() {
  library(odbc)
  dbConnect(odbc::odbc(),
    Driver       = "Snowflake",
    Server       = paste0(Sys.getenv("SNOWFLAKE_ACCOUNT"), ".snowflakecomputing.com"),
    UID          = Sys.getenv("SNOWFLAKE_USER"),
    PWD          = Sys.getenv("SNOWFLAKE_PASSWORD"),
    Database     = Sys.getenv("SNOWFLAKE_DATABASE"),
    Schema       = Sys.getenv("SNOWFLAKE_SCHEMA"),
    Warehouse    = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
    Role         = "ACCOUNTADMIN"
  )
}

# ---------------------------------------------------------------------------
# Event Processing
# ---------------------------------------------------------------------------
process_batch <- function(events) {
  if (length(events) == 0) return(0)

  df <- do.call(rbind, lapply(events, function(e) {
    as.data.frame(e, stringsAsFactors = FALSE)
  }))

  if (OUTPUT_MODE == "console") {
    message(sprintf("[%s] Received %d events", format(Sys.time()), nrow(df)))
    print(head(df, 3))

  } else if (OUTPUT_MODE == "snowflake") {
    con <- connect_snowflake()
    on.exit(dbDisconnect(con))

    dbWriteTable(con, "raw.kafka_events_buffer", df, append = TRUE)
    message(sprintf("Inserted %d events into Snowflake", nrow(df)))

  } else if (OUTPUT_MODE == "file") {
    output_dir <- "/tmp/kafka_consumed"
    if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

    file_path <- file.path(output_dir, sprintf("consumed_%s.json", format(Sys.time(), "%Y%m%d_%H%M%S")))
    writeLines(sapply(events, toJSON, auto_unbox = TRUE), file_path)
    message(sprintf("Wrote %d events to %s", nrow(df), file_path))
  }

  nrow(df)
}

# ---------------------------------------------------------------------------
# Main Consumer Loop (simulated via polling files for simplicity in R)
# ---------------------------------------------------------------------------
main <- function() {
  message(sprintf("Kafka Consumer | Topic: %s | Group: %s | Output: %s", TOPIC, GROUP, OUTPUT_MODE))

  # For production, use kafka-console-consumer piped to R
  # Here we simulate by reading from the producer output directory
  input_dir <- "/tmp/kafka_simulation"

  if (!dir.exists(input_dir)) {
    message("No input directory found. Waiting for events...")
    Sys.sleep(5)
  }

  processed_files <- character(0)
  total_events <- 0

  repeat {
    files <- list.files(input_dir, pattern = "events_.*\\.json", full.names = TRUE)
    new_files <- setdiff(files, processed_files)

    for (f in new_files) {
      lines <- readLines(f, warn = FALSE)
      events <- lapply(lines, fromJSON)
      n <- process_batch(events)
      total_events <- total_events + n
      processed_files <- c(processed_files, f)
    }

    Sys.sleep(2)
  }
}

if (!interactive()) {
  main()
}
