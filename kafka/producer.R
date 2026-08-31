#!/usr/bin/env Rscript
# =============================================================================
# Kafka Event Producer (R)
# =============================================================================
# Simulates e-commerce events and publishes them to Kafka topics.
# Can run in batch mode (one-shot) or continuous mode.
#
# Usage:
#   Rscript producer.R --mode=continuous --duration=300 --rate=100
#   Rscript producer.R --mode=batch --count=10000
# =============================================================================

library(jsonlite)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(name, default = NULL) {
  arg <- grep(paste0("^--", name, "="), args, value = TRUE)
  if (length(arg) == 0) return(default)
  sub(paste0("^--", name, "="), "", arg)
}

MODE        <- parse_arg("mode", "batch")
COUNT       <- as.integer(parse_arg("count", "1000"))
DURATION    <- as.integer(parse_arg("duration", "60"))
RATE        <- as.integer(parse_arg("rate", "50"))
BOOTSTRAP   <- parse_arg("bootstrap-servers", Sys.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
TOPIC       <- parse_arg("topic", "user_events")

# ---------------------------------------------------------------------------
# Event Generation
# ---------------------------------------------------------------------------
set.seed(42)

EVENT_TYPES <- c("view", "click", "add_to_cart", "checkout", "purchase")
DEVICES     <- c("desktop", "mobile", "tablet")
COUNTRIES   <- c("US", "GB", "DE", "FR", "CA", "AU", "JP", "BR", "IN", "MX")
PAGES       <- c(
  "/home", "/products", "/products/electronics", "/products/clothing",
  "/cart", "/checkout", "/checkout/payment", "/checkout/success",
  "/account", "/search"
)

generate_event <- function() {
  event_type <- sample(EVENT_TYPES, 1, prob = c(0.5, 0.25, 0.12, 0.08, 0.05))

  amount <- if (event_type == "purchase") {
    round(runif(1, 10, 500), 2)
  } else if (event_type %in% c("checkout", "add_to_cart")) {
    round(runif(1, 10, 300), 2)
  } else {
    NA
  }

  list(
    event_id        = paste0("evt_", formatC(sample(1:1e9, 1), width = 9, flag = "0")),
    event_timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    user_id         = paste0("usr_", sample(1:10000, 1)),
    event_type      = event_type,
    session_id      = paste0("sess_", sample(1:500000, 1)),
    page_url        = sample(PAGES, 1),
    device_type     = sample(DEVICES, 1, prob = c(0.45, 0.45, 0.10)),
    country         = sample(COUNTRIES, 1),
    amount          = amount,
    `_ingestion_timestamp` = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
}

# ---------------------------------------------------------------------------
# Kafka Publishing (using system kafka-console-producer)
# ---------------------------------------------------------------------------
publish_events <- function(events) {
  json_lines <- sapply(events, function(e) toJSON(e, auto_unbox = TRUE, na = "null"))

  # Write to temp file and use kafka-console-producer
  tmp_file <- tempfile(fileext = ".json")
  writeLines(json_lines, tmp_file)

  cmd <- sprintf(
    "cat %s | docker exec -i realtime-analytics-platform-kafka-1 \
     kafka-console-producer --bootstrap-server kafka:9092 --topic %s",
    tmp_file, TOPIC
  )

  system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)
  unlink(tmp_file)

  invisible(length(events))
}

# Alternative: Write to file for Spark to read (simpler for local dev)
publish_to_file <- function(events, output_dir = "/tmp/kafka_simulation") {
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  file_path <- file.path(output_dir, sprintf("events_%s.json", timestamp))

  json_lines <- sapply(events, function(e) toJSON(e, auto_unbox = TRUE, na = "null"))
  writeLines(json_lines, file_path)

  message(sprintf("Written %d events to %s", length(events), file_path))
  invisible(file_path)
}

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
main <- function() {
  message(sprintf("Kafka Producer | Mode: %s | Topic: %s | Bootstrap: %s", MODE, TOPIC, BOOTSTRAP))

  if (MODE == "batch") {
    message(sprintf("Generating %d events...", COUNT))
    events <- replicate(COUNT, generate_event(), simplify = FALSE)
    publish_to_file(events)
    message(sprintf("Batch complete. Produced %d events.", COUNT))

  } else if (MODE == "continuous") {
    message(sprintf("Running for %d seconds at %d events/sec...", DURATION, RATE))
    start_time <- Sys.time()
    total_produced <- 0

    while (as.numeric(difftime(Sys.time(), start_time, units = "secs")) < DURATION) {
      batch_start <- Sys.time()
      events <- replicate(RATE, generate_event(), simplify = FALSE)
      publish_to_file(events)
      total_produced <- total_produced + RATE

      elapsed <- as.numeric(difftime(Sys.time(), batch_start, units = "secs"))
      sleep_time <- max(0, 1 - elapsed)
      if (sleep_time > 0) Sys.sleep(sleep_time)
    }

    message(sprintf("Continuous mode complete. Produced %d events over %d seconds.", 
                    total_produced, DURATION))
  }
}

if (!interactive()) {
  main()
}
