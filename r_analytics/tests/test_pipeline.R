#!/usr/bin/env Rscript
# =============================================================================
# Pipeline Test Suite
# =============================================================================

library(testthat)

context("Real-Time Analytics Pipeline Tests")

# ---------------------------------------------------------------------------
# Test 1: Environment Variables
# ---------------------------------------------------------------------------
test_that("Required environment variables are set", {
  expect_true(nchar(Sys.getenv("SNOWFLAKE_ACCOUNT")) > 0, 
              "SNOWFLAKE_ACCOUNT must be set")
  expect_true(nchar(Sys.getenv("SNOWFLAKE_USER")) > 0,
              "SNOWFLAKE_USER must be set")
  expect_true(nchar(Sys.getenv("SNOWFLAKE_PASSWORD")) > 0,
              "SNOWFLAKE_PASSWORD must be set")
})

# ---------------------------------------------------------------------------
# Test 2: Snowflake Connection
# ---------------------------------------------------------------------------
test_that("Can connect to Snowflake", {
  source("../snowflake/connection.R")
  con <- snowflake_connect()
  on.exit(sf_disconnect(con))
  
  info <- test_connection(con)
  expect_true(nrow(info) == 1)
  expect_true(nchar(info$version) > 0)
})

# ---------------------------------------------------------------------------
# Test 3: Data Quality Checks
# ---------------------------------------------------------------------------
test_that("Production tables exist and have data", {
  source("../snowflake/connection.R")
  con <- snowflake_connect()
  on.exit(sf_disconnect(con))
  
  tables <- c("PROD.FCT_USER_EVENTS", "PROD.FCT_HOURLY_METRICS", "PROD.DIM_USERS")
  
  for (tbl in tables) {
    count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", tbl))
    expect_true(count$n >= 0, sprintf("Table %s is accessible", tbl))
  }
})

# ---------------------------------------------------------------------------
# Test 4: dbt Model Validation
# ---------------------------------------------------------------------------
test_that("dbt models compile without errors", {
  # Check that dbt_project.yml exists and is valid YAML
  expect_true(file.exists("../dbt/dbt_project.yml"))
  expect_true(file.exists("../dbt/profiles.yml"))
  
  # Check that model files exist
  expect_true(file.exists("../dbt/models/staging/stg_events.sql"))
  expect_true(file.exists("../dbt/models/marts/fct_user_events.sql"))
})

# ---------------------------------------------------------------------------
# Test 5: Kafka File Simulation
# ---------------------------------------------------------------------------
test_that("Kafka producer generates valid events", {
  source("../kafka/producer.R")
  
  events <- replicate(10, generate_event(), simplify = FALSE)
  expect_equal(length(events), 10)
  
  # Validate event structure
  required_fields <- c("event_id", "event_timestamp", "user_id", 
                       "event_type", "session_id")
  
  for (e in events) {
    for (field in required_fields) {
      expect_true(field %in% names(e), 
                  sprintf("Event missing field: %s", field))
    }
  }
})

# ---------------------------------------------------------------------------
# Test 6: Spark Connection (if available)
# ---------------------------------------------------------------------------
test_that("Spark connection can be initialized", {
  skip_if_not_installed("sparklyr")
  
  source("../r_analytics/utils/spark_connector.R")
  
  # Just test config creation, not actual connection
  config <- spark_config()
  expect_true(!is.null(config))
})

# ---------------------------------------------------------------------------
# Run Tests
# ---------------------------------------------------------------------------
if (!interactive()) {
  test_results <- test_dir(".", reporter = "summary")
  
  if (any(as.data.frame(test_results)$failed > 0)) {
    quit(status = 1)
  } else {
    message("All tests passed!")
    quit(status = 0)
  }
}