#!/usr/bin/env Rscript
library(testthat)

# Live Snowflake tests only run when real credentials are provided.
# CI supplies dummy_* placeholders, so these are skipped there automatically.
is_live_snowflake <- function() {
  Sys.getenv("SNOWFLAKE_ACCOUNT") != "" &&
    Sys.getenv("SNOWFLAKE_ACCOUNT") != "dummy_account"
}

# All test_that() blocks run inside with_reporter() so we can read pass/fail
# counts back afterwards WITHOUT re-scanning/re-sourcing this file (test_dir(".")
# would match and re-run test_pipeline.R itself, causing infinite recursion and
# a C stack overflow -- this is why that approach was removed).
reporter <- SummaryReporter$new()

with_reporter(reporter, {
  context("Real-Time Analytics Pipeline Tests")

  test_that("Required environment variables are set", {
    expect_true(nchar(Sys.getenv("SNOWFLAKE_ACCOUNT")) > 0)
    expect_true(nchar(Sys.getenv("SNOWFLAKE_USER")) > 0)
    expect_true(nchar(Sys.getenv("SNOWFLAKE_PASSWORD")) > 0)
  })

  test_that("Can connect to Snowflake", {
    skip_if_not(is_live_snowflake(), "No live Snowflake credentials available")
    source("../snowflake/connection.R")
    con <- snowflake_connect()
    on.exit(sf_disconnect(con))
    info <- test_connection(con)
    expect_true(nrow(info) == 1)
  })

  test_that("Production tables exist", {
    skip_if_not(is_live_snowflake(), "No live Snowflake credentials available")
    source("../snowflake/connection.R")
    con <- snowflake_connect()
    on.exit(sf_disconnect(con))
    tables <- c("PROD.FCT_USER_EVENTS", "PROD.FCT_HOURLY_METRICS", "PROD.DIM_USERS")
    for (tbl in tables) {
      count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", tbl))
      expect_true(count$n >= 0)
    }
  })

  test_that("dbt project files exist", {
    expect_true(file.exists("../dbt/dbt_project.yml"))
    expect_true(file.exists("../dbt/profiles.yml"))
    expect_true(file.exists("../dbt/models/staging/stg_events.sql"))
    expect_true(file.exists("../dbt/models/marts/fct_user_events.sql"))
  })

  test_that("Kafka producer generates valid events", {
    source("../kafka/producer.R")
    events <- replicate(10, generate_event(), simplify = FALSE)
    expect_equal(length(events), 10)
    required_fields <- c("event_id", "event_timestamp", "user_id", "event_type", "session_id")
    for (e in events) {
      for (field in required_fields) {
        expect_true(field %in% names(e))
      }
    }
  })
})

if (!interactive()) {
  n_failed <- reporter$failed
  if (n_failed > 0) {
    message(sprintf("%d test failure(s).", n_failed))
    quit(status = 1)
  } else {
    message("All tests passed!")
    quit(status = 0)
  }
}
