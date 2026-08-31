#!/usr/bin/env Rscript
library(testthat)

# Live Snowflake tests only run when real credentials are provided.
# CI supplies dummy_* placeholders, so these are skipped there automatically.
is_live_snowflake <- function() {
  Sys.getenv("SNOWFLAKE_ACCOUNT") != "" &&
    Sys.getenv("SNOWFLAKE_ACCOUNT") != "dummy_account"
}

run_tests <- function() {
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
}

# Guard against self-recursion: this script previously ended with
# `test_dir(".")`, which re-scanned this directory, matched this same file,
# and re-sourced it -- causing infinite recursion and a C stack overflow.
#
# PIPELINE_TESTS_DRIVER is a sentinel we control ourselves (not testthat
# internals), so correctness here doesn't depend on testthat's version or
# undocumented behavior:
#   - First run (direct `Rscript test_pipeline.R`, sentinel unset): set the
#     sentinel, then drive the suite through test_file() exactly once to get
#     a proper results object (with $failed counts) and a correct exit code.
#   - Second run (this file being sourced BY that test_file() call, sentinel
#     now set): just define and run the tests, then return control to the
#     driver above -- do NOT recurse again.
if (Sys.getenv("PIPELINE_TESTS_DRIVER") == "true") {
  run_tests()
} else {
  Sys.setenv(PIPELINE_TESTS_DRIVER = "true")
  this_file <- sub("^--file=", "", grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE))
  results <- test_file(this_file, reporter = "summary")
  df <- as.data.frame(results)
  if (any(df$failed > 0)) {
    message(sprintf("%d test failure(s).", sum(df$failed)))
    quit(status = 1)
  } else {
    message("All tests passed!")
    quit(status = 0)
  }
}
