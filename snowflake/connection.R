#!/usr/bin/env Rscript
# =============================================================================
# Snowflake Connection Utility (R)
# =============================================================================
# Provides a reusable Snowflake connection using ODBC or JDBC.
#
# Usage:
#   source("snowflake/connection.R")
#   con <- snowflake_connect()
#   dbGetQuery(con, "SELECT CURRENT_VERSION()")
# =============================================================================

library(DBI)

#' Create a Snowflake database connection
#'
#' @param account Snowflake account identifier
#' @param user Username
#' @param password Password
#' @param database Database name
#' @param schema Schema name
#' @param warehouse Warehouse name
#' @param role Role name
#' @param driver Driver type: "odbc" or "jdbc"
#' @return DBI connection object
#' @export
snowflake_connect <- function(
    account   = Sys.getenv("SNOWFLAKE_ACCOUNT"),
    user      = Sys.getenv("SNOWFLAKE_USER"),
    password  = Sys.getenv("SNOWFLAKE_PASSWORD"),
    database  = Sys.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DB"),
    schema    = Sys.getenv("SNOWFLAKE_SCHEMA", "PROD"),
    warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    role      = Sys.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    driver    = c("odbc", "jdbc")
) {
  driver <- match.arg(driver)

  if (driver == "odbc") {
    if (!requireNamespace("odbc", quietly = TRUE)) {
      stop("Package 'odbc' is required. Install with: install.packages('odbc')")
    }

    con <- dbConnect(odbc::odbc(),
      Driver    = "Snowflake",
      Server    = paste0(account, ".snowflakecomputing.com"),
      UID       = user,
      PWD       = password,
      Database  = database,
      Schema    = schema,
      Warehouse = warehouse,
      Role      = role
    )

  } else if (driver == "jdbc") {
    if (!requireNamespace("RJDBC", quietly = TRUE)) {
      stop("Package 'RJDBC' is required. Install with: install.packages('RJDBC')")
    }

    jdbc_url <- paste0(
      "jdbc:snowflake://", account, ".snowflakecomputing.com/",
      "?db=", database,
      "&schema=", schema,
      "&warehouse=", warehouse,
      "&role=", role
    )

    # Note: snowflake-jdbc.jar must be in classpath
    con <- RJDBC::dbConnect(
      RJDBC::JDBC("net.snowflake.client.jdbc.SnowflakeDriver", 
                  "/path/to/snowflake-jdbc.jar"),
      jdbc_url,
      user,
      password
    )
  }

  message(sprintf("Connected to Snowflake: %s/%s.%s", account, database, schema))
  return(con)
}

#' Test Snowflake connection and return basic info
#'
#' @param con DBI connection object
#' @return Data frame with connection info
#' @export
test_connection <- function(con) {
  info <- dbGetQuery(con, "SELECT CURRENT_VERSION() AS version, CURRENT_DATABASE() AS database, CURRENT_SCHEMA() AS schema, CURRENT_WAREHOUSE() AS warehouse")
  return(info)
}

#' Execute a query and return results as a tibble
#'
#' @param con DBI connection object
#' @param query SQL query string
#' @return Tibble with query results
#' @export
sf_query <- function(con, query) {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    return(dbGetQuery(con, query))
  }
  dplyr::tbl(con, dplyr::sql(query))
}

#' Close Snowflake connection safely
#'
#' @param con DBI connection object
#' @export
sf_disconnect <- function(con) {
  if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con)
    message("Snowflake connection closed.")
  }
}

# ---------------------------------------------------------------------------
# Main: Test connection if run directly
# ---------------------------------------------------------------------------
if (!interactive() && sys.nframe() == 0) {
  message("Testing Snowflake connection...")
  con <- snowflake_connect()
  on.exit(sf_disconnect(con))

  info <- test_connection(con)
  print(info)

  # Test query
  tables <- dbGetQuery(con, "SHOW TABLES IN SCHEMA PROD")
  message(sprintf("Found %d tables in PROD schema", nrow(tables)))
}
