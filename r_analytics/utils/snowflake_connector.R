library(DBI)

sf_connect <- function(
    account   = Sys.getenv("SNOWFLAKE_ACCOUNT"),
    user      = Sys.getenv("SNOWFLAKE_USER"),
    password  = Sys.getenv("SNOWFLAKE_PASSWORD"),
    database  = Sys.getenv("SNOWFLAKE_DATABASE", "ANALYTICS_DB"),
    schema    = Sys.getenv("SNOWFLAKE_SCHEMA", "PROD"),
    warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    role      = Sys.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
) {
  dbConnect(odbc::odbc(),
    Driver    = "Snowflake",
    Server    = paste0(account, ".snowflakecomputing.com"),
    UID       = user,
    PWD       = password,
    Database  = database,
    Schema    = schema,
    Warehouse = warehouse,
    Role      = role
  )
}

sf_query <- function(con, sql) { dbGetQuery(con, sql) }

sf_insert <- function(con, table, df) {
  dbWriteTable(con, table, df, append = TRUE, row.names = FALSE)
}
