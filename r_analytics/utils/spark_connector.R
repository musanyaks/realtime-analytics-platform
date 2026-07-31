library(sparklyr)

spark_connect_custom <- function(master = Sys.getenv("SPARK_MASTER", "local[*]")) {
  config <- spark_config()
  config$`sparklyr.shell.packages` <- paste(
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "net.snowflake:snowflake-jdbc:3.14.3",
    "net.snowflake:spark-snowflake_2.12:2.12.0",
    sep = ","
  )
  spark_connect(master = master, app_name = "RAnalytics", config = config)
}
