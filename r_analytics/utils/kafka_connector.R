library(jsonlite)

kafka_produce <- function(events, output_dir = "/tmp/kafka_simulation") {
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  file_path <- file.path(output_dir, sprintf("events_%s.json", timestamp))
  json_lines <- sapply(events, function(e) toJSON(e, auto_unbox = TRUE, na = "null"))
  writeLines(json_lines, file_path)
  invisible(file_path)
}

kafka_consume <- function(input_dir = "/tmp/kafka_simulation", pattern = "events_.*\.json") {
  files <- list.files(input_dir, pattern = pattern, full.names = TRUE)
  if (length(files) == 0) return(data.frame())
  lines <- unlist(lapply(files, readLines))
  events <- lapply(lines, fromJSON)
  do.call(rbind, lapply(events, as.data.frame))
}
