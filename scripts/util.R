
eval_log <- function() {
  # Directories and log file path
  target_dir <- "~/Documents/segMINT/offload_dir/outputs"
  log_dir <- "~/Documents/segMINT/offload_dir/logs"
  log_file <- file.path(log_dir, "offload_log.csv")
  
  # Expand paths
  target_dir <- path.expand(target_dir)
  log_file <- path.expand(log_file)
  
  # Check if log file exists
  if (!file.exists(log_file)) {
    stop("Log file does not exist at: ", log_file)
  }
  
  # Read the log file
  log_data <- read.csv(log_file, stringsAsFactors = FALSE)
  
  # List of files present in the target directory
  target_files <- list.files(target_dir, full.names = FALSE)
  
  # Extract relative paths for missing files
  missing_files <- log_data %>%
    filter(status == "offloaded" & !(file_name %in% target_files)) %>%
    mutate(relative_path = gsub("^.+/simulation_controller/", "", source_path)) %>%
    pull(relative_path)
  
  # Extract relative paths for failed files
  failed_files <- log_data %>%
    filter(status == "failed to copy") %>%
    mutate(relative_path = gsub("^.+/simulation_controller/", "", source_path)) %>%
    pull(relative_path)
  
  # Combine missing and failed files
  unresolved_files <- c(missing_files, failed_files)
  
  # Initialize re_fresh_command
  re_fresh_command <- NULL
  
  # Generate the re_fresh command if unresolved files exist
  if (length(unresolved_files) > 0) {
    re_fresh_command <- paste0("re_fresh(file = c(", 
                                paste(shQuote(unresolved_files), collapse = ", "), 
                                "))")
    cli::cli_alert_warning("To resolve this, run the following command:")
    cat(re_fresh_command, "\n")
  } else {
    cli::cli_alert_success("No unresolved files. All files are accounted for.")
  }
  
  # Print the results
  if (length(missing_files) > 0) {
    cli::cli_alert_warning("The following files are missing in outputs despite being logged as offloaded:")
    print(missing_files)
  } else {
    cli::cli_alert_success("No missing files found.")
  }
  
  if (length(failed_files) > 0) {
    cli::cli_alert_warning("The following files failed to copy:")
    print(failed_files)
  } else {
    cli::cli_alert_success("No failed files found.")
  }
  
  # Return lists of missing and failed files
  return(list(
    missing_files = missing_files,
    failed_files = failed_files,
    re_fresh_command = re_fresh_command
  ))
}
