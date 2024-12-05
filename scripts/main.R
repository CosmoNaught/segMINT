library(dplyr)
library(cli)
source("re_fresh.R")
source("util.R")

re_fresh()


re_fresh <- function(all = TRUE, file = NULL, force = FALSE) {
  source_dir <- "/home/cosmo/net/malaria/Cosmo/MINT/archive/simulation_controller"
  target_dir <- "~/Documents/segMINT/offload_dir/outputs"
  log_dir <- "~/Documents/segMINT/offload_dir/logs"
  log_file <- file.path(log_dir, "offload_log.csv")
  
  target_dir <- path.expand(target_dir)
  log_dir <- path.expand(log_dir)
  log_file <- path.expand(log_file)
  
  if (!dir.exists(target_dir)) dir.create(target_dir, recursive = TRUE)
  if (!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)
  
  if (!file.access(target_dir, 2) == 0) {
    stop("Cannot write to target directory: ", target_dir)
  }
  
  existing_log <- if (file.exists(log_file)) {
    read.csv(log_file, stringsAsFactors = FALSE)
  } else {
    data.frame(file_name = character(),
               source_path = character(),
               target_path = character(),
               status = character(),
               stringsAsFactors = FALSE)
  }
  
  if (!is.null(file)) all <- FALSE
  
  if (all) {
    folder_paths <- list.dirs(source_dir, recursive = FALSE)
    total_files <- sum(sapply(folder_paths, function(folder) {
      length(list.files(folder, pattern = "simulation_results_\\d+\\.(RDS|rds)$", full.names = TRUE))
    }))
    pb <- cli::cli_progress_bar("Offloading files", total = total_files)
    
    for (folder in folder_paths) {
      rds_files <- list.files(folder, pattern = "simulation_results_\\d+\\.(RDS|rds)$", full.names = TRUE)
      for (file_path in rds_files) {
        file_name <- basename(file_path)
        target_file <- file.path(target_dir, file_name)
        
        if (!force && any(existing_log$file_name == file_name & existing_log$status == "offloaded")) {
          cli::cli_alert_info("Skipping already offloaded file: {file_name}")
          next
        }
        
        tryCatch({
          success <- file.copy(file_path, target_file, overwrite = TRUE)
          if (success) {
            Sys.chmod(target_file, mode = "0644")  # Unlock copied file
          }
          new_status <- ifelse(success, "offloaded", "failed to copy")
          
          if (file_name %in% existing_log$file_name) {
            existing_log <- existing_log %>%
              mutate(status = ifelse(file_name == !!file_name, new_status, status),
                     target_path = ifelse(file_name == !!file_name, target_file, target_path))
          } else {
            new_entry <- data.frame(
              file_name = file_name,
              source_path = file_path,
              target_path = target_file,
              status = new_status,
              stringsAsFactors = FALSE
            )
            existing_log <- bind_rows(existing_log, new_entry)
          }
          cli::cli_progress_update()
        }, error = function(e) {
          cli::cli_alert_warning("Failed to offload file: {file_name}, error: {e$message}")
        })
      }
    }
    write.csv(existing_log, log_file, row.names = FALSE)
    cli::cli_progress_done()
    message("Offloading completed.")
  } else {
    if (is.null(file)) stop("Please specify a file or set all = TRUE.")
    for (f in file) {
      file_name <- basename(f)
      source_file <- file.path(source_dir, f)
      target_file <- file.path(target_dir, file_name)
      
      if (!file.exists(source_file)) {
        cli::cli_alert_danger("The specified file does not exist: {source_file}")
        next
      }
      
      tryCatch({
        success <- file.copy(source_file, target_file, overwrite = TRUE)
        if (success) {
          Sys.chmod(target_file, mode = "0644")  # Unlock copied file
        }
        new_status <- ifelse(success, "offloaded", "failed to copy")
        
        if (file_name %in% existing_log$file_name) {
          existing_log <- existing_log %>%
            mutate(status = ifelse(file_name == !!file_name, new_status, status),
                   target_path = ifelse(file_name == !!file_name, target_file, target_path))
        } else {
          new_entry <- data.frame(
            file_name = file_name,
            source_path = source_file,
            target_path = target_file,
            status = new_status,
            stringsAsFactors = FALSE
          )
          existing_log <- bind_rows(existing_log, new_entry)
        }
        write.csv(existing_log, log_file, row.names = FALSE)
        if (success) {
          cli::cli_alert_success("File successfully offloaded: {file_name}")
        } else {
          cli::cli_alert_warning("Failed to offload file: {file_name}")
        }
      }, error = function(e) {
        cli::cli_alert_danger("Failed to offload file: {file_name}, error: {e$message}")
      })
    }
  }
}


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
