#' Get simulation results from RDS files
#'
#' Reads and processes simulation result files from a specified directory
#'
#' @param dir Character string. Path to directory containing simulation_results_*.rds files
#' @param N Either "all" to process all files, or an integer to process files 1:N
#'
#' @return A data frame containing processed simulation results with flattened parameters
#' @export
#'
#' @examples
#' \dontrun{
#' results <- get_results("/path/to/data", "all")
#' results <- get_results("/path/to/data", 100)
#' }
get_results <- function(dir, N = "all") {
  files <- list_rds_files(dir, N)
  if (!length(files)) {
    stop("No .rds files found to process.")
  }
  
  # Process all files and combine
  all_results <- list()
  pb <- utils::txtProgressBar(min = 0, max = length(files), style = 3)
  
  for (i in seq_along(files)) {
    file_path <- files[i]
    df <- tryCatch({
      process_rds_file(file_path, param_index = i)
    }, error = function(e) {
      message(sprintf("Error encountered while processing '%s': %s", file_path, e$message))
      return(NULL)
    })
    
    if (!is.null(df) && nrow(df) > 0) {
      all_results[[i]] <- df
    }
    
    utils::setTxtProgressBar(pb, i)
  }
  close(pb)
  
  # Combine all results
  combined_df <- do.call(rbind, Filter(Negate(is.null), all_results))
  
  # Return info about problematic simulations if any
  if (exists("problematic_sims") && nrow(problematic_sims) > 0) {
    attr(combined_df, "problematic_sims") <- problematic_sims
    message(sprintf("Note: %d problematic simulations were skipped. Access with attr(results, 'problematic_sims')", 
                    nrow(problematic_sims)))
  }
  
  combined_df
}

#' Create DuckDB database from simulation results
#'
#' Creates or updates a DuckDB database with simulation results
#'
#' @param dir Character string. Directory where the database will be created
#' @param file_name Character string. Name of the database file (e.g., "db_test.duckdb")
#' @param table_name Character string. Name of the table to create/update
#' @param data Data frame. Optional. If provided, writes this data to the database. 
#'             If NULL, you must provide data_dir and N parameters
#' @param data_dir Character string. Directory containing RDS files (required if data is NULL)
#' @param N Either "all" or an integer. Number of files to process (required if data is NULL)
#' @param randomize Logical. Whether to randomize file processing order (default: FALSE)
#' @param demo Logical. Whether to run demo queries after creating database (default: TRUE)
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @examples
#' \dontrun{
#' # Option 1: Process files directly to database
#' create_database("/path/to/db", "my_db.duckdb", "results_table", 
#'                 data_dir = "/path/to/data", N = "all")
#' 
#' # Option 2: Use pre-processed data
#' results <- get_results("/path/to/data", "all")
#' create_database("/path/to/db", "my_db.duckdb", "results_table", data = results)
#' }
create_database <- function(dir, file_name, table_name, 
                          data = NULL, data_dir = NULL, N = NULL,
                          randomize = FALSE, demo = TRUE) {
  
  # Validate inputs
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
  
  db_path <- file.path(dir, file_name)
  
  if (is.null(data)) {
    if (is.null(data_dir) || is.null(N)) {
      stop("If data is not provided, both data_dir and N must be specified")
    }
    
    files <- list_rds_files(data_dir, N)
    if (!length(files)) {
      stop("No .rds files found to process.")
    }
    
    if (randomize) {
      set.seed(123)
      files <- sample(files)
      message("Database will be randomized")
    }
    
    write_to_duckdb(files, db_path, table_name)
    
    # Write problematic simulations if any
    if (exists("problematic_sims") && nrow(problematic_sims) > 0) {
      prob_file <- file.path(dir, "problematic_simulations.csv")
      utils::write.csv(problematic_sims, prob_file, row.names = FALSE)
      message(sprintf("Wrote problematic simulations to '%s'.", prob_file))
    }
    
  } else {
    # Write pre-processed data to database
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
    
    DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)
    message(sprintf("Wrote %d rows to table '%s' in database '%s'", 
                    nrow(data), table_name, db_path))
  }
  
  if (demo) {
    demo_queries(db_path, table_name)
  }
  
  invisible(TRUE)
}

# -------------------------------------------------------------------
# INTERNAL FUNCTIONS (not exported)
# -------------------------------------------------------------------

# Initialize problematic_sims in package namespace
.onLoad <- function(libname, pkgname) {
  assign("problematic_sims", data.frame(
    parameter_index = integer(),
    simulation_index = integer(),
    global_index = character(),
    stringsAsFactors = FALSE
  ), envir = parent.env(environment()))
}

# List RDS files
list_rds_files <- function(folder, N) {
  if (N == "all") {
    files <- list.files(
      path = folder,
      pattern = "^simulation_results_.*\\.rds$",
      full.names = TRUE
    )
  } else {
    candidate_paths <- file.path(folder, paste0("simulation_results_", seq_len(N), ".rds"))
    files <- candidate_paths[file.exists(candidate_paths)]
  }
  files
}

# Flatten nested lists
flatten_list <- function(x, parent_key = "") {
  out <- list()
  for (nm in names(x)) {
    val <- x[[nm]]
    new_name <- if (parent_key == "") nm else paste0(parent_key, "_", nm)
    if (is.list(val) && !is.null(names(val))) {
      deeper <- flatten_list(val, new_name)
      out <- c(out, deeper)
    } else {
      out[[new_name]] <- val
    }
  }
  out
}

# Process a single RDS file
process_rds_file <- function(rds_path, param_index) {
  # Read the RDS
  res <- readRDS(rds_path)
  
  # Flatten MINT_parameters
  flattened_params <- flatten_list(res$input$MINT_parameters)
  df_params <- as.data.frame(flattened_params, stringsAsFactors = FALSE)

  # Convert treatment timesteps to comma-separated strings
  bednet <- res$input$treatment_timesteps$mass_bednet
  irs    <- res$input$treatment_timesteps$irs
  lsm    <- res$input$treatment_timesteps$lsm
  
  df_params$treatment_dt_bednet <- if (!is.null(bednet)) paste(bednet, collapse = ",") else NA
  df_params$treatment_dt_irs    <- if (!is.null(irs))    paste(irs, collapse = ",")    else NA
  df_params$treatment_dt_lsm    <- if (!is.null(lsm))    paste(lsm, collapse = ",")    else NA

  # Combine each simulation's output with flattened parameters
  df_list <- lapply(seq_along(res$outputs), function(i) {
    out <- res$outputs[[i]]
    
    # If 'timestep' is length zero, we skip and record the problem
    if (length(out$timestep) == 0) {
      message(sprintf("\nSkipping empty simulation: file='%s', param_index=%d, simulation_index=%d",
                      rds_path, param_index, i))
      # Append the skip info to the global data frame
      problematic_sims <<- rbind(
        problematic_sims,
        data.frame(
          parameter_index = param_index,
          simulation_index = i,
          global_index = basename(rds_path),
          stringsAsFactors = FALSE
        )
      )
      return(NULL)
    }
    
    df_out <- data.frame(
      parameter_index        = param_index,
      simulation_index       = i,
      global_index           = basename(rds_path),
      timesteps              = out$timestep,
      n_detect_lm_0_1825     = out$n_detect_lm_0_1825,
      n_age_0_1825           = out$n_age_0_1825,
      n_inc_clinical_0_36500 = out$n_inc_clinical_0_36500,
      n_age_0_36500          = out$n_age_0_36500
    )
    cbind(df_out, df_params[rep(1, nrow(df_out)), ])
  })
  
  # Drop any NULL entries (the skipped sims) before row-binding
  do.call(rbind, Filter(Negate(is.null), df_list))
}

# Write to DuckDB incrementally
write_to_duckdb <- function(files, db_path, table_name) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  first_file <- TRUE
  pb <- utils::txtProgressBar(min = 0, max = length(files), style = 3)
  
  for (i in seq_along(files)) {
    file_path <- files[i]
    
    df <- tryCatch({
      process_rds_file(file_path, param_index = i)
    }, error = function(e) {
      message(sprintf("Error encountered while processing '%s': %s", file_path, e$message))
      return(NULL)
    })
    
    # If df is NULL, skip writing to DB
    if (!is.null(df) && nrow(df) > 0) {
      DBI::dbWriteTable(
        conn = con,
        name = table_name,
        value = df,
        overwrite = first_file,
        append = !first_file
      )
      first_file <- FALSE
    }
    
    rm(df)
    gc()
    utils::setTxtProgressBar(pb, i)
  }
  close(pb)
}