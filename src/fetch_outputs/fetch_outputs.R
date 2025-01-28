library(duckdb)
library(utils)

# Create a global data frame to store problematic simulations
problematic_sims <- data.frame(
  parameter_index = integer(),
  simulation_index = integer(),
  global_index         = character(),
  stringsAsFactors = FALSE
)

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
config <- list(
  my_folder = "/home/cosmo/net/malaria/Cosmo/MINT/archive/simulation_controller/20241216-153857-9eb5b453",
  N = "all",  # or an integer
  db_path = "/home/cosmo/net/malaria/Cosmo/segMINT/db.duckdb",
  table_name = "simulation_results"
)

# -------------------------------------------------------------------
# STEP 1: LIST .RDS FILES
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# STEP 2: FLATTEN NESTED LISTS
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# STEP 3: PROCESS A SINGLE FILE
# -------------------------------------------------------------------
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
          global_index         = basename(rds_path),
          stringsAsFactors = FALSE
        )
      )
      return(NULL)
    }
    
    df_out <- data.frame(
      parameter_index        = param_index,
      simulation_index       = i,
      global_index         = basename(rds_path),
      timesteps             = out$timestep,
      n_detect_lm_0_1825    = out$n_detect_lm_0_1825,
      n_age_0_1825          = out$n_age_0_1825,
      n_inc_clinical_0_36500= out$n_inc_clinical_0_36500,
      n_age_0_36500         = out$n_age_0_36500
    )
    cbind(df_out, df_params[rep(1, nrow(df_out)), ])
  })
  
  # Drop any NULL entries (the skipped sims) before row-binding
  do.call(rbind, Filter(Negate(is.null), df_list))
}

# -------------------------------------------------------------------
# STEP 4: WRITE INCREMENTALLY TO DUCKDB + SKIP PROBLEM FILES
# -------------------------------------------------------------------
write_to_duckdb <- function(files, db_path, table_name) {
  con <- dbConnect(duckdb(), dbdir = db_path, read_only = FALSE)
  on.exit(dbDisconnect(con), add = TRUE)
  
  first_file <- TRUE
  pb <- txtProgressBar(min = 0, max = length(files), style = 3)
  
  for (i in seq_along(files)) {
    file_path <- files[i]
    
    df <- tryCatch({
      process_rds_file(file_path, param_index = i)
    }, error = function(e) {
      message(sprintf("Error encountered while processing '%s': %s", file_path, e$message))
      # Continue to the next file rather than stopping the entire pipeline
      return(NULL)
    })
    
    # If df is NULL, skip writing to DB
    if (!is.null(df) && nrow(df) > 0) {
      dbWriteTable(
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
    setTxtProgressBar(pb, i)
  }
  close(pb)
}

# -------------------------------------------------------------------
# STEP 5: DEMO QUERIES
# -------------------------------------------------------------------
demo_queries <- function(db_path, table_name) {
  con <- dbConnect(duckdb(), dbdir = db_path)
  on.exit(dbDisconnect(con), add = TRUE)
  
  queried_data1 <- dbGetQuery(con, sprintf("
    SELECT timesteps, n_detect_lm_0_1825, parameter_index, simulation_index index, global_index
    FROM %s
    LIMIT 10", table_name))
  
  queried_data2 <- dbGetQuery(con, sprintf("
    SELECT timesteps, n_detect_lm_0_1825, parameter_index, simulation_index, global_index
    FROM %s
    WHERE timesteps = 3000", table_name))
  
  queried_data3 <- dbGetQuery(con, sprintf("
    SELECT parameter_index, n_detect_lm_0_1825, n_age_0_1825, n_inc_clinical_0_36500, n_age_0_36500 , parameter_index, simulation_index, global_index
    FROM %s
    WHERE seasonal = 0
    LIMIT 10", table_name))
  
  df_all <- dbGetQuery(con, sprintf("SELECT * FROM %s", table_name))
  
  cat("Tail of entire data:\n")
  print(tail(df_all))
  
  cat("\nFirst 10 rows:\n")
  print(queried_data1)
  
  cat("\nRows where timesteps = 3000:\n")
  print(queried_data2)
  
  cat("\nFirst 10 rows where seasonal = 0:\n")
  print(queried_data3)
}

# -------------------------------------------------------------------
# MAIN PIPELINE
# -------------------------------------------------------------------
run_pipeline <- function(cfg) {
  files <- list_rds_files(cfg$my_folder, cfg$N)
  if (!length(files)) {
    stop("No .rds files found to process.")
  }
  
  write_to_duckdb(files, cfg$db_path, cfg$table_name)
  
  # After processing is done, write the 'problematic_sims' data frame to CSV
  # (if it has any rows)
  if (nrow(problematic_sims) > 0) {
    write.csv(problematic_sims, "problematic_simulations.csv", row.names = FALSE)
    message("Wrote problematic simulations to 'problematic_simulations.csv'.")
  }
  
  demo_queries(cfg$db_path, cfg$table_name)
}

# Execute the pipeline
run_pipeline(config)
