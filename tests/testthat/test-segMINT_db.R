test_that("list_rds_files works correctly", {
  # Create temp directory with test files
  temp_dir <- tempdir()
  test_files <- c("simulation_results_1.rds", "simulation_results_2.rds", "other_file.txt")
  
  for (f in test_files) {
    file.create(file.path(temp_dir, f))
  }
  
  # Test "all" option
  files <- segMINT:::list_rds_files(temp_dir, "all")
  expect_length(files, 2)
  expect_true(all(grepl("simulation_results_.*\\.rds$", basename(files))))
  
  # Test numeric option
  files <- segMINT:::list_rds_files(temp_dir, 1)
  expect_length(files, 1)
  
  # Clean up
  unlink(file.path(temp_dir, test_files))
})

test_that("flatten_list works correctly", {
  test_list <- list(
    a = 1,
    b = list(
      c = 2,
      d = list(e = 3)
    )
  )
  
  result <- segMINT:::flatten_list(test_list)
  
  expect_equal(result$a, 1)
  expect_equal(result$b_c, 2)
  expect_equal(result$b_d_e, 3)
  expect_length(result, 3)
})

test_that("get_results handles missing files gracefully", {
  expect_error(get_results("/non/existent/path", "all"), 
               "No .rds files found to process")
})

test_that("create_database validates inputs", {
  expect_error(
    create_database("/tmp", "test.db", "table"),
    "If data is not provided, both data_dir and N must be specified"
  )
})