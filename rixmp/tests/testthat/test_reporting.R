test_that('the canning problem can be reported', {
  # Create the Scenario
  mp <- test_mp()
  scen <- ixmp$testing$make_dantzig(mp)

  # Solve
  model_path = file.path(Sys.getenv('IXMP_TEST_DATA_PATH'), 'transport_ixmp')
  scen$solve(model = model_path)

  # Reporter.from_scenario can handle the Dantzig problem
  rep <- ixmp$reporting$Reporter$from_scenario(scen)

  # Partial sums are available
  d_i <- rep$get('d:i')
})
