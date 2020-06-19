context('Reporting')


test_that('the canning problem can be reported', {
  # Create the Scenario
  mp <- test_mp()
  scen <- ixmp$testing$make_dantzig(mp, solve = TRUE)

  # Reporter.from_scenario can handle the Dantzig problem
  rep <- ixmp$reporting$Reporter$from_scenario(scen)

  # Partial sums are available
  d_i <- rep$get('d:i')
})
