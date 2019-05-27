context("Core")

# Simulate 'library(retixmp)' without installing
devtools::load_all(file.path('..', '..'))

# Import fixtures
source('conftest.R')


test_that('a local Platform can be instantiated', {
  mp <- ixmp$Platform(dbtype = 'HSQLDB')
  expect_equal(mp$dbtype, 'HSQLDB')
})

test_that('a new Scenario can be instantiated', {
  # TODO complete this test
  succeed()
})

test_that('parameter values can be set on a Scenario', {
  # TODO complete this test
  succeed()
})

test_that('the canning problem can be solved', {
  # Create the Scenario
  mp <- test_mp()
  scen <- ixmp$testing$dantzig_transport(mp)

  # Solve
  model_path = file.path(Sys.getenv('IXMP_TEST_DATA_PATH'), 'transport_ixmp')
  scen$solve(model = model_path)

  # Check value
  expect_equal(scen$var('z')$lvl, 153.675, tolerance = 1e-5)
})
