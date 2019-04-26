context("Core")

test_that('ixmp can be imported', {
  library(reticulate)

  # Force reticulate to pick up on e.g. RETICULATE_PYTHON environment variable
  py_config()

  expect(py_numpy_available(), 'reticulate reports numpy not available')
  ixmp <- import('ixmp')
})

test_that('a local Platform can be instantiated', {
  ixmp <- import('ixmp')
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
