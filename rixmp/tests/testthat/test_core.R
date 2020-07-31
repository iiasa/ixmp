context('Core')


test_that('correct Python and dependencies are used', {
  ixmp$show_versions()
})


test_that('the JVM can be started', {
  ixmp$backend$jdbc$start_jvm()
})


test_that('a local Platform can be instantiated', {
  ixmp$Platform(backend = "jdbc", driver = "hsqldb",
                url="jdbc:hsqldb:mem:rixmp test")
})


test_that('set, mapping sets and par values can be set on a Scenario', {
  mp <- test_mp()

  # Details for creating a new scenario in the ix modeling platform
  model <- 'test_model'
  scenario  <- 'standard'
  annot <- 'scenario for testing retixmp'

  # Initialize a new ixmp.Scenario
  # The parameter version='new' indicates that this is a new scenario instance
  scen <- ixmp$Scenario(mp, model, scenario, 'new', annotation = annot)
  expect_silent(scen)

  # Define the sets of locations of canning plants
  scen$init_set('i')
  i.set = c('seattle', 'san-diego')
  scen$add_set('i', i.set)
  expect_equal(as.character(scen$set('i')), i.set)

  scen$init_set('j')
  j.set = c('country', 'city')
  scen$add_set('j', j.set)
  map_ij.set = data.frame(i = i.set, j = 'city', stringsAsFactors = F)
  scen$init_set('map_ij', c('i', 'j'))
  scen$add_set('map_ij', adapt_to_ret(map_ij.set))
  a = scen$set('map_ij')
  attributes(a)$pandas.index = NULL
  expect_equal(a, map_ij.set)

  scen$commit('initialize scenario')
  scen$check_out()

  # Initialize parameter
  scen$init_par('a', c('i'))
  a.df = data.frame(i = c('seattle', 'san-diego'), value = c(350, 600),
                    unit = 'cases', stringsAsFactors = F)
  scen$add_par('a', adapt_to_ret(a.df))
  a = scen$par('a')
  attributes(a)$pandas.index = NULL

  expect_equal(a, a.df)
})


test_that('the canning problem can be solved', {
  # Create the Scenario
  mp <- test_mp()

  scen <- ixmp$testing$make_dantzig(mp, solve = TRUE)

  # Check value
  expect_equal(scen$var("z")$lvl, 153.675, tolerance = 1e-4)
})
