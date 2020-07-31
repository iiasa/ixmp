.test_mp_count <- 0

test_mp <- function () {
  # An ixmp.Platform connected to a temporary, local database.
  #
  # This function mirrors the pytest fixture of the same name, defined in
  # ixmp.testing.
  # FIXME use pytest internals to call the fixture functions directly

  # Name for the temporary platform
  url <- paste0("jdbc:hsqldb:mem:rixmp ", .test_mp_count)
  .test_mp_count <<- .test_mp_count + 1

  # launch Platform and connect to testdb (reconnect if closed)
  mp <- ixmp$Platform(backend = "jdbc", driver = "hsqldb", url = url)
  mp$open_db()

  return(mp)
}
