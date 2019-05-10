#' ixmp core
#'
#' This page provides documentation for the ixmp-R-reticulate implementation.
#' The package is sourcing the ixmp-core Python package, translating it with the R 'reticulate' package.
#'
#' Main features:
#'
#'  - a platform core linked to a database instance,
#'
#'  - integration with the |MESSAGEix| Integrated Assessment model
#'    and other numerical models
#'
#'  - for full documentation of the Python functions, use py_help(<package>$<class>)
#' @name retixmp
NULL

#' @import reticulate
NULL

.onAttach <- function(libname, pkgname) {
  packageStartupMessage(
    sprintf(paste0("Loaded retixmp v%s. See ?retixmp for help, ",
                   "citation(\"retixmp\") for use in publication.\n"),
            utils::packageDescription(pkgname)$Version))
}

ixmp <- NULL

.onLoad <- function(libname, pkgname) {
  # Force reticulate to pick up on e.g. RETICULATE_PYTHON environment variable
  reticulate::py_config()

  # If $IXMP_DATA and $XDG_DATA_HOME are not set, ixmp.config.Config uses
  # $HOME/.local/ixmp for configuration and local databases. On Windows, $HOME
  # is C:\Users\[Username]\Documents. Here, XDG_DATA_HOME is set for the
  # session importing retixmp (and thus its embedded reticulate Python session)
  # to instead place these files in C:\Users\[Username]\.local\share

  # If the user has aleady set $IXMP_DATA or $XDG_DATA_HOME, do nothing
  vars_set = any(nchar(Sys.getenv(c('IXMP_DATA', 'XDG_DATA_HOME'))))
  if ( Sys.info()['sysname'] == 'Windows' & ! vars_set ) {
    # Split $HOME to components
    home <- strsplit(Sys.getenv('HOME'), .Platform$file.sep)[[1]]

    # Filter out 'Documents' and add '.local\share'
    xdg_data_home <- file.path(c(Filter(function (s) s != 'Documents', home),
                                 '.local', 'share'))

    # Set $XDG_DATA_HOME
    Sys.setenv(XDG_DATA_HOME=xdg_data_home)
  }

  # Set 'ixmp' in the global namespace
  ixmp <<- reticulate::import('ixmp', delay_load = TRUE)
}
