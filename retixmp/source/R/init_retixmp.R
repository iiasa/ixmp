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
  # Set $XDG_DATA_HOME for ixmp.config.Config
  # If the user has aleady set $IXMP_DATA or $XDG_DATA_HOME, do nothing
  vars_set = any(nchar(Sys.getenv(c('IXMP_DATA', 'XDG_DATA_HOME'))))
  if ( Sys.info()['sysname'] == 'Windows' & ! vars_set ) {
    # Split $HOME to components
    home <- strsplit(Sys.getenv('HOME'), .Platform$file.sep)[[1]]
    # Filter out 'Documents' and add '.local\share'
    xdg_data_home <- file.path(Filter(function (s) s != 'Documents', home),
                               '.local', 'share')
    print(xdg_data_home)
    Sys.setenv(XDG_DATA_HOME=xdg_data_home)
  }

  ModelConfig <<- list(default = list(model_file = gsub("/","\\\\" , file.path( "." , "{model}.gms") ),
                                      inp = gsub("/","\\\\" , file.path( ".", "{model}_in.gdx") ),
                                      outp = gsub("/","\\\\" , file.path(".", "{model}_out.gdx") ),
                                      args = gsub("/","\\\\" , paste('--in=',file.path( ".", "{model}_in.gdx"),' --out=',file.path(".", "{model}_out.gdx")))))

  ixmp <<- reticulate::import('ixmp', delay_load = TRUE)
}
