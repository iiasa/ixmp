step <- commandArgs(TRUE)[1]

options(repos=c('https://cloud.r-project.org'))

if ( step == '1' ) {
  # Install only binary packages on AppVeyor/Windows. This prevents build
  # failures when the source package version is ahead of the binary version and
  # the AppVeyor worker lacks the necessary libraries etc. to compile the
  # source.
  options(pkgType='win.binary')

  install.packages(c('devtools', 'IRkernel'), quiet = TRUE)

  # Install the IR kernel specification so that Jupyter can handle R notebooks
  IRkernel::installspec()
} else if ( step == '2' ) {

  # upgrade = FALSE is to keep mime (above) at 0.6
  devtools::install(file.path('.', 'rixmp'),
                    args = '--no-multiarch',
                    dependencies = TRUE,
                    upgrade = FALSE)

}
