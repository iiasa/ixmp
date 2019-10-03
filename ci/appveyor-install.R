# Installation script for Windows CI on Appveyor
#
# This script takes a single 'step' argument, either '1' or '2'. On Travis,
# these steps are handled by lines in travis-install.sh and .travis.yml,
# respectively. On Windows/Appveyor, the options(…) call must be given for both
# steps, so they are combined in this file.

step <- commandArgs(TRUE)[1]

# Install *only* binary packages on Windows/AppVeyor. This prevents build
# failures that occur when a package's source version is ahead of its binary
# version, and the AppVeyor worker lacks the necessary libraries etc. to
# compile the source.
options(repos=c('https://cloud.r-project.org'), pkgType='win.binary')

if ( step == '1' ) {
  # Step 1: Install packages needed for testing
  install.packages(c('devtools', 'IRkernel'), quiet = TRUE)

  # Install the IR kernel specification so that Jupyter can handle R notebooks
  IRkernel::installspec()
} else if ( step == '2' ) {
  devtools::install(file.path('.', 'rixmp'),
                    args = '--no-multiarch',
                    dependencies = TRUE)
}
