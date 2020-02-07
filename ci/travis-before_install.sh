# Pre-installation script for Linux/macOS CI on Travis

# Download files into the cache directory
maybe_download () {
  if [ ! -x $CACHE/$2 ]; then
    curl $1 --output $CACHE/$2 --remote-time --time-cond $CACHE/$2
  else
    curl $1 --output $CACHE/$2 --remote-time
  fi
  chmod +x $CACHE/$2
}

maybe_download $GAMSURL $GAMSFNAME
maybe_download $CONDAURL $CONDAFNAME


# Install R packages needed for testing
#
# Travis' R language support (https://docs.travis-ci.com/user/languages/r)
# provides travis.yml keys for installing R packages. However, these are only
# executed *after* the 'install' script from travis.yml—too late to set up
# dependencies for the commands we use. Thus, we need to install packages here.
Rscript - <<EOF
options(pkgType = 'source')
install.packages(c('devtools', 'IRkernel'), lib = '$R_LIBS_USER')
devtools::install_dev_deps('rixmp')
EOF


# Install graphiz on OS X (requires updating homebrew)
if [ `uname` = "Darwin" ];
then
  brew update
  brew install graphviz
fi
