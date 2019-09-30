# Install GAMS
$CACHE/$GAMSFNAME > install.out

# Show location
which gams


# Install and update conda
# -b: run in batch mode with no user input
# -u: update existing installation
# -p: install prefix
$CACHE/$CONDAFNAME -b -u -p $HOME/miniconda
conda update --yes conda

# Create named env
conda create -n testing python=$PYVERSION --yes

# Install dependencies
conda install -n testing -c conda-forge --yes \
      ixmp \
      codecov \
      pytest \
      pytest-cov
conda remove -n testing --force --yes ixmp

# Activate the environment. Use '.' (POSIX) instead of 'source' (a bashism).
. activate testing

# Show information
conda info --all

# Install R packages needed for testing
Rscript -e "install.packages(c('devtools', 'IRkernel'), lib = '$R_LIBS_USER')"
Rscript -e "IRkernel::installspec()"
