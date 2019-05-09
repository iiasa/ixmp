# Install GAMS
$CACHE/$GAMSFNAME > install.out

# Show location
which gams


# Install and update conda
$CACHE/$CONDAFNAME -b -u -p $HOME/miniconda
conda update --yes conda

# Create named env
conda create -n testing python=$PYVERSION --yes

# Install deps
conda install -n testing -c conda-forge --yes \
      ixmp \
      pytest \
      coveralls \
      pytest-cov
conda remove -n testing --force --yes ixmp

# Use '.' (POSIX) instead of 'source' (a bashism)
. activate testing

# Show information
conda info --all

# Install R packages needed for testing
Rscript -e "install.packages(c('devtools', 'IRkernel'), lib = '$R_LIBS_USER')"
Rscript -e "IRkernel::installspec()"
