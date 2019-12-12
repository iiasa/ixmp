# Installation script for Linux/macOS CI on Travis

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

# Create named environment
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

# Install IR kernel spec for Jupyter notebook
Rscript -e "IRkernel::installspec()"
