# Create cache directory
CACHE=$HOME/.cache/ixmp
mkdir -p $CACHE

# Download files into the cache directory
maybe_download () {
  echo "Starting download from $1"
  curl $1 --output $CACHE/$2 --remote-time --time-cond $CACHE/$2
  echo "Download complete from $1"
}

# install gams
maybe_download $GAMSURL $GAMSFNAME

chmod +x $CACHE/$GAMSFNAME
$CACHE/$GAMSFNAME > install.out
which gams

# install and update conda
maybe_download $CONDAURL $CONDAFNAME

chmod +x $CACHE/$CONDAFNAME
$CACHE/$CONDAFNAME -b -p $HOME/miniconda
conda update --yes conda

# create named env
conda create -n testing python=$PYVERSION --yes

# install deps
conda install -n testing -c conda-forge --yes \
      ixmp \
      pytest \
      coveralls \
      pytest-cov
conda remove -n testing --force --yes ixmp
