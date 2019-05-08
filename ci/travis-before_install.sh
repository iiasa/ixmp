set -x

# Create required directories
mkdir -p $CACHE
mkdir -p $R_LIBS_USER

# Download files into the cache directory
maybe_download () {
  curl $1 --output $CACHE/$2 --remote-time --time-cond $CACHE/$2
  chmod +x $CACHE/$2
}

maybe_download $GAMSURL $GAMSFNAME
maybe_download $CONDAURL $CONDAFNAME
