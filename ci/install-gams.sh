#!/bin/sh
# Install GAMS.
#
# The environment variables CI_OS and GAMS_VERSION must be set.

BASE=$PWD/gams
mkdir -p $BASE

# GAMS source URL fragment, and path fragment for extracted files
case $CI_OS in
  linux* | ubuntu*)
    GAMS_OS=linux
    FRAGMENT=${GAMS_OS}_x64_64_sfx
    ;;
  macos*)
    GAMS_OS=macosx
    FRAGMENT=osx_x64_64_sfx
    ;;
  windows*)
    GAMS_OS=windows
    FRAGMENT=${GAMS_OS}_x64_64
    ;;
esac

# Retrieve
BASE_URL=https://d37drm4t2jghv5.cloudfront.net/distributions
URL=$BASE_URL/$GAMS_VERSION/$GAMS_OS/$FRAGMENT.exe

if [ -x $BASE/gams.exe ]; then
  # Don't retrieve if the remote file is older than the cached one
  TIME_CONDITION=--remote-time --time-cond $BASE/gams.exe
fi

curl --silent $URL --output $BASE/gams.exe $TIME_CONDITION

# TODO confirm checksum

# Path fragment for extraction or install
DEST=gams$(echo $GAMS_VERSION | cut -d. -f1-2)_$FRAGMENT

if [ $GAMS_OS = "windows" ]; then
  cat << EOF >install-gams.ps1

# Windows-format equivalent of BASE
\$BASE = "\$PWD\gams"

# Install to the same directory as Linux/macOS unzip
Start-Process "\$BASE\gams.exe" "/SP-", "/SILENT", "/DIR=\$BASE\\$DEST", \`
  "/NORESTART" -Wait

EOF
  cat install-gams.ps1
  pwsh install-gams.ps1
else
  # Extract files
  unzip -q -d $BASE $BASE/gams.exe
fi

# Update PATH
export PATH=$BASE/$DEST:$PATH

# For GitHub Actions
echo "$BASE/$DEST" >> $GITHUB_PATH

# Show location
which gams
