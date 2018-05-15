#!/usr/bin/env bash

PYTHON_VERSION=${1:-3}
docker build -t ixmp/ixmp:latest .
[ $PYTHON_VERSION == "2" ] && docker run -it --rm ixmp/ixmp:latest python2 -m pytest /ixmp/tests
[ $PYTHON_VERSION == "3" ] && docker run -it --rm ixmp/ixmp:latest python3 -m pytest /ixmp/tests
