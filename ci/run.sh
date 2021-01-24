#!/bin/sh
# Run tests in the container & virtualenv created by Dockerfile

# Activate the virtualenv
. /opt/python3/bin/activate

# Install
pip install .[tests]

# Run tests
# NB add options like --trace-config, --trace-gc here to debug
pytest ixmp --verbose --color=yes
