#!/bin/sh

. /opt/python3/bin/activate

pip install --editable .[tests,tutorial]

py.test --trace-config ixmp
