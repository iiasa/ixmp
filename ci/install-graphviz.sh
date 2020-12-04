#!/bin/sh
# Install Graphviz.

case $CI_OS in
  linux* | ubuntu*)
    sudo apt install --quiet graphviz
    ;;
  macos*)
    brew install graphviz
    ;;
  windows*)
    # Temporary; see https://github.com/iiasa/ixmp/pull/387
    choco install --no-progress --version 2.38.0.20190211 graphviz
    ;;
esac

# Print version
dot -V
