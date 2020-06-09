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
    choco install --no-progress graphviz
    ;;
esac

# Print version
dot -V
