#!/bin/sh
# Install Graphviz.

case $CI_OS in
  linux|ubuntu-latest)
    sudo apt install --quiet graphviz
    ;;
  macosx|macos-latest)
    brew install graphviz
    ;;
  windows|windows-latest)
    choco install --no-progress graphviz
    ;;
esac

# Print version
dot -V
