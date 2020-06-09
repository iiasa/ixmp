#!/bin/sh
# Install cURL

case $CI_OS in
  windows*)
    choco install --no-progress curl
    ;;
esac

# Print version
curl --version
