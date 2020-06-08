#!/bin/sh
# Install cURL

case $CI_OS in
  windows|windows-latest)
    choco install --no-progress curl
    ;;
esac

# Print version
curl --version
