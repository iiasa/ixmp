# Download GAMS
Start-FileDownload 'https://d37drm4t2jghv5.cloudfront.net/distributions/25.1.1/windows/windows_x64_64.exe'

# Install the r-appveyor tool
$ErrorActionPreference = "Stop"

Start-FileDownload 'http://raw.githubusercontent.com/krlmlr/r-appveyor/master/scripts/appveyor-tool.ps1'

Import-Module appveyor-tool.ps1
