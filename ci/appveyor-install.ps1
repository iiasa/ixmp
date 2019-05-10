# Halt instead of stalling on failure
$ErrorActionPreference = 'Stop'

# Echo commands. For debugging, use -Trace 2
Set-PSDebug -Trace 1

# Download GAMS
Start-FileDownload 'https://d37drm4t2jghv5.cloudfront.net/distributions/25.1.1/windows/windows_x64_64.exe'

# Install GAMS
& '.\windows_x64_64.exe' '/SP- /NORESTART /DIR=.\gams /NOICONS'

$env:PATH = $(Get-Location).Path + '\gams;' + $env:PATH

# Update conda

# These correspond to folder naming of miniconda installs on appveyor
# See https://www.appveyor.com/docs/windows-images-software/#miniconda
if ( $env:PYTHON_VERSION -eq '2.7' ) {
  $MC_PYTHON_VERSION = ''
} else {
  $MC_PYTHON_VERSION = $env:PYTHON_VERSION.Replace('.', '')
}
if ( $env:PYTHON_ARCH -eq '64' ) { $ARCH_LABEL = '-x64' }

$CR = 'C:\Miniconda' + $MC_PYTHON_VERSION + $ARCH_LABEL
$env:CONDA_ROOT = $CR

$env:PATH = $CR + ';' + $CR + '\Scripts;' + $CR + '\Library\bin;' + $env:PATH

Write-Output $env:PATH

# Use the 'Exec' cmdlet from appveyor-tool.ps1 to handle output redirection
# and errors.
Exec { conda update --yes conda }

# TODO create a 'testing' env as on Travis?

Exec { conda install -c conda-forge --yes ixmp pytest coveralls pytest-cov }
Exec { conda remove --force --yes ixmp }

# Show information
Exec { conda info --all }


# Set up r-appveyor
Bootstrap

# Install R packages needed for testing
TravisTool { install_r devtools IRkernel }
Exec { Rscript -e "IRkernel::installspec()" }
