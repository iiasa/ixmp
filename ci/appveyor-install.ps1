# Halt instead of stalling on failure
$ErrorActionPreference = 'Stop'

# For debugging, use -Trace 1 or -Trace 2.
Set-PSDebug -Trace 0

# Download GAMS
Start-FileDownload 'https://d37drm4t2jghv5.cloudfront.net/distributions/25.1.1/windows/windows_x64_64.exe'

# Install GAMS
$GAMSArgs = '/SP- /NORESTART /DIR=' + $(Get-Location).Path + '\gams /NOICONS'
& '.\windows_x64_64.exe' $GAMSArgs

$env:PATH = $(Get-Location).Path + '\gams;' + $env:PATH

Write-Output $env:PATH

# Use the 'Exec' cmdlet from appveyor-tool.ps1 to handle output redirection
# and errors.
Exec { gams }

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

Exec { conda update --quiet --yes conda }

# TODO create a 'testing' env, as on Travis.
# TODO for PYTHON_VERSION = 2.7, this causes mkl and openjdk to be installed,
# each about 150 MB. Enable Appveyor caching or tweak conda configuration to
# speed up.
Exec { conda install --channel conda-forge  --quiet --yes `
       ixmp[tests] pytest coveralls pytest-cov }
Exec { conda remove --force --yes ixmp }

# Show information
Exec { conda info --all }


# Set up r-appveyor
Bootstrap

# Install R packages needed for testing and the package itself
Exec { Rscript .\ci\appveyor-install.R 1 }
