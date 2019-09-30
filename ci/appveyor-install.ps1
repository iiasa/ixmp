# Halt instead of stalling on failure
$ErrorActionPreference = 'Stop'

# For debugging, use -Trace 1 or -Trace 2.
Set-PSDebug -Trace 0

# Download GAMS
$GAMSInstaller = '..\windows_x64_64.exe'
Start-FileDownload 'https://d37drm4t2jghv5.cloudfront.net/distributions/25.1.1/windows/windows_x64_64.exe' -FileName $GAMSInstaller

# Install GAMS
$GAMSPath = 'C:\GAMS'
$GAMSArgs = '/SP- /SILENT /DIR=' + $GAMSPath + ' /NORESTART'
Start-Process $GAMSInstaller $GAMSArgs -Wait

# Add to PATH
$env:PATH = $GAMSPath + ';' + $env:PATH

# Show information
gams | Out-Default

Write-Output '-----'

Exec { gams }

Write-Output '-----'

Start-Process gams -Wait


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

# Use the 'Exec' cmdlet from appveyor-tool.ps1 to handle output redirection
# and errors.
Exec { conda update -n testing --quiet --yes conda }

# Create named environment
Exec { conda create -n testing python=$PYTHON_VERSION --yes}


# Install dependencies
Exec { conda install -n testing --channel conda-forge  --quiet --yes `
       ixmp[tests] codecov "pytest>=3.9" pytest-cov }
Exec { conda remove -n testing --force --yes ixmp }


# Activate the environment. This is not wrapped in Exec { } because that
# cmdlet operates in a sub-process and doesn't modify the current shell.
activate testing

# Show information
Exec { conda info --all }

# Install graphviz (for dask.visualize)
Exec { choco install --no-progress graphviz }

# Set up r-appveyor
Bootstrap

# Install R packages needed for testing and the package itself
Exec { Rscript .\ci\appveyor-install.R 1 }
