# Halt instead of stalling on failure
$ErrorActionPreference = 'Stop'

# For debugging, use -Trace 1 or -Trace 2.
Set-PSDebug -Trace 1

# The 'Progress' cmdlet is also from appveyor-tool.ps1
Progress "Download GAMS"
$GAMSInstaller = '..\windows_x64_64.exe'
Start-FileDownload 'https://d37drm4t2jghv5.cloudfront.net/distributions/25.1.1/windows/windows_x64_64.exe' -FileName $GAMSInstaller

Progress "Install GAMS"
$GAMSPath = 'C:\GAMS'
$GAMSArgs = '/SP- /SILENT /DIR=' + $GAMSPath + ' /NORESTART'
Start-Process $GAMSInstaller $GAMSArgs -Wait

# Add to PATH
$env:PATH = $GAMSPath + ';' + $env:PATH

# Show information
Exec { gams }


Progress 'Set conda version/path'
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
$env:RETICULATE_PYTHON = $CR + '\python.exe'

Progress 'Update conda'
Exec { conda update --quiet --yes conda }

Progress 'Install dependencies'
Exec { conda install --channel conda-forge --quiet --yes `
      ixmp[tests] `
      codecov `
      "pytest>=3.9" `
      pytest-cov }
Exec { conda remove --force --yes ixmp }

Progress 'Conda information'
conda info --all

Progress 'Install graphviz (for dask.visualize)'
choco install --no-progress graphviz

Bootstrap

Progress 'Install R packages needed for testing'
Exec { Rscript .\ci\appveyor-install.R 1 }
