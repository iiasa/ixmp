# Modified from 'Exec' cmdlet in krlmlr/r-appveyor/scripts/appveyor-tool.ps1
#
# The sole difference is '. $Command' instead of '& $Command'; this ensures the
# script block is executed in the current scope, rather than a child scope.
Function Exec
{
    [CmdletBinding()]
    param (
        [Parameter(Position=0, Mandatory=1)]
        [scriptblock]$Command,
        [Parameter(Position=1, Mandatory=0)]
        [string]$ErrorMessage = "Execution of command failed.`n$Command"
    )
    $ErrorActionPreference = "Continue"
    . $Command 2>&1 | %{ "$_" }
    if ($LastExitCode -ne 0) {
        throw "Exec: $ErrorMessage`nExit code: $LastExitCode"
    }
}


# Halt instead of stalling on failure
$ErrorActionPreference = 'Stop'

# For debugging, use -Trace 1 or -Trace 2.
Set-PSDebug -Trace 0

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
gams 2>&1 | Write-Host

Write-Output '-----'

Exec { gams }

Write-Output '-----'

Start-Process gams -Wait


Progress "Update conda"

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
conda update --quiet --yes conda | Write-Host

Progress "Create 'testing' environment"
Exec { conda create -n testing python=$PYTHON_VERSION --yes }

Progress "Install dependencies"
Exec { conda install -n testing --channel conda-forge --quiet --yes `
      ixmp[tests] `
      codecov `
      "pytest>=3.9" `
      pytest-cov }
Exec { conda remove -n testing --force --yes ixmp }

Progress "Activate the environment"
activate.bat testing | Write-Host
where jupyter

Progress "Conda information"
Exec { conda info --all }

Progress "Install graphviz (for dask.visualize)"
choco install --no-progress graphviz

Progress "Set up r-appveyor"
Bootstrap

Progress "Install R packages needed for testing and the package itself"
Exec { Rscript .\ci\appveyor-install.R 1 }
