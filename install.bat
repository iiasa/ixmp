rem Install ixmp and rixmp from source on Windows
rem
rem TODO if this script is to be kept/supported, describe it in the docs.

@echo off

python setup.py install

if %errorlevel% neq 0 GOTO InstallError

where /q r
IF ERRORLEVEL 1 (
  echo R not installed; skipping installation of rixmp.
) ELSE (
  cd rixmp
  R CMD build .
  R CMD INSTALL --html rixmp_0.1.3.9000.tar.gz
  cd ..
)

pytest

exit

@rem install error
:InstallError
echo =====================================================
echo  There was an error during the install process!
echo  Make sure that there is no instance of ixmp running
echo =====================================================
pause
exit
