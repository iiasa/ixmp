rem Install ixmp and rixmp from source on Windows
rem
rem TODO if this script is to be kept/supported, describe it in the docs.

rem TODO remove?
rem cd > .foo
rem set /p IXMP=<.foo
rem del .foo
rem echo %IXMP%
rem
rem python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())" > .foo
rem set /p RPTH=<.foo
rem del .foo
rem echo %RPTH%

@echo off

python setup.py install

if %errorlevel% neq 0 GOTO InstallError

where /q r
IF ERRORLEVEL 1 (
  echo R not installed; skipping installation of rixmp.
) ELSE (
    rem TODO remove?
    rem rscript rixmp.legacy/build_rixmp.legacy.R [--verbose]

    cd rixmp
    R CMD build .
    R CMD INSTALL --html rixmp_0.1.3.9000.tar.gz
    cd ..
)

rem TODO remove?
rem setx IXMP_PATH "%IXMP%"
rem setx IXMP_R_PATH "%RPTH%/ixmp"

pytest

pause
exit

@rem install error
:InstallError
echo =====================================================
echo  There was an error during the install process!
echo  Make sure that there is no instance of ixmp running
echo =====================================================
pause
exit
