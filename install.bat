rem Install ixmp and retixmp from source on Windows
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
  echo R not installed; skipping installation of retixmp.
) ELSE (
    rem TODO remove?
    rem rscript rixmp/build_rixmp.R [--verbose]

    cd retixmp
    R CMD build .
    R CMD INSTALL --html retixmp_0.1.3.9000.tar.gz
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
