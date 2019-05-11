rem Install ixmp and retixmp from source on Windows
rem
rem TODO if this script is to be kept/supported, describe it in the docs.

rem TODO remove?
cd > .foo
set /p IXMP=<.foo
del .foo
echo %IXMP%

python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())" > .foo
set /p RPTH=<.foo
del .foo
echo %RPTH%

@echo off

python setup.py install

if %errorlevel% neq 0 GOTO InstallError

where /q r
IF ERRORLEVEL 1 (
    ECHO No valid installation of R found, skipped build and installation of R package.
) ELSE (
    rem TODO remove?
    rem rscript rixmp/build_rixmp.R [--verbose]

    cd retixmp/source
    R CMD build .
    R CMD INSTALL --html retixmp_0.2.0.9000.tar.gz
)

rem TODO remove?
setx IXMP_PATH "%IXMP%"
setx IXMP_R_PATH "%RPTH%/ixmp"

py.test tests/

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
