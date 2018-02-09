
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
    rscript rixmp/build_rixmp.R [--verbose]
)

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