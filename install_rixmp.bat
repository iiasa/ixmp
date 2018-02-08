where /q r
IF ERRORLEVEL 1 (
    ECHO Error: R is missing. Ensure it is installed and placed in your PATH to build the r package.
) ELSE (
    rscript rixmp/build_rixmp.R [--verbose]
)

pause
exit