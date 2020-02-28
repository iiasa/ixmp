from pathlib import Path
import re
import subprocess
from subprocess import run
import sys

import pytest


@pytest.fixture
def r_args(request, tmp_env, test_data_path, tmp_path_factory):
    """Arguments for subprocess calls to R."""
    # Path to the rixmp source
    rixmp_path = Path(request.fspath).parents[2] / 'rixmp'

    # Ensure reticulate uses the same Python as the pytest session
    tmp_env['RETICULATE_PYTHON'] = sys.executable

    # Show all lines on tests failure
    tmp_env['_R_CHECK_TESTS_NLINES_'] = '0'

    args = dict(cwd=rixmp_path, env=tmp_env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)

    arg = 'text' if sys.version_info[:2] >= (3, 7) else 'universal_newlines'
    args[arg] = True

    yield args


@pytest.mark.rixmp
def test_r_build_and_check(r_args):
    """R package can be built and R CMD check succeeds on the built package."""
    cmd = ['R', 'CMD', 'build', '.']
    subprocess.check_call(cmd, **r_args)

    cmd = ['R', 'CMD', 'check', '--no-examples']
    if 'APPVEYOR' in r_args['env']:
        # - Do not cross-build/-check e.g. i386 on x64 Appveyor workers
        # - Do not build manual (avoids overhead of LaTeX install)
        cmd.extend(['--no-multiarch', '--no-manual'])
    cmd.extend(map(str, r_args['cwd'].glob('*.tar.gz')))
    info = run(cmd, **r_args)

    try:
        info.check_returncode()
    except subprocess.CalledProcessError:
        # Copy the log to stdout
        sys.stdout.write(info.stdout)
        raise


@pytest.mark.rixmp
def test_r_testthat(r_args):
    """Tests succeed on R code without building the package."""
    # NB previously used file.path('tests', 'testthat'), which produces an
    # identical string, but caused errors on Windows
    cmd = ['Rscript', '-e', "testthat::test_dir('tests/testthat')"]

    info = run(cmd, **r_args)

    try:
        # Number of failing testthat tests
        failures = int(re.findall(r'Failed:\s*(\d*)', info.stdout)[0])

        if failures:
            # Pipe R output to stdout
            sys.stdout.write(info.stdout)
            pytest.fail('{} R tests'.format(failures))
    except IndexError:
        sys.stdout.write(info.stdout)
        raise
