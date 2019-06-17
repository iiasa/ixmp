import os
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
import re
import subprocess
try:
    from subprocess import run
except ImportError:
    # Python 2.7 compatibility
    def run(*args, **kwargs):
        popen = subprocess.Popen(*args, **kwargs)
        stdout, _ = popen.communicate()
        # Convert stream to sequence
        popen.stdout = str(stdout)
        return popen
import sys

import pytest


def r_installed():
    try:
        return subprocess.call(['R', '--version']) == 0
    except OSError:
        # FileNotFoundError (Python 3) or WindowsError (Python 2.7)
        return False


pytestmark = [
    pytest.mark.skipif(not r_installed(), reason='R not installed'),
    pytest.mark.skipif(os.environ.get('APPVEYOR', '') == 'True' and
                       sys.version_info[0] == 2,
                       reason='Timeout on Appveyor / Python 2.7'),
    ]


@pytest.fixture
def r_args(request, tmp_env, test_data_path, tmp_path_factory):
    """Arguments for subprocess calls to R."""
    # Path to the rixmp source
    rixmp_path = Path(request.fspath).parent.parent / 'rixmp'

    # Ensure reticulate uses the same Python as the pytest session
    tmp_env['RETICULATE_PYTHON'] = sys.executable

    # Path to the files in tests/data
    tmp_env['IXMP_TEST_DATA_PATH'] = str(test_data_path)

    # Path to a directory for temporary databases
    tmp_env['IXMP_TEST_TMP_PATH'] = str(tmp_path_factory.mktemp('test_mp'))

    # Show all lines on tests failure
    tmp_env['_R_CHECK_TESTS_NLINES_'] = '0'

    # str() here is for Python 2.7 compatibility on Windows
    args = dict(cwd=str(rixmp_path), env=tmp_env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)

    arg = 'text' if sys.version_info[:2] >= (3, 7) else 'universal_newlines'
    args[arg] = True

    yield args


def test_r_build_and_check(r_args):
    """R package can be built and R CMD check succeeds on the built package."""
    cmd = ['R', 'CMD', 'build', '.']
    subprocess.check_call(cmd, **r_args)

    cmd = ['R', 'CMD', 'check', '--no-examples']
    if 'APPVEYOR' in r_args['env']:
        # - Do not cross-build/-check e.g. i386 on x64 Appveyor workers
        # - Do not build manual (avoids overhead of LaTeX install)
        cmd.extend(['--no-multiarch', '--no-manual'])
    # Path() here is required because of str() in r_args for Python 2.7 compat
    cmd.extend(map(str, Path(r_args['cwd']).glob('*.tar.gz')))
    info = run(cmd, **r_args)

    try:
        info.check_returncode()
    except subprocess.CalledProcessError:
        # Copy the log to stdout
        sys.stdout.write(info.stdout)
        raise
    except AttributeError:
        # Python 2.7
        if info.returncode != 0:
            sys.stdout.write(info.stdout)
            raise


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
