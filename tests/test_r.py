try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
import re
import subprocess
import sys

import pytest

from ixmp.testing import create_local_testdb


def r_installed():
    try:
        return subprocess.call(['R', '--version']) == 0
    except OSError:
        # FileNotFoundError (Python 3) or WindowsError (Python 2.7)
        return False


# Skip the entire file if R is not installed
pytestmark = pytest.mark.skipif(not r_installed(), reason='R not installed')


@pytest.fixture
def r_args(request, tmp_env, test_data_path):
    """Arguments for subprocess calls to R."""
    # Path to the retixmp source
    retixmp_path = Path(request.fspath).parent.parent / 'retixmp' / 'source'

    # Ensure reticulate uses the same Python as the pytest session
    tmp_env['RETICULATE_PYTHON'] = sys.executable
    tmp_env['IXMP_TEST_DATA_PATH'] = str(test_data_path)

    # str() here is for Python 2.7 compatibility on Windows
    args = dict(cwd=str(retixmp_path), env=tmp_env, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT)

    arg = 'text' if sys.version_info[:2] >= (3, 7) else 'universal_newlines'
    args[arg] = True

    yield args


def test_r_build_and_check(r_args):
    """R package can be built and R CMD check succeeds on the built package."""
    cmd = ['R', 'CMD', 'build', '.']
    subprocess.check_call(cmd, **r_args)

    cmd = ['R', 'CMD', 'check'] + list(r_args['cwd'].glob('*.tar.gz'))
    info = subprocess.run(cmd, **r_args)

    try:
        info.check_returncode()
    except subprocess.CalledProcessError:
        # Copy the log to stdout
        sys.stdout.write(info.stdout)
        raise


def test_r_testthat(r_args):
    """Tests succeed on R code without building the package."""
    # Python 2.7 compatibility
    try:
        from subprocess import run
    except ImportError:
        def run(*args, **kwargs):
            popen = subprocess.Popen(*args, **kwargs)
            popen.wait()
            # Convert stream to sequence
            popen.stdout = str(popen.stdout)
            return popen

    tests_path = Path('tests', 'testthat')
    cmd = ['R', '--quiet', '-e', 'testthat::test_dir("{}")'.format(tests_path)]

    info = run(cmd, **r_args)

    # Number of testthat tests that failed
    failures = int(re.findall(r'Failed:\s*(\d*)', info.stdout)[0])

    if failures:
        # Pipe R output to stdout
        sys.stdout.write(info.stdout)
        pytest.fail('{} R tests'.format(failures))
