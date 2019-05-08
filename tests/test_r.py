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
    return subprocess.call(['R', '--version']) == 0


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

    yield dict(cwd=retixmp_path, env=tmp_env, stdout=subprocess.PIPE)


def test_r_build_and_check(r_args):
    """R package can be built and R CMD check succeeds on the built package."""
    cmd = ['R', 'CMD', 'build']
    subprocess.check_call(cmd, **r_args)

    cmd = ['R', 'CMD', 'check'] + list(r_args['cwd'].glob('*.tar.gz'))
    subprocess.check_call(cmd, **r_args)


def test_r_testthat(r_args):
    """Tests succeed on R code without building the package."""
    tests_path = Path('tests', 'testthat')
    cmd = ['R', '--quiet', '-e', 'testthat::test_dir("{}")'.format(tests_path)]

    info = subprocess.run(cmd, **r_args)

    # Number of testthat tests that failed
    failures = int(re.findall(r'Failed:\s*(\d*)', str(info.stdout))[0])

    if failures:
        # Pipe R output to stdout
        sys.stdout.write(info.stdout)
        pytest.fail('{} R tests'.format(failures))
