from pathlib import Path
import re
from subprocess import CalledProcessError, check_call, run
import sys

import pytest

from ixmp.testing import create_local_testdb


def r_installed():
    try:
        check_call(['R', '--version'])
        return True
    except CalledProcessError:
        return False


# Skip the entire file if R is not installed
pytestmark = pytest.mark.skipif(not r_installed(), reason='R not installed')


@pytest.fixture
def retixmp_path(request):
    """Path to the retixmp source."""
    yield Path(request.fspath).parent.parent / 'retixmp' / 'source'


def test_r_build(retixmp_path, tmp_env):
    """R package can be built."""
    cmd = ['R', 'CMD', 'build']
    check_call(cmd, cwd=retixmp_path, env=tmp_env)


def test_r_check(retixmp_path, tmp_env):
    """R CMD check succeeds on built package."""
    cmd = ['R', 'CMD', 'check'] + list(retixmp_path.glob('*.tar.gz'))
    check_call(cmd, cwd=retixmp_path, env=tmp_env)


def test_r_testthat(retixmp_path, tmp_env, test_data_path):
    """Tests succeed on R code without building the package."""
    tests_path = Path('tests', 'testthat')
    cmd = ['R', '--quiet', '-e', 'testthat::test_dir("{}")'.format(tests_path)]

    # Ensure reticulate uses the same Python as the pytest session
    tmp_env['RETICULATE_PYTHON'] = sys.executable
    tmp_env['IXMP_TEST_DATA_PATH'] = str(test_data_path)
    info = run(cmd, cwd=retixmp_path, stdout=subprocess.PIPE, env=tmp_env)

    # Number of testthat tests that failed
    failures = int(re.findall(r'Failed:\s*(\d*)', str(info.stdout))[0])

    if failures:
        # Pipe R output to stdout
        sys.stdout.write(info.stdout)
        pytest.fail('{} R tests'.format(failures))
