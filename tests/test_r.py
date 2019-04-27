from pathlib import Path
import re
import subprocess
import sys

import pytest

from ixmp.testing import create_local_testdb


def r_installed():
    try:
        subprocess.check_call(['R', '--version'])
        subprocess.check_call(['R', 'library(optparse)'])
        return True
    except subprocess.CalledProcessError:
        return False


# Skip the entire file if R is not installed
pytestmark = pytest.mark.skipif(not r_installed(), reason='R not installed')


@pytest.fixture
def retixmp_path(request):
    """Path to the retixmp source."""
    yield Path(request.fspath).parent.parent / 'retixmp' / 'source'


def test_run(request):
    test_props = create_local_testdb()
    testf = request.fspath / 'r_tests.r'
    cmd = ['Rscript', testf, '--props={}'.format(test_props)]
    subprocess.check_call(cmd)


def test_r_build(retixmp_path, tmp_env):
    """R package can be built."""
    cmd = ['R', 'CMD', 'build']
    subprocess.check_call(cmd, cwd=retixmp_path, env=tmp_env)


def test_r_check(retixmp_path, tmp_env):
    """R CMD check succeeds on built package."""
    cmd = ['R', 'CMD', 'check'] + list(retixmp_path.glob('*.tar.gz'))
    subprocess.check_call(cmd, cwd=retixmp_path, env=tmp_env)


def test_r_testthat(retixmp_path, tmp_env):
    """Tests succeed on R code without building the package."""
    tests_path = Path('tests', 'testthat')
    cmd = ['R', '--quiet', '-e', 'testthat::test_dir("{}")'.format(tests_path)]

    # Ensure reticulate uses the same Python as the pytest session
    tmp_env['RETICULATE_PYTHON'] = sys.executable
    info = subprocess.run(cmd, cwd=retixmp_path, stdout=subprocess.PIPE,
                          env=tmp_env)

    # Number of testthat tests that failed
    failures = int(re.findall(r'Failed:\s*(\d*)', str(info.stdout))[0])

    if failures:
        # Pipe R output to stdout
        sys.stdout.write(info.stdout)
        pytest.fail('{} R tests'.format(failures))
