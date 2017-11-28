import os
import subprocess
import pytest

from testing_utils import create_local_testdb, here


def r_installed():
    try:
        subprocess.check_call(['R', '--version'])
        subprocess.check_call(['R', 'library(optparse)'])
        return True
    except:
        return False


@pytest.mark.skipif(not r_installed(), reason='requires R to be installed')
def test_run():
    test_props = create_local_testdb()
    testf = os.path.join(here, 'r_tests.r')
    cmd = ['Rscript', testf, '--props={}'.format(test_props)]
    subprocess.check_call(cmd)
