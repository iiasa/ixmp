import pytest
import os
import subprocess

import ixmp

from ixmp.default_path_constants import CONFIG_PATH

from testing_utils import create_local_testdb


@pytest.fixture(scope="session")
def test_mp():
    test_props = create_local_testdb()

    # start jvm
    ixmp.start_jvm()

    # launch Platform and connect to testdb (reconnect if closed)
    mp = ixmp.Platform(test_props)
    mp.open_db()

    yield mp


@pytest.fixture()
def test_mp_use_db_config_path():
    assert not os.path.exists(CONFIG_PATH)

    test_props = create_local_testdb()
    dirname = os.path.dirname(test_props)
    basename = os.path.basename(test_props)

    # configure
    cmd = 'ixmp-config --db_config_path {}'.format(dirname)
    subprocess.check_call(cmd.split())

    # start jvm
    ixmp.start_jvm()

    # launch Platform and connect to testdb (reconnect if closed)
    try:
        mp = ixmp.Platform(basename)
        mp.open_db()
    except:
        os.remove(CONFIG_PATH)
        raise

    yield mp

    os.remove(CONFIG_PATH)


@pytest.fixture()
def test_mp_use_default_dbprops_file():
    assert not os.path.exists(CONFIG_PATH)

    test_props = create_local_testdb()

    # configure
    cmd = 'ixmp-config --default_dbprops_file {}'.format(test_props)
    subprocess.check_call(cmd.split())

    # start jvm
    ixmp.start_jvm()

    # launch Platform and connect to testdb (reconnect if closed)
    try:
        mp = ixmp.Platform()
        mp.open_db()
    except:
        os.remove(CONFIG_PATH)
        raise

    yield mp

    os.remove(CONFIG_PATH)


@pytest.fixture(scope="session")
def test_mp_props():
    test_props = create_local_testdb()

    # start jvm
    ixmp.start_jvm()

    yield test_props
