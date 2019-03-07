import os
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
import subprocess

import ixmp
from ixmp.default_path_constants import CONFIG_PATH
from ixmp.testing import create_local_testdb
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--win_ci_skip", action="store_true", default=False,
        help="weird skips for windows ci"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--win_ci_skip"):
        skip_win_ci = pytest.mark.skip(reason="weird effects on windows ci")
        for item in items:
            if "skip_win_ci" in item.keywords:
                item.add_marker(skip_win_ci)


def pytest_report_header(config, startdir):
    return 'ixmp location: {}'.format(os.path.dirname(ixmp.__file__))


@pytest.fixture(scope='session')
def test_data_path(request):
    """Path to the directory containing test data."""
    return Path(__file__).parent / 'data'


@pytest.fixture
def tutorial_path(request):
    """Path to the directory containing tutorials."""
    return Path(__file__).parent / '..' / 'tutorial'


@pytest.fixture(scope="session")
def test_mp(tmp_path_factory, test_data_path):
    """An ixmp.Platform backed by a local database.

    *test_mp* is used across the entire test session, so the contents of the
    database may reflect other tests already run.
    """
    db_path = tmp_path_factory.mktemp('test_mp')
    test_props = create_local_testdb(db_path, test_data_path / 'testdb')

    # launch Platform and connect to testdb (reconnect if closed)
    mp = ixmp.Platform(test_props)
    mp.open_db()

    yield mp


@pytest.fixture()
def test_mp_use_db_config_path(tmp_path_factory, test_data_path):
    """An ixmp.Platform backed by a local database.

    Like *test_mp*, except 'ixmp-config' is used to write the database
    configuration path to the user's configuration.
    """
    assert not os.path.exists(CONFIG_PATH)

    db_path = tmp_path_factory.mktemp('test_mp_use_db_config_path')
    test_props = create_local_testdb(db_path, test_data_path / 'testdb')
    dirname = os.path.dirname(test_props)
    basename = os.path.basename(test_props)

    # configure
    cmd = 'ixmp-config --db_config_path {}'.format(dirname)
    subprocess.check_call(cmd.split())

    # launch Platform and connect to testdb (reconnect if closed)
    try:
        mp = ixmp.Platform(basename)
        mp.open_db()
    except Exception:
        os.remove(CONFIG_PATH)
        raise

    yield mp

    os.remove(CONFIG_PATH)


@pytest.fixture()
def test_mp_use_default_dbprops_file(tmp_path_factory, test_data_path):
    """An ixmp.Platform backed by a local database.

    Like *test_mp*, except 'ixmp-config' is used to write the location of the
    default database properties file to the user's configuration.
    """
    assert not os.path.exists(CONFIG_PATH)

    db_path = tmp_path_factory.mktemp('test_mp_use_default_dbprops_file')
    test_props = create_local_testdb(db_path, test_data_path / 'testdb')

    # configure
    cmd = 'ixmp-config --default_dbprops_file {}'.format(test_props)
    subprocess.check_call(cmd.split())

    # launch Platform and connect to testdb (reconnect if closed)
    try:
        mp = ixmp.Platform()
        mp.open_db()
    except Exception:
        os.remove(CONFIG_PATH)
        raise

    yield mp

    os.remove(CONFIG_PATH)


@pytest.fixture(scope="session")
def test_mp_props(tmp_path_factory, test_data_path):
    """Path to a database properties file referring to a test database."""
    db_path = tmp_path_factory.mktemp('test_mp_props')
    test_props = create_local_testdb(db_path, test_data_path / 'testdb')

    yield test_props
