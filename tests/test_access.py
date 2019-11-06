from subprocess import Popen
import sys
from time import sleep

from pretenders.client.http import HTTPMock
from pretenders.common.constants import FOREVER
import pytest

from pathlib import Path
import ixmp
import shutil


@pytest.fixture(scope='function')
def mock():
    proc = Popen(
        [sys.executable, '-m',
         'pretenders.server.server',
         '--host', 'localhost',
         '--port', '8000'])
    print('Mock server started with pid {}'.format(proc.pid))

    # Wait for server to start up
    sleep(1)

    yield HTTPMock('localhost', 8000)

    proc.terminate()
    print('Mock server terminated')


def create_local_testdb(db_path, data_path, db='ixmptest',
                        auth_url='http://localhost'):
    """Create a local database for testing in the directory *db_path*.

    Returns the path to a database properties file in the directory. Contents
    are copied from *data_path*.
    """
    # Copy test database
    dst = Path(db_path) / 'testdb'
    # str() here is for py2 compatibility
    shutil.copytree(str(data_path), str(dst))

    # Create properties file
    props = (Path(data_path) / 'test_auth.properties_template').read_text()
    test_props = dst / 'test.properties'
    test_props.write_text(props.format(
        here=str(dst).replace("\\", "/"),
        db=db,
        auth_url=auth_url
    ))

    return test_props


def test_check_single_model_access(mock, tmpdir, test_data_path):
    mock.when(
        'POST /login'
    ).reply(
        '"security-token"',
        headers={'Content-Type': 'application/json'},
        times=FOREVER)
    mock.when(
        'POST /access/list',
        body=".+\"test_user\".+",
        headers={'Authorization': 'Bearer security-token'}
    ).reply('[true]',
            headers={'Content-Type': 'application/json'},
            times=FOREVER)
    mock.when(
        'POST /access/list',
        body=".+\"non_granted_user\".+",
        headers={'Authorization': 'Bearer security-token'}
    ).reply(
        '[false]',
        headers={'Content-Type': 'application/json'},
        times=FOREVER)
    test_props = create_local_testdb(db_path=tmpdir,
                                     data_path=test_data_path / 'testdb',
                                     auth_url=mock.pretend_url)

    mp = ixmp.Platform(dbprops=test_props)
    mp.set_log_level('DEBUG')

    granted = mp.check_access('test_user', 'test_model')
    assert granted

    granted = mp.check_access('non_granted_user', 'test_model')
    assert not granted

    granted = mp.check_access('non_existing_user', 'test_model')
    assert not granted


def test_check_multi_model_access(mock, tmpdir, test_data_path):
    mock.when(
        'POST /login'
    ).reply(
        '"security-token"',
        headers={'Content-Type': 'application/json'},
        times=FOREVER)
    mock.when(
        'POST /access/list',
        body=".+\"test_user\".+",
        headers={'Authorization': 'Bearer security-token'}
    ).reply('[true, false]',
            headers={'Content-Type': 'application/json'},
            times=FOREVER)
    mock.when(
        'POST /access/list',
        body=".+\"non_granted_user\".+",
        headers={'Authorization': 'Bearer security-token'}
    ).reply(
        '[false, false]',
        headers={'Content-Type': 'application/json'},
        times=FOREVER)
    test_props = create_local_testdb(db_path=tmpdir,
                                     data_path=test_data_path / 'testdb',
                                     auth_url=mock.pretend_url)

    mp = ixmp.Platform(dbprops=test_props)
    mp.set_log_level('DEBUG')

    access = mp.check_access('test_user', ['test_model', 'non_existing_model'])
    assert access['test_model']
    assert not access['non_existing_model']

    access = mp.check_access('non_granted_user',
                             ['test_model', 'non_existing_model'])
    assert not access['test_model']
    assert not access['non_existing_model']

    access = mp.check_access('non_existing_user',
                             ['test_model', 'non_existing_model'])
    assert not access['test_model']
    assert not access['non_existing_model']
