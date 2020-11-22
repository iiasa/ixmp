import logging
import sys
from subprocess import Popen
from time import sleep

import pytest
from pretenders.client.http import HTTPMock
from pretenders.common.constants import FOREVER

import ixmp
from ixmp.testing import create_test_platform

log = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def server():
    proc = Popen(
        [
            sys.executable,
            "-m",
            "pretenders.server.server",
            "--host",
            "localhost",
            "--port",
            "8000",
        ]
    )
    log.info(f"Mock server started with pid {proc.pid}")

    # Wait for server to start up
    sleep(5)

    yield

    proc.terminate()
    log.info("Mock server terminated")


@pytest.fixture(scope="function")
def mock(server):
    # Create the mock server
    httpmock = HTTPMock("localhost", 8000)

    # Common responses for both tests
    httpmock.when("POST /login").reply(
        '"security-token"', headers={"Content-Type": "application/json"}, times=FOREVER
    )

    yield httpmock


def test_check_single_model_access(mock, tmp_path, test_data_path):
    mock.when(
        "POST /access/list",
        body='.+"test_user".+',
        headers={"Authorization": "Bearer security-token"},
    ).reply("[true]", headers={"Content-Type": "application/json"}, times=FOREVER)
    mock.when(
        "POST /access/list",
        body='.+"non_granted_user".+',
        headers={"Authorization": "Bearer security-token"},
    ).reply("[false]", headers={"Content-Type": "application/json"}, times=FOREVER)

    test_props = create_test_platform(
        tmp_path, test_data_path, "access", auth_url=mock.pretend_url
    )

    mp = ixmp.Platform(backend="jdbc", dbprops=test_props)

    granted = mp.check_access("test_user", "test_model")
    assert granted

    granted = mp.check_access("non_granted_user", "test_model")
    assert not granted

    granted = mp.check_access("non_existing_user", "test_model")
    assert not granted


def test_check_multi_model_access(mock, tmp_path, test_data_path):
    mock.when(
        "POST /access/list",
        body='.+"test_user".+',
        headers={"Authorization": "Bearer security-token"},
    ).reply(
        "[true, false]", headers={"Content-Type": "application/json"}, times=FOREVER
    )
    mock.when(
        "POST /access/list",
        body='.+"non_granted_user".+',
        headers={"Authorization": "Bearer security-token"},
    ).reply(
        "[false, false]", headers={"Content-Type": "application/json"}, times=FOREVER
    )

    test_props = create_test_platform(
        tmp_path, test_data_path, "access", auth_url=mock.pretend_url
    )

    mp = ixmp.Platform(backend="jdbc", dbprops=test_props)

    access = mp.check_access("test_user", ["test_model", "non_existing_model"])
    assert access["test_model"]
    assert not access["non_existing_model"]

    access = mp.check_access("non_granted_user", ["test_model", "non_existing_model"])
    assert not access["test_model"]
    assert not access["non_existing_model"]

    access = mp.check_access("non_existing_user", ["test_model", "non_existing_model"])
    assert not access["test_model"]
    assert not access["non_existing_model"]
