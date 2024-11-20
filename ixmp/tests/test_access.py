import json

import pytest

import ixmp
from ixmp.testing import create_test_platform


@pytest.fixture
def mock(httpserver):
    """Mock server with responses for both tests."""
    from werkzeug import Request, Response

    httpserver.expect_request("/login", method="POST").respond_with_json(
        "security-token"
    )

    # Mock the behaviour of the ixmp_source (Java) access API
    # - Request data is valid JSON containing a list of dict.
    # - Response is a JSON list of bool of equal length.
    def handler(r: Request) -> Response:
        data = r.get_json()
        result = [
            (i["username"], i["entityType"], i["entityId"])
            == ("test_user", "MODEL", "test_model")
            for i in data
        ]
        return Response(json.dumps(result), content_type="application/json")

    # Use the same handler for all test requests against the /access/list URL
    httpserver.expect_request(
        "/access/list",
        method="POST",
        headers={"Authorization": "Bearer security-token"},  # JSON Web Token header
    ).respond_with_handler(handler)

    return httpserver


@pytest.fixture
def test_props(mock, request, tmp_path, test_data_path):
    return create_test_platform(
        tmp_path, test_data_path, "test_access", auth_url=mock.url_for("")
    )


M = ["test_model", "non_existing_model"]


@pytest.mark.parametrize(
    "user, models, exp",
    (
        ("test_user", "test_model", True),
        ("non_granted_user", "test_model", False),
        ("non_existing_user", "test_model", False),
        ("test_user", M, {"test_model": True, "non_existing_model": False}),
        ("non_granted_user", M, {"test_model": False, "non_existing_model": False}),
        ("non_existing_user", M, {"test_model": False, "non_existing_model": False}),
    ),
)
def test_check_access(test_props, user, models, exp):
    """:meth:`.check_access` correctly handles certain arguments and responses."""
    mp = ixmp.Platform(backend="jdbc", dbprops=test_props)
    assert exp == mp.check_access(user, models)
