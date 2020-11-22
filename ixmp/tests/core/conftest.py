import pytest

from ixmp.testing import populate_test_platform


@pytest.fixture(scope="class")
def mp(test_mp):
    """A Platform containing test data."""
    populate_test_platform(test_mp)
    yield test_mp
