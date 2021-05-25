import pytest

from ixmp.testing import populate_test_platform


@pytest.fixture(scope="module")
def mp(test_mp):
    """A :class:`.Platform` containing test data.

    This fixture is **module** -scoped, and is used in :mod:`.test_platform`,
    :mod:`.test_timeseries`, and :mod:`.test_scenario`. :mod:`.test_meta` overrides this
    with a **function** -scoped fixture; see there for more details.
    """
    populate_test_platform(test_mp)
    yield test_mp
