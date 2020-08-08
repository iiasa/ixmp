import pytest

import ixmp
from ixmp.testing import make_dantzig


@pytest.fixture(scope="function")
def mp():
    yield ixmp.Platform(backend="xarray")


def test_class(mp):
    assert mp._backend.__class__.__name__ == "XarrayBackend"


def test_unit(mp):
    mp.add_unit("kg", "kilogram")
    assert mp.units() == ["kg"]

    # Re-adding a unit logs a message but produces no exception
    mp.add_unit("kg", "oops")
    # The list of units remains the same
    assert mp.units() == ["kg"]


def test_ts(mp):
    ts = ixmp.TimeSeries(mp, "model name 1", "scenario name 1", version="new")
    assert ts.run_id() == 1

    s = ixmp.Scenario(mp, "model name 2", "scenario name 2", version="new")
    assert s.run_id() == 2

    # run_id is the same
    ts2 = ixmp.TimeSeries(mp, "model name 1", "scenario name 1")
    assert ts.run_id() == ts2.run_id()


def test_make_dantzig(mp):
    """The Dantzig model can be created."""
    make_dantzig(mp)
