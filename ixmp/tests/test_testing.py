import ixmp
from ixmp.testing import models
from ixmp.testing.data import random_ts_data
from ixmp.testing.resource import memory_usage


def test_random_ts_data(N=100):
    df = random_ts_data(N)
    assert N == len(df)


def test_resource_limit(resource_limit, test_mp):
    """Exercise :func:`memory_usage` and :func:`resource_limit`."""
    # TODO expand to cover other missed lines in those functions

    memory_usage("setup")

    s = ixmp.TimeSeries(test_mp, **models["h2g2"], version="new")

    memory_usage("1 TimeSeries")

    del s

    # commented: this is nondeterministic, not always true
    # assert info0.python <= info1.python
