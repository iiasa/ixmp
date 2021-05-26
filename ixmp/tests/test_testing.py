import ixmp
from ixmp.testing import models
from ixmp.testing.resource import memory_usage


def test_resource_limit(resource_limit, test_mp):
    """Exercise :func:`memory_usage` and :func:`resource_limit`."""
    # TODO expand to cover other missed lines in those functions

    info0 = memory_usage("setup")

    s = ixmp.TimeSeries(test_mp, **models["h2g2"], version="new")

    info1 = memory_usage("1 TimeSeries")

    del s

    assert info0.python < info1.python
