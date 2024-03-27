"""Performance tests."""

from functools import partial

import pytest

from ixmp import Scenario
from ixmp.testing import models
from ixmp.testing.data import add_random_model_data


def add_par_setup(mp, length):  # pragma: no cover
    return (Scenario(mp, **models["dantzig"], version="new"), length), dict()


def add_par(scen, length):  # pragma: no cover
    with scen.transact():
        add_random_model_data(scen, length)


@pytest.mark.parametrize("length", [1e2, 1e3, 1e4, 1e6])
def test_add_par(benchmark, test_mp, length):  # pragma: no cover
    """Test performance of :meth:`.add_par`."""
    benchmark.pedantic(
        add_par,
        setup=partial(add_par_setup, test_mp, length),
    )
