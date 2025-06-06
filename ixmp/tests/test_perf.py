"""Performance tests."""

from functools import partial
from typing import TYPE_CHECKING, Any

import pytest

from ixmp import Scenario
from ixmp.testing import models
from ixmp.testing.data import add_random_model_data

if TYPE_CHECKING:
    from ixmp.core.platform import Platform


def add_par_setup(
    mp: "Platform", length: int
) -> tuple[tuple[Scenario, int], dict[str, Any]]:  # pragma: no cover
    return (Scenario(mp, **models["dantzig"], version="new"), length), dict()


def add_par(scen: Scenario, length: int) -> None:  # pragma: no cover
    with scen.transact():
        add_random_model_data(scen, length)


@pytest.mark.parametrize("length", [1e2, 1e3, 1e4, 1e6])
def test_add_par(
    benchmark: Any, test_mp: "Platform", length: int
) -> None:  # pragma: no cover
    """Test performance of :meth:`.add_par`."""
    benchmark.pedantic(
        add_par,
        setup=partial(add_par_setup, test_mp, length),
    )
