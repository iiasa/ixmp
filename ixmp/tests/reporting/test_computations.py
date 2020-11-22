import logging

import numpy as np
import pint
import pytest
import xarray as xr
from pandas.testing import assert_series_equal

import ixmp
from ixmp.reporting import Quantity, Reporter, computations
from ixmp.reporting.testing import random_qty
from ixmp.testing import assert_logs, assert_qty_equal

from . import add_test_data

pytestmark = pytest.mark.usefixtures("parametrize_quantity_class")


@pytest.fixture(scope="function")
def data(test_mp, request):
    scen = ixmp.Scenario(test_mp, request.node.name, request.node.name, "new")
    data_objs = list(add_test_data(scen))
    rep = Reporter.from_scenario(scen)
    yield [scen, rep] + data_objs


@pytest.mark.parametrize(
    "operands, size",
    [
        (("a", "a"), 18),
        (("a", "x"), 36),
        (("x", "b"), 36),
        (("a", "b"), 36),
        (("a", "x", "b"), 36),
    ],
)
def test_add(data, operands, size):
    scen, rep, t, t_foo, t_bar, x = data

    y = scen.set("y").tolist()
    x = rep.get("x:t-y")
    a = Quantity(
        xr.DataArray(
            np.random.rand(len(t_foo), len(y)), coords=[t_foo, y], dims=["t", "y"]
        ),
        units=x.attrs["_unit"],
    )
    b = Quantity(
        xr.DataArray(
            np.random.rand(len(t_bar), len(y)), coords=[t_bar, y], dims=["t", "y"]
        ),
        units=x.attrs["_unit"],
    )

    rep.add("a:t-y", a)
    rep.add("b:t-y", b)

    key = rep.add(
        "result", tuple([computations.add] + [f"{name}:t-y" for name in operands])
    )

    result = rep.get(key)
    assert size == result.size, result.to_series()


def test_apply_units(data, caplog):
    # Unpack
    *_, x = data

    registry = pint.get_application_registry()

    # Brute-force replacement with incompatible units
    with assert_logs(caplog, "Replace 'kilogram' with incompatible 'liter'"):
        result = computations.apply_units(x, "litres")
    assert result.attrs["_unit"] == registry.Unit("litre")
    # No change in values
    assert_series_equal(result.to_series(), x.to_series())

    # Compatible units: magnitudes are also converted
    with assert_logs(
        caplog, "Convert 'kilogram' to 'metric_ton'", at_level=logging.DEBUG
    ):
        result = computations.apply_units(x, "tonne")
    assert result.attrs["_unit"] == registry.Unit("tonne")
    assert_series_equal(result.to_series(), x.to_series() * 0.001)

    # Remove unit
    x.attrs["_unit"] = registry.Unit("dimensionless")

    caplog.clear()
    result = computations.apply_units(x, "kg")
    # Nothing logged when _unit attr is missing
    assert len(caplog.messages) == 0
    assert result.attrs["_unit"] == registry.Unit("kg")
    assert_series_equal(result.to_series(), x.to_series())


@pytest.mark.xfail(reason="Outer join of non-intersecting dimensions (AttrSeries only)")
def test_product0():
    A = Quantity(xr.DataArray([1, 2], coords=[["a0", "a1"]], dims=["a"]))
    B = Quantity(xr.DataArray([3, 4], coords=[["b0", "b1"]], dims=["b"]))
    exp = Quantity(
        xr.DataArray(
            [[3, 4], [6, 8]],
            coords=[["a0", "a1"], ["b0", "b1"]],
            dims=["a", "b"],
        ),
        units="1",
    )

    assert_qty_equal(exp, computations.product(A, B))
    computations.product(exp, B)


def test_product1():
    """Product of quantities with overlapping dimensions."""
    A = random_qty(dict(a=2, b=2, c=2, d=2))
    B = random_qty(dict(b=2, c=2, d=2, e=2, f=2))

    assert computations.product(A, B).size == 2 ** 6


def test_select(data):
    # Unpack
    *_, t_foo, t_bar, x = data

    x = Quantity(x)
    assert x.size == 6 * 6

    # Selection with inverse=False
    indexers = {"t": t_foo[0:1] + t_bar[0:1]}
    result_0 = computations.select(x, indexers=indexers)
    assert result_0.size == 2 * 6

    # Single indexer along one dimension results in 1D data
    indexers["y"] = "2010"
    result_1 = computations.select(x, indexers=indexers)
    assert result_1.size == 2 * 1

    # Selection with inverse=True
    result_2 = computations.select(x, indexers=indexers, inverse=True)
    assert result_2.size == 4 * 5
