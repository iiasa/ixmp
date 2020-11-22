"""Tests for ixmp.reporting.quantity."""
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from xarray.testing import assert_equal as assert_xr_equal

from ixmp import Reporter, Scenario
from ixmp.reporting import Quantity, computations
from ixmp.reporting.attrseries import AttrSeries
from ixmp.reporting.sparsedataarray import SparseDataArray
from ixmp.testing import assert_qty_allclose, assert_qty_equal


@pytest.mark.usefixtures("parametrize_quantity_class")
class TestQuantity:
    """Tests of Quantity."""

    @pytest.fixture
    def a(self):
        da = xr.DataArray([0.8, 0.2], coords=[["oil", "water"]], dims=["p"])
        yield Quantity(da)

    @pytest.fixture(scope="class")
    def scen_with_big_data(self, test_mp, num_params=10):
        from itertools import zip_longest

        # test_mp.add_unit('kg')
        scen = Scenario(test_mp, "TestQuantity", "big data", version="new")

        # Dimensions and their lengths (Fibonacci numbers)
        N_dims = 6
        dims = "abcdefgh"[: N_dims + 1]
        sizes = [1, 5, 21, 21, 89, 377, 1597, 6765][: N_dims + 1]

        # commented: "377 / 73984365 elements = 0.00051% full"
        # from functools import reduce
        # from operator import mul
        # size = reduce(mul, sizes)
        # print('{} / {} elements = {:.5f}% full'
        #       .format(max(sizes), size, 100 * max(sizes) / size))

        # Names like f_0000 ... f_1596 along each dimension
        coords = []
        for d, N in zip(dims, sizes):
            coords.append([f"{d}_{i:04d}" for i in range(N)])
            # Add to Scenario
            scen.init_set(d)
            scen.add_set(d, coords[-1])

        def _make_values():
            """Make a DataFrame containing each label in *coords* ≥ 1 time."""
            values = list(zip_longest(*coords, np.random.rand(max(sizes))))
            result = pd.DataFrame(values, columns=list(dims) + ["value"]).ffill()
            result["unit"] = "kg"
            return result

        # Fill the Scenario with quantities named q_01 ... q_09
        names = []
        for i in range(num_params):
            name = f"q_{i:02d}"
            scen.init_par(name, list(dims))
            scen.add_par(name, _make_values())
            names.append(name)

        yield scen

    def test_assert(self, a):
        """Test assertions about Quantity.

        These are tests without `attr` property, in which case direct pd.Series
        and xr.DataArray comparisons are possible.
        """
        # Convert to pd.Series
        b = a.to_series()

        assert_qty_equal(a, b, check_type=False)
        assert_qty_equal(b, a, check_type=False)
        assert_qty_allclose(a, b, check_type=False)
        assert_qty_allclose(b, a, check_type=False)

        c = Quantity(a)

        assert_qty_equal(a, c, check_type=True)
        assert_qty_equal(c, a, check_type=True)
        assert_qty_allclose(a, c, check_type=True)
        assert_qty_allclose(c, a, check_type=True)

    def test_assert_with_attrs(self, a):
        """Test assertions about Quantity with attrs.

        Here direct pd.Series and xr.DataArray comparisons are *not* possible.
        """
        attrs = {"foo": "bar"}
        a.attrs = attrs

        b = Quantity(a)

        # make sure it has the correct property
        assert a.attrs == attrs
        assert b.attrs == attrs

        assert_qty_equal(a, b)
        assert_qty_equal(b, a)
        assert_qty_allclose(a, b)
        assert_qty_allclose(b, a)

        # check_attrs=False allows a successful equals assertion even when the
        # attrs are different
        a.attrs = {"bar": "foo"}
        assert_qty_equal(a, b, check_attrs=False)

    def test_size(self, scen_with_big_data):
        """Stress-test reporting of large, sparse quantities."""
        scen = scen_with_big_data

        # Create the reporter
        rep = Reporter.from_scenario(scen)

        # Add a task to compute the product, i.e. requires all the q_*
        keys = [rep.full_key(name) for name in scen.par_list()]
        rep.add("bigmem", tuple([computations.product] + keys))

        # One quantity fits in memory
        rep.get(keys[0])

        # All quantities can be multiplied without raising MemoryError
        result = rep.get("bigmem")

        # Result can be converted to pd.Series
        result.to_series()


class TestAttrSeries:
    """Tests of AttrSeries in particular."""

    @pytest.fixture
    def foo(self):
        idx = pd.MultiIndex.from_product([["a1", "a2"], ["b1", "b2"]], names=["a", "b"])
        yield AttrSeries([0, 1, 2, 3], index=idx)

    @pytest.fixture
    def bar(self):
        yield AttrSeries([0, 1], index=pd.Index(["a1", "a2"], name="a"))

    def test_rename(self, foo):
        assert foo.rename({"a": "c", "b": "d"}).dims == ("c", "d")

    def test_sel(self, bar):
        # Selecting 1 element from 1-D parameter still returns AttrSeries
        result = bar.sel(a="a2")
        assert isinstance(result, AttrSeries)
        assert result.size == 1
        assert result.dims == ("a",)
        assert result.iloc[0] == 1

    def test_squeeze(self, foo):
        assert foo.sel(a="a1").squeeze().dims == ("b",)
        assert foo.sel(a="a2", b="b1").squeeze().values == 2

    def test_sum(self, foo, bar):
        # AttrSeries can be summed across all dimensions
        result = foo.sum(dim=["a", "b"])
        assert isinstance(result, AttrSeries)  # returns an AttrSeries
        assert result.size == 1  # with one element
        assert result.item() == 6  # that has the correct value

        # Sum with wrong dim raises ValueError
        with pytest.raises(ValueError):
            bar.sum("b")

    def test_others(self, foo, bar):
        # Exercise other compatibility functions
        assert type(foo.to_frame()) is pd.DataFrame
        assert foo.drop("a").dims == ("b",)
        assert bar.dims == ("a",)

        with pytest.raises(NotImplementedError):
            bar.item("a2")
        with pytest.raises(ValueError):
            bar.item()


def test_sda_accessor():
    """Test conversion to sparse.COO-backed xr.DataArray."""
    x_series = pd.Series(
        data=[1.0, 2, 3, 4],
        index=pd.MultiIndex.from_product(
            [["a", "b"], ["c", "d"]], names=["foo", "bar"]
        ),
    )
    y_series = pd.Series(data=[5.0, 6], index=pd.Index(["e", "f"], name="baz"))

    x = SparseDataArray.from_series(x_series)
    y = SparseDataArray.from_series(y_series)

    x_dense = x._sda.dense_super
    y_dense = y._sda.dense_super
    assert not x_dense._sda.COO_data or x_dense._sda.nan_fill
    assert not y_dense._sda.COO_data or y_dense._sda.nan_fill

    # As of sparse 0.10, sparse `y` is automatically broadcast to `x_dense`
    # Previously, this raised ValueError.
    x_dense * y

    z1 = x_dense._sda.convert() * y

    z2 = x * y_dense._sda.convert()
    assert z1.dims == ("foo", "bar", "baz") == z2.dims
    assert_xr_equal(z1, z2)

    z3 = x._sda.convert() * y._sda.convert()
    assert_xr_equal(z1, z3)

    z4 = x._sda.convert() * y
    assert_xr_equal(z1, z4)

    z5 = SparseDataArray.from_series(x_series) * y
    assert_xr_equal(z1, z5)
