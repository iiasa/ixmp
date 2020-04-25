"""Tests for ixmp.reporting.quantity."""
import pandas as pd
import pytest
import xarray as xr
from xarray.testing import assert_equal as assert_xr_equal

from ixmp import Reporter, Scenario
from ixmp.reporting import Quantity, computations
from ixmp.reporting.attrseries import AttrSeries
from ixmp.reporting.sparsedataarray import SparseDataArray
from ixmp.testing import assert_qty_allclose, assert_qty_equal


@pytest.mark.usefixtures('parametrize_quantity_class')
class TestQuantity:
    """Tests of Quantity."""
    @pytest.fixture
    def a(self):
        da = xr.DataArray([0.8, 0.2], coords=[['oil', 'water']], dims=['p'])
        yield Quantity(da)

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
        attrs = {'foo': 'bar'}
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
        a.attrs = {'bar': 'foo'}
        assert_qty_equal(a, b, check_attrs=False)


class TestAttrSeries:
    """Tests of AttrSeries in particular."""
    @pytest.fixture
    def foo(self):
        idx = pd.MultiIndex.from_product([['a1', 'a2'], ['b1', 'b2']],
                                         names=['a', 'b'])
        yield AttrSeries([0, 1, 2, 3], index=idx)

    def test_sum(self, foo):
        # AttrSeries can be summed across all dimensions
        result = foo.sum(dim=['a', 'b'])
        assert isinstance(result, AttrSeries)  # returns an AttrSeries
        assert len(result) == 1                # with one element
        assert result[0] == 6                  # that has the correct value

    def test_others(self, foo):
        # Exercise other compatibility functions
        assert isinstance(foo.as_xarray(), xr.DataArray)
        assert type(foo.to_frame()) is pd.DataFrame
        assert foo.drop('a').dims == ('b',)


def test_sda_accessor():
    """Test conversion to sparse.COO-backed xr.DataArray."""
    x_series = pd.Series(
        data=[1., 2, 3, 4],
        index=pd.MultiIndex.from_product([['a', 'b'], ['c', 'd']],
                                         names=['foo', 'bar']),
    )
    y_series = pd.Series(data=[5., 6], index=pd.Index(['e', 'f'], name='baz'))

    x = SparseDataArray.from_series(x_series)
    y = SparseDataArray.from_series(y_series)

    x_dense = x._sda.dense_super
    y_dense = y._sda.dense_super
    assert not x_dense._sda.COO_data or x_dense._sda.nan_fill
    assert not y_dense._sda.COO_data or y_dense._sda.nan_fill

    with pytest.raises(ValueError, match='make sure that the broadcast shape'):
        x_dense * y

    z1 = x_dense._sda.convert() * y

    z2 = x * y_dense._sda.convert()
    assert z1.dims == ('foo', 'bar', 'baz') == z2.dims
    assert_xr_equal(z1, z2)

    z3 = x._sda.convert() * y._sda.convert()
    assert_xr_equal(z1, z3)

    z4 = x._sda.convert() * y
    assert_xr_equal(z1, z4)

    # Doesn't work: can't align automatically
    with pytest.raises(ValueError, match='Please make sure that the broadcast '
                       'shape of just the sparse arrays is the same as the '
                       'broadcast shape of all the operands.'):
        z5 = SparseDataArray(x_series) * y
        assert_xr_equal(z1, z5)
