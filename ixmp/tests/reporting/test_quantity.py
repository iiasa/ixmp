"""Tests for ixmp.reporting.quantity."""
import pandas as pd
import pytest
import xarray as xr
from xarray.testing import assert_equal as assert_xr_equal

from ixmp.reporting.quantity import AttrSeries, Quantity, as_sparse_xarray
from ixmp.testing import assert_qty_allclose, assert_qty_equal


class TestQuantity:
    """Tests of Quantity.

    NB. these tests should pass whether Quantity is set to AttrSeries or
    xr.DataArray in ixmp.reporting.utils. As written, they only test the
    current form of Quantity. @gidden tested both by hand-swapping the Quantity
    class and running tests as of commit df1ec6f of PR #147.
    """
    @pytest.fixture()
    def a(self):
        yield xr.DataArray([0.8, 0.2], coords=[['oil', 'water']], dims=['p'])

    def test_assert(self, a):
        """Test assertions about Quantity.

        These are tests without `attr` property, in which case direct pd.Series
        and xr.DataArray comparisons are possible.
        """
        # Convert to pd.Series
        b = a.to_series()

        assert_qty_equal(a, b)
        assert_qty_equal(b, a)
        assert_qty_allclose(a, b)
        assert_qty_allclose(b, a)

        c = Quantity(a)

        assert_qty_equal(a, c)
        assert_qty_equal(c, a)
        assert_qty_allclose(a, c)
        assert_qty_allclose(c, a)

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


@pytest.mark.skip(reason="Pending #317")
def test_as_sparse_xarray():
    """Test conversion to sparse.COO-backed xr.DataArray."""
    x_series = pd.Series(
        data=[1., 2, 3, 4],
        index=pd.MultiIndex.from_product([['a', 'b'], ['c', 'd']],
                                         names=['foo', 'bar']),
    )
    y_series = pd.Series(data=[5., 6], index=pd.Index(['e', 'f'], name='baz'))

    x = xr.DataArray.from_series(x_series, sparse=True)
    y = xr.DataArray.from_series(y_series, sparse=True)

    x_dense = xr.DataArray.from_series(x_series)
    y_dense = xr.DataArray.from_series(y_series)

    with pytest.raises(ValueError, match='make sure that the broadcast shape'):
        x_dense * y

    z1 = as_sparse_xarray(x_dense) * y
    z2 = x * as_sparse_xarray(y_dense)
    assert z1.dims == ('foo', 'bar', 'baz')
    assert_xr_equal(z1, z2)

    z3 = as_sparse_xarray(x) * as_sparse_xarray(y)
    assert_xr_equal(z1, z3)

    z4 = as_sparse_xarray(x) * y
    assert_xr_equal(z1, z4)

    z5 = as_sparse_xarray(x_series) * y
    assert_xr_equal(z1, z5)
