"""Tests for ixmp.reporting.utils."""
import pandas as pd
import pytest
import xarray as xr

from ixmp.reporting import Key
from ixmp.reporting.quantity import AttrSeries, Quantity, as_quantity
from ixmp.testing import assert_qty_allclose, assert_qty_equal


def test_reporting_key():
    k1 = Key('foo', ['a', 'b', 'c'])
    k2 = Key('bar', ['d', 'c', 'b'])

    # String
    assert str(k1) == 'foo:a-b-c'

    # Representation
    assert repr(k1) == '<foo:a-b-c>'

    # Key hashes the same as its string representation
    assert hash(k1) == hash('foo:a-b-c')

    # Key compares equal to its string representation
    assert k1 == 'foo:a-b-c'

    # product:
    assert Key.product('baz', k1, k2) == Key('baz', ['a', 'b', 'c', 'd'])

    # iter_sums: Number of partial sums for a 3-dimensional quantity
    assert sum(1 for a in k1.iter_sums()) == 7

    # Key with name and tag but no dimensions
    assert Key('foo', tag='baz') == 'foo::baz'


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


@pytest.mark.skipif(Quantity is AttrSeries, reason='For xr.DataArray only')
def test_as_quantity():
    """Test conversion to sparse.COO-backed xr.DataArray in as_quantity()."""
    x_series = pd.Series(
        data=[1, 2, 3, 4],
        index=pd.MultiIndex.from_product([['a', 'b'], ['c', 'd']],
                                         names=['foo', 'bar']),
    )
    y_series = pd.Series(data=[5, 6], index=pd.Index(['e', 'f'], name='baz'))

    x = xr.DataArray.from_series(x_series, sparse=True)
    y = xr.DataArray.from_series(y_series, sparse=True)

    x_dense = xr.DataArray.from_series(x_series)
    y_dense = xr.DataArray.from_series(y_series)

    with pytest.raises(ValueError, match='make sure that the broadcast shape'):
        x_dense * y

    z1 = as_quantity(x_dense) * y
    z2 = x * as_quantity(y_dense)
    assert z1.dims == ('foo', 'bar', 'baz')
    assert_qty_equal(z1, z2)

    z3 = as_quantity(x) * as_quantity(y)
    assert_qty_equal(z1, z3)

    z4 = as_quantity(x) * y
    assert_qty_equal(z1, z4)

    z5 = as_quantity(x_series) * y
    assert_qty_equal(z1, z5)
