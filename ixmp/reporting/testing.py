from typing import Dict

import numpy as np
import xarray as xr
from pandas.testing import assert_series_equal

from .quantity import Quantity


def assert_qty_equal(a, b, check_type=True, check_attrs=True, **kwargs):
    """Assert that Quantity objects *a* and *b* are equal.

    When Quantity is AttrSeries, *a* and *b* are first passed through
    :meth:`as_quantity`.
    """
    if not check_type:
        a = Quantity(a)
        b = Quantity(b)

    if Quantity.CLASS == "AttrSeries":
        try:
            a = a.sort_index()
            b = b.sort_index()
        except TypeError:
            pass
        assert_series_equal(a, b, check_dtype=False, **kwargs)
    else:
        import xarray.testing

        xarray.testing.assert_equal(a, b, **kwargs)

    # check attributes are equal
    if check_attrs:
        assert a.attrs == b.attrs


def assert_qty_allclose(a, b, check_type=True, check_attrs=True, **kwargs):
    """Assert that Quantity objects *a* and *b* have numerically close values.

    When Quantity is AttrSeries, *a* and *b* are first passed through
    :meth:`as_quantity`.
    """
    if not check_type:
        a = Quantity(a)
        b = Quantity(b)

    if Quantity.CLASS == "AttrSeries":
        assert_series_equal(a.sort_index(), b.sort_index(), **kwargs)
    else:
        import xarray.testing

        kwargs.pop("check_dtype", None)
        xarray.testing.assert_allclose(a._sda.dense, b._sda.dense, **kwargs)

    # check attributes are equal
    if check_attrs:
        assert a.attrs == b.attrs


def random_qty(shape: Dict[str, int], **kwargs):
    """Return a Quantity with *shape* and random contents.

    Parameters
    ----------
    shape : dict
        Mapping from dimension names to
    kwargs
        Other keyword arguments to :class:`Quantity`.

    Returns
    -------
    Quantity
        Keys in `shape`—e.g. "foo"—result in a dimension named "foo" with
        coords "foo0", "foo1", etc., with total length matching the value.
        Data is random.
    """
    return Quantity(
        xr.DataArray(
            np.random.rand(*shape.values()),
            coords={
                dim: [f"{dim}{i}" for i in range(length)]
                for dim, length in shape.items()
            },
            dims=shape.keys(),
        ),
        **kwargs,
    )
