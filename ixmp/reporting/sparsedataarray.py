from typing import Tuple

import numpy as np
import pandas as pd
import sparse  # NB warnings from sparse are filtered in computations.py
import xarray as xr
from xarray.core.utils import either_dict_or_kwargs


@xr.register_dataarray_accessor("_sda")
class SparseAccessor:
    """:mod:`xarray` accessor to help :class:`SparseDataArray`.

    See the xarray accessor documentation, e.g.
    :func:`~xarray.register_dataarray_accessor`.
    """

    def __init__(self, obj):
        self.da = obj

    def convert(self):
        """Return a :class:`SparseDataArray` instance."""
        if not self.da._sda.COO_data:
            # Dense (numpy.ndarray) data; convert to sparse
            data = sparse.COO.from_numpy(self.da.data, fill_value=None)
        elif not np.isnan(self.da.data.fill_value):
            # sparse.COO with non-NaN fill value; copy and change
            data = self.da.data.copy(deep=False)
            data.fill_value = data.dtype.type(np.nan)
        else:
            # No change
            data = self.da.data

        if isinstance(self.da, SparseDataArray):
            # Replace the variable, returning a copy
            variable = self.da.variable._replace(data=data)
            return self.da._replace(variable=variable)
        else:
            # Construct
            return SparseDataArray(
                data=data,
                coords=self.da.coords,
                dims=self.da.dims,
                name=self.da.name,
                attrs=self.da.attrs,
            )

    @property
    def COO_data(self):
        """:obj:`True` if the DataArray has :class:`sparse.COO` data."""
        return isinstance(self.da.data, sparse.COO)

    @property
    def dense(self):
        """Return a copy with dense (:class:`.ndarray`) data."""
        # Use existing method xr.Variable._to_dense()
        return self.da._replace(variable=self.da.variable._to_dense())

    @property
    def dense_super(self):
        """Return a proxy to a :class:`.ndarray`-backed :class:`.DataArray`."""
        return super(SparseDataArray, self.dense)


class SparseDataArray(xr.DataArray):
    """:class:`~xarray.DataArray` with sparse data.

    SparseDataArray uses :class:`sparse.COO` for storage with :data:`numpy.nan`
    as its :attr:`sparse.COO.fill_value`. Some methods of
    :class:`~xarray.DataArray` are overridden to ensure data is in sparse, or
    dense, format as necessary, to provide expected functionality not currently
    supported by :mod:`sparse`, and to avoid exhausting memory for some
    operations that require dense data.
    """

    __slots__: Tuple = tuple()

    @classmethod
    def from_series(cls, obj, sparse=True):
        """Convert a pandas.Series into a SparseDataArray."""
        # Call the parent method always with sparse=True, then re-wrap
        return xr.DataArray.from_series(obj, sparse=True)._sda.convert()

    def equals(self, other) -> bool:
        """True if two SparseDataArrays have the same dims, coords, and values.

        Overrides :meth:`~xarray.DataArray.equals` for sparse data.
        """
        # Necessary for :meth:`xarray.testing.assert_equal` to work.
        return self.variable.equals(other.variable, equiv=np.equal)

    @property
    def loc(self):
        """Attribute for location based indexing like pandas.

        .. note:: This version does not allow assignment, since the underlying
           sparse array is read-only. To modify the contents, create a copy or
           perform an operation that returns a new array.
        """
        return self._sda.dense_super.loc

    def sel(
        self, indexers=None, method=None, tolerance=None, drop=False, **indexers_kwargs
    ) -> "SparseDataArray":
        """Return a new array by selecting labels along the specified dim(s).

        Overrides :meth:`~xarray.DataArray.sel` to handle >1-D indexers with
        sparse data.
        """
        indexers = either_dict_or_kwargs(indexers, indexers_kwargs, "sel")
        if isinstance(indexers, dict) and len(indexers) > 1:
            result = self
            for k, v in indexers.items():
                result = result.sel(
                    {k: v}, method=method, tolerance=tolerance, drop=drop
                )
            return result
        else:
            return (
                super()
                .sel(indexers=indexers, method=method, tolerance=tolerance, drop=drop)
                ._sda.convert()
            )

    def to_dataframe(self, name=None):
        """Convert this array and its coords into a :class:`~xarray.DataFrame`.

        Overrides :meth:`~xarray.DataArray.to_dataframe`.
        """
        return self.to_series().to_frame(name)

    def to_series(self) -> pd.Series:
        """Convert this array into a :class:`~pandas.Series`.

        Overrides :meth:`~xarray.DataArray.to_series` to create the series
        without first converting to a potentially very large
        :class:`numpy.ndarray`.
        """
        # Use SparseArray.coords and .data (each already 1-D) to construct the
        # pd.Series

        # Construct a pd.MultiIndex without using .from_product
        index = pd.MultiIndex.from_arrays(self.data.coords, names=self.dims).set_levels(
            [self.coords[d].values for d in self.dims]
        )
        return pd.Series(self.data.data, index=index, name=self.name)
