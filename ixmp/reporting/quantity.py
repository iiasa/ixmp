from collections import OrderedDict
from collections.abc import Collection
from copy import deepcopy

import numpy
import pandas as pd
from pandas.core.generic import NDFrame
import pint
import xarray as xr


class AttrSeries(pd.Series):
    """:class:`pandas.Series` subclass imitating :class:`xarray.DataArray`.

    Future versions of :mod:`ixmp.reporting` will use :class:`xarray.DataArray`
    as :class:`Quantity`; however, because :mod:`xarray` currently lacks sparse
    matrix support, ixmp quantities may be too large for available memory.

    The AttrSeries class provides similar methods and behaviour to
    :class:`xarray.DataArray`, such as an `attrs` dictionary for metadata, so
    that :mod:`ixmp.reporting.computations` methods can use xarray-like syntax.
    """

    # normal properties
    _metadata = ('attrs', )

    def __init__(self, *args, **kwargs):
        if 'units' in kwargs:
            kwargs.setdefault('attrs', {})
            kwargs['attrs']['_unit'] = pint.Unit(kwargs.pop('units'))
        if 'attrs' in kwargs:
            # Use provided attrs
            attrs = kwargs.pop('attrs')
        elif hasattr(args[0], 'attrs'):
            # Use attrs from an xarray object
            attrs = args[0].attrs.copy()

            # pre-convert to a pd.Series to preserve names and labels
            args = list(args)
            try:
                args[0] = args[0].to_series()
            except AttributeError:
                pass  # args[0] was already pd.Series
        else:
            # default empty
            attrs = OrderedDict()

        super().__init__(*args, **kwargs)

        self.attrs = attrs

    @classmethod
    def from_series(cls, series, sparse=None):
        return cls(series)

    def assign_attrs(self, d):
        self.attrs.update(d)
        return self

    def assign_coords(self, **kwargs):
        return pd.concat([self], keys=kwargs.values(), names=kwargs.keys())

    @property
    def coords(self):
        """Read-only."""
        return dict(zip(self.index.names, self.index.levels))

    @property
    def dims(self):
        return tuple(self.index.names)

    def drop(self, label):
        return self.droplevel(label)

    def rename(self, new_name_or_name_dict):
        if isinstance(new_name_or_name_dict, dict):
            return self.rename_axis(index=new_name_or_name_dict)
        else:
            return super().rename(new_name_or_name_dict)

    def sel(self, indexers=None, drop=False, **indexers_kwargs):
        indexers = indexers or {}
        indexers.update(indexers_kwargs)
        if len(indexers) == 1:
            level, key = list(indexers.items())[0]
            if not isinstance(key, Collection) and not drop:
                # When using .loc[] to select 1 label on 1 level, pandas drops
                # the level. Use .xs() to avoid this behaviour unless drop=True
                return AttrSeries(self.xs(key, level=level, drop_level=False))

        idx = tuple(indexers.get(l, slice(None)) for l in self.index.names)
        return AttrSeries(self.loc[idx])

    def sum(self, *args, **kwargs):
        try:
            dim = kwargs.pop('dim')
            if isinstance(self.index, pd.MultiIndex):
                if len(dim) == len(self.index.names):
                    # assume dimensions = full multi index, do simple sum
                    obj = self
                    kwargs = {}
                else:
                    # pivot and sum across columns
                    obj = self.unstack(dim)
                    kwargs['axis'] = 1
            else:
                if dim != [self.index.name]:
                    raise ValueError(dim, self.index.name, self)
                obj = super()
                kwargs['level'] = dim
        except KeyError:
            obj = super()
        return AttrSeries(obj.sum(*args, **kwargs))

    def squeeze(self, *args, **kwargs):
        kwargs.pop('drop')
        return super().squeeze(*args, **kwargs) if len(self) > 1 else self

    def as_xarray(self):
        return xr.DataArray.from_series(self)

    def transpose(self, *dims):
        return self.reorder_levels(dims)

    def to_dataframe(self):
        return self.to_frame()

    def to_series(self):
        return self

    @property
    def _constructor(self):
        return AttrSeries

    def __finalize__(self, other, method=None, **kwargs):
        """Propagate metadata from other to self.

        This is identical to the version in pandas, except deepcopy() is added
        so that the 'attrs' OrderedDict is not double-referenced.
        """
        if isinstance(other, NDFrame):
            for name in self._metadata:
                object.__setattr__(self, name,
                                   deepcopy(getattr(other, name, None)))
        return self


#: The current internal class used to represent reporting quantities.
#: :meth:`as_quantity` always converts to this type.
Quantity = AttrSeries
# See also:
# - test_report_size() for a test that shows how non-sparse xr.DataArray
#   triggers MemoryError.
# Quantity = xr.DataArray


def as_sparse_xarray(obj, units=None):  # pragma: no cover
    """Convert *obj* to :class:`xarray.DataArray` with sparse.COO storage."""
    import sparse
    from xarray.core.dtypes import maybe_promote

    if isinstance(obj, xr.DataArray) and isinstance(obj.data, numpy.ndarray):
        result = xr.DataArray(
            data=sparse.COO.from_numpy(
                obj.data,
                fill_value=maybe_promote(obj.data.dtype)[1]),
            coords=obj.coords,
            dims=obj.dims,
            name=obj.name,
            attrs=obj.attrs,
        )
    elif isinstance(obj, pd.Series):
        result = xr.DataArray.from_series(obj, sparse=True)
    else:
        print(type(obj), type(obj.data))
        result = obj

    if units:
        result.attrs['_unit'] = pint.Unit(units)

    return result


#: Returns
#: -------
#: .Quantity
#:     `obj` converted to the current Quantity type.
as_quantity = AttrSeries if Quantity is AttrSeries else as_sparse_xarray
