import pandas as pd
import pandas.core.indexes.base as ibase
import xarray as xr


class AttrSeries(pd.Series):
    """:class:`pandas.Series` subclass imitating :class:`xarray.DataArray`.

    The AttrSeries class provides similar methods and behaviour to
    :class:`xarray.DataArray`, so that :mod:`ixmp.reporting.computations`
    methods can use xarray-like syntax.

    Parameters
    ----------
    units : str or pint.Unit, optional
        Set the units attribute. The value is converted to :class:`pint.Unit`
        and added to `attrs`.
    attrs : :class:`~collections.abc.Mapping`, optional
        Set the :attr:`~pandas.Series.attrs` of the AttrSeries. This attribute
        was added in `pandas 1.0
        <https://pandas.pydata.org/docs/whatsnew/v1.0.0.html>`_, but is not
        currently supported by the Series constructor.
    """

    # See https://pandas.pydata.org/docs/development/extending.html
    @property
    def _constructor(self):
        return AttrSeries

    def __init__(self, data=None, *args, name=None, attrs=None, **kwargs):
        attrs = attrs or dict()

        if hasattr(data, "attrs"):
            # Use attrs from an existing object
            new_attrs = data.attrs.copy()

            # Overwrite with explicit attrs argument
            new_attrs.update(attrs)
            attrs = new_attrs

        if isinstance(data, (AttrSeries, xr.DataArray)):
            # Extract name from existing object or use the argument
            name = ibase.maybe_extract_name(name, data, type(self))

            # Pre-convert to pd.Series from xr.DataArray to preserve names and
            # labels. For AttrSeries, this is a no-op (see below).
            data = data.to_series()

        # Don't pass attrs to pd.Series constructor; it currently does not
        # accept them
        super().__init__(data, *args, name=name, **kwargs)

        # Update the attrs after initialization
        self.attrs.update(attrs)

    @classmethod
    def from_series(cls, series, sparse=None):
        """Like :meth:`xarray.DataArray.from_series`."""
        return cls(series)

    def assign_coords(self, **kwargs):
        """Like :meth:`xarray.DataArray.assign_coords`."""
        return pd.concat([self], keys=kwargs.values(), names=kwargs.keys())

    @property
    def coords(self):
        """Like :attr:`xarray.DataArray.coords`. Read-only."""
        result = dict()
        for name, levels in zip(self.index.names, self.index.levels):
            result[name] = xr.Dataset(None, coords={name: levels})[name]
        return result

    @property
    def dims(self):
        """Like :attr:`xarray.DataArray.dims`."""
        return tuple(self.index.names)

    def drop(self, label):
        """Like :meth:`xarray.DataArray.drop`."""
        return self.droplevel(label)

    def item(self, *args):
        """Like :meth:`xarray.DataArray.item`."""
        if len(args) and args != (None,):
            raise NotImplementedError
        elif self.size != 1:
            raise ValueError
        return self.iloc[0]

    def rename(self, new_name_or_name_dict):
        """Like :meth:`xarray.DataArray.rename`."""
        if isinstance(new_name_or_name_dict, dict):
            return self.rename_axis(index=new_name_or_name_dict)
        else:
            return super().rename(new_name_or_name_dict)

    def sel(self, indexers=None, drop=False, **indexers_kwargs):
        """Like :meth:`xarray.DataArray.sel`."""
        indexers = indexers.copy() if indexers else {}
        indexers.update(indexers_kwargs)
        if len(indexers) == 1:
            level, key = list(indexers.items())[0]
            if isinstance(key, str) and not drop:
                if isinstance(self.index, pd.MultiIndex):
                    # When using .loc[] to select 1 label on 1 level, pandas
                    # drops the level. Use .xs() to avoid this behaviour unless
                    # drop=True
                    return AttrSeries(self.xs(key, level=level, drop_level=False))
                else:
                    # No MultiIndex; use .loc with a slice to avoid returning
                    # scalar
                    return self.loc[slice(key, key)]

        idx = tuple(indexers.get(n, slice(None)) for n in self.index.names)
        return AttrSeries(self.loc[idx])

    def sum(self, *args, **kwargs):
        """Like :meth:`xarray.DataArray.sum`."""
        obj = super(AttrSeries, self)
        attrs = None

        try:
            dim = kwargs.pop("dim")
        except KeyError:
            dim = list(args)
            args = tuple()

        if len(dim) == len(self.index.names):
            bad_dims = set(dim) - set(self.index.names)
            if bad_dims:
                raise ValueError(
                    f"{bad_dims} not found in array dimensions " f"{self.index.names}"
                )
            # Simple sum
            kwargs = {}
        else:
            # pivot and sum across columns
            obj = self.unstack(dim)
            kwargs["axis"] = 1
            # Result will be DataFrame; re-attach attrs when converted to
            # AttrSeries
            attrs = self.attrs

        return AttrSeries(obj.sum(*args, **kwargs), attrs=attrs)

    def squeeze(self, dim=None, *args, **kwargs):
        """Like :meth:`xarray.DataArray.squeeze`."""
        assert kwargs.pop("drop", True)

        try:
            idx = self.index.remove_unused_levels()
        except AttributeError:
            return self

        to_drop = []
        for i, name in enumerate(idx.names):
            if dim and name != dim:
                continue
            elif len(idx.levels[i]) > 1:
                if dim is None:
                    continue
                else:
                    raise ValueError(
                        "cannot select a dimension to squeeze out which has "
                        "length greater than one"
                    )

            to_drop.append(name)

        if dim and not to_drop:
            # Specified dimension does not exist
            raise KeyError(dim)

        return self.droplevel(to_drop)

    def transpose(self, *dims):
        """Like :meth:`xarray.DataArray.transpose`."""
        return self.reorder_levels(dims)

    def to_dataframe(self):
        """Like :meth:`xarray.DataArray.to_dataframe`."""
        return self.to_frame()

    def to_series(self):
        """Like :meth:`xarray.DataArray.to_series`."""
        return self

    def align_levels(self, other):
        """Work around https://github.com/pandas-dev/pandas/issues/25760.

        Return a copy of *obj* with common levels in the same order as *ref*.
        """
        if not isinstance(self.index, pd.MultiIndex):
            return self
        common = [n for n in other.index.names if n in self.index.names]
        unique = [n for n in self.index.names if n not in common]
        return self.reorder_levels(common + unique)
