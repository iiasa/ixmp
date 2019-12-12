from functools import lru_cache, partial
import logging

import numpy
import pandas as pd
import pint
import xarray as xr

from .attrseries import AttrSeries
from .key import Key


log = logging.getLogger(__name__)

# See also:
# - docstring of attrseries.AttrSeries.
# - test_report_size() for a test that shows how non-sparse xr.DataArray
#   triggers MemoryError.
Quantity = AttrSeries
# Quantity = xr.DataArray

#: Replacements to apply to quantity units before parsing by
#: :doc:`pint <pint:index>`. Mapping from original unit -> preferred unit.
REPLACE_UNITS = {
    '%': 'percent',
}

#: Dimensions to rename when extracting raw data from Scenario objects.
#: Mapping from Scenario dimension name -> preferred dimension name.
RENAME_DIMS = {}

#: :doc:`pint <pint:index>` unit registry for processing quantity units.
#: All units handled by :mod:`ixmp.reporting` must be either standard SI units,
#: or added to this registry.
UNITS = pint.UnitRegistry()


def clean_units(input_string):
    """Tolerate messy strings for units.

    Handles two specific cases found in |MESSAGEix| test cases:

    - Dimensions enclosed in '[]' have these characters stripped.
    - The '%' symbol cannot be supported by pint, because it is a Python
      operator; it is translated to 'percent'.

    """
    input_string = input_string.strip('[]')
    for old, new in REPLACE_UNITS.items():
        input_string = input_string.replace(old, new)
    return input_string


def collect_units(*args):
    """Return an list of '_unit' attributes for *args*."""
    for arg in args:
        if '_unit' in arg.attrs:
            # Convert units if necessary
            if isinstance(arg.attrs['_unit'], str):
                arg.attrs['_unit'] = UNITS.parse_units(arg.attrs['_unit'])
        else:
            log.debug('assuming {} is unitless'.format(arg))
            arg.attrs['_unit'] = UNITS.parse_units('')

    return [arg.attrs['_unit'] for arg in args]


def dims_for_qty(data):
    """Return the list of dimensions for *data*.

    If *data* is a :class:`pandas.DataFrame`, its columns are processed;
    otherwise it must be a list.

    ixmp.reporting.RENAME_DIMS is used to rename dimensions.
    """
    if isinstance(data, pd.DataFrame):
        # List of the dimensions
        dims = data.columns.tolist()
    else:
        dims = list(data)

    # Remove columns containing values or units; dimensions are the remainder
    for col in 'value', 'lvl', 'mrg', 'unit':
        try:
            dims.remove(col)
        except ValueError:
            continue

    # Rename dimensions
    return [RENAME_DIMS.get(d, d) for d in dims]


def filter_concat_args(args):
    """Filter out str and Key from *args*.

    A warning is logged for each element removed.
    """
    for arg in args:
        if isinstance(arg, (str, Key)):
            log.warn('concat() argument {arg!r} missing; will be omitted')
            continue
        yield arg


@lru_cache(1)
def get_reversed_rename_dims():
    return {v: k for k, v in RENAME_DIMS.items()}


def keys_for_quantity(ix_type, name, scenario):
    """Iterate over keys for *name* in *scenario*."""
    # Retrieve names of the indices of the ixmp item, without loading the data
    dims = dims_for_qty(scenario.idx_names(name))

    # Column for retrieving data
    column = 'value' if ix_type == 'par' else 'lvl'

    # A computation to retrieve the data
    key = Key(name, dims)
    yield (key, (partial(data_for_quantity, ix_type, name, column),
                 'scenario', 'filters'))

    # Add the marginal values at full resolution, but no aggregates
    if ix_type == 'equ':
        yield (Key('{}-margin'.format(name), dims),
               (partial(data_for_quantity, ix_type, name, 'mrg'),
                'scenario', 'filters'))

    # Partial sums
    yield from key.iter_sums()


def _parse_units(units_series):
    """Return a :class:`pint.Unit` for a :class:`pd.Series` of strings."""
    unit = pd.unique(units_series)

    if len(unit) > 1:
        # py3.5 compat: could use an f-string here
        raise ValueError('mixed units {!r}'.format(list(unit)))

    # Helper method to return an intelligible exception
    def invalid(unit):
        chars = ''.join(c for c in '-?$' if c in unit)
        return ValueError(("unit {!r} cannot be parsed; contains invalid "
                           "character(s) {!r}").format(unit, chars))

    # Parse units
    try:
        unit = clean_units(unit[0])
        unit = UNITS.parse_units(unit)
    except IndexError:
        # Quantity has no unit
        unit = UNITS.parse_units('')
    except pint.UndefinedUnitError:
        # Unit(s) do not exist; define them in the UnitRegistry

        # Split possible compound units
        for u in unit.split('/'):
            if u in dir(UNITS):
                # Unit already defined
                continue

            # py3.5 compat: could use f-strings here
            definition = '{0} = [{0}]'.format(u)
            log.info('Add unit definition: {}'.format(definition))

            # This line will fail silently for units like 'G$'
            UNITS.define(definition)

        # Try to parse again
        try:
            unit = UNITS.parse_units(unit)
        except pint.UndefinedUnitError:
            # Handle the silent failure of define(), above
            raise invalid(unit) from None
    except AttributeError:
        # Unit contains a character like '-' that throws off pint
        # NB this 'except' clause must be *after* UndefinedUnitError, since
        #    that is a subclass of AttributeError.
        raise invalid(unit)

    return unit


def data_for_quantity(ix_type, name, column, scenario, filters=None):
    """Retrieve data from *scenario*.

    Parameters
    ----------
    ix_type : 'equ' or 'par' or 'var'
        Type of the ixmp object.
    name : str
        Name of the ixmp object.
    column : 'mrg' or 'lvl' or 'value'
        Data to retrieve. 'mrg' and 'lvl' are valid only for ``ix_type='equ'``,
        and 'level' otherwise.
    scenario : ixmp.Scenario
        Scenario containing data to be retrieved.
    filters : dict, optional
        Mapping from dimensions to iterables of allowed values along each
        dimension.

    Returns
    -------
    :class:`Quantity`
        Data for *name*.
    """
    log.debug('Retrieving data for {}'.format(name))

    # Only use the relevant filters
    idx_names = scenario.idx_names(name)
    if filters:
        # Dimensions of the object
        dims = dims_for_qty(idx_names)

        # Mapping from renamed dimensions to Scenario dimension names
        MAP = get_reversed_rename_dims()

        filters_to_use = {}
        for dim, values in filters.items():
            if dim in dims:
                # *dim* is in this ixmp object, so the filter can be used
                filters_to_use[MAP.get(dim, dim)] = values

        filters = filters_to_use

    # Retrieve quantity data
    data = getattr(scenario, ix_type)(name, filters)

    # ixmp/GAMS scalar is not returned as pd.DataFrame
    if isinstance(data, dict):
        data = pd.DataFrame.from_records([data])

    # Warn if no values are returned.
    # TODO construct an empty Quantity with the correct dimensions *even if* no
    #      values are returned.
    if len(data) == 0:
        log.warning(f'0 values for {ix_type} {name!r} using filters:'
                    f'\n  {filters!r}\n  Subsequent computations may fail.')

    # Convert categorical dtype to str
    data = data.astype({col: str for col in idx_names})

    # List of the dimensions
    dims = dims_for_qty(data)

    # Remove the unit from the DataFrame
    try:
        attrs = {'_unit': _parse_units(data.pop('unit'))}
    except KeyError:
        # 'equ' are returned without units
        attrs = {}
    except ValueError as e:
        if 'mixed units' in e.args[0]:
            # Discard mixed units
            log.warn('{} discarded for {!r}'.format(e.args[0], name))
            attrs = {'_unit': UNITS.parse_units('')}
        else:
            # Raise all other ValueErrors
            raise

    # Set index if 1 or more dimensions
    if len(dims):
        # First rename, then set index
        data = data.rename(columns=RENAME_DIMS) \
                   .set_index(dims)

    # Check sparseness
    # try:
    #     shape = list(map(len, data.index.levels))
    # except AttributeError:
    #     shape = [data.index.size]
    # size = reduce(mul, shape)
    # filled = 100 * len(data) / size if size else 'NA'
    # need_to_chunk = size > 1e7 and filled < 1
    # info = (name, shape, filled, size, need_to_chunk)
    # log.debug(' '.join(map(str, info)))

    # Convert to a Quantity, assign attrbutes and name
    qty = as_quantity(data[column]) \
        .assign_attrs(attrs) \
        .rename(name + ('-margin' if column == 'mrg' else ''))

    try:
        # Remove length-1 dimensions for scalars
        qty = qty.squeeze('index', drop=True)
    except KeyError:
        pass

    return qty


def as_attrseries(obj):
    """Convert obj to an AttrSeries."""
    return AttrSeries(obj)


def as_sparse_xarray(obj):  # pragma: no cover
    """Convert *obj* to an :class:`xarray.DataArray` with sparse.COO
    storage."""
    import sparse
    from xarray.core.dtypes import maybe_promote

    if isinstance(obj, xr.DataArray) and isinstance(obj.data, numpy.ndarray):
        return xr.DataArray(
            data=sparse.COO.from_numpy(
                obj.data,
                fill_value=maybe_promote(obj.data.dtype)[1]),
            coords=obj.coords,
            dims=obj.dims,
            name=obj.name,
            attrs=obj.attrs,
        )
    elif isinstance(obj, pd.Series):
        return xr.DataArray.from_series(obj, sparse=True)

    else:
        print(type(obj), type(obj.data))
        return obj


as_quantity = as_attrseries if Quantity is AttrSeries else as_sparse_xarray
