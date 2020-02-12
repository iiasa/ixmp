"""Elementary computations for reporting."""
# Notes:
# - To avoid ambiguity, computations should not have default arguments. Define
#   default values for the corresponding methods on the Reporter class.
import logging
from pathlib import Path

import pandas as pd
import xarray as xr

from .quantity import AttrSeries, Quantity, as_quantity
from .utils import (
    UNITS,
    RENAME_DIMS,
    dims_for_qty,
    collect_units,
    filter_concat_args,
    get_reversed_rename_dims,
    parse_units,
)

__all__ = [
    'aggregate',
    'concat',
    'data_for_quantity',
    'disaggregate_shares',
    'load_file',
    'product',
    'ratio',
    'sum',
    'write_report',
]


log = logging.getLogger(__name__)


# Carry unit attributes automatically
xr.set_options(keep_attrs=True)


def data_for_quantity(ix_type, name, column, scenario, config):
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
    filters = config.get('filters', None)
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
        attrs = {'_unit': parse_units(data.pop('unit'))}
    except KeyError:
        # 'equ' are returned without units
        attrs = {}
    except ValueError as e:
        if 'mixed units' in e.args[0]:
            # Discard mixed units
            log.warning(f'{name}: {e.args[0]} discarded')
            attrs = {'_unit': UNITS.parse_units('')}
        else:
            # Raise all other ValueErrors
            raise

    # Apply units
    try:
        new_unit = config['units']['apply'][name]
    except KeyError:
        pass
    else:
        log.info(f"{name}: replace units {attrs['_unit']} with {new_unit}")
        attrs['_unit'] = UNITS.parse_units(new_unit)

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


# Calculation
# TODO: should we call this weighted sum?
def sum(quantity, weights=None, dimensions=None):
    """Sum *quantity* over *dimensions*, with optional *weights*."""
    if weights is None:
        weights, w_total = 1, 1
    else:
        w_total = weights.sum(dim=dimensions)

    result = (quantity * weights).sum(dim=dimensions) / w_total
    result.attrs['_unit'] = collect_units(quantity)[0]

    return result


def aggregate(quantity, groups, keep):
    """Aggregate *quantity* by *groups*.

    Parameters
    ----------
    quantity : :class:`Quantity <ixmp.reporting.utils.Quantity>`
    groups: dict of dict
        Top-level keys are the names of dimensions in `quantity`. Second-level
        keys are group names; second-level values are lists of labels along the
        dimension to sum into a group.
    keep : bool
        If True, the members that are aggregated into a group are returned with
        the group sums. If False, they are discarded.

    Returns
    -------
    :class:`Quantity <ixmp.reporting.utils.Quantity>`
        Same dimensionality as `quantity`.

    """
    # NB .transpose() below is necessary only for Quantity == AttrSeries. It
    #   can be removed when Quantity = xr.DataArray.
    dim_order = quantity.dims

    for dim, dim_groups in groups.items():
        # Optionally keep the original values
        values = [quantity] if keep else []

        # Aggregate each group
        for group, members in dim_groups.items():
            values.append(quantity.sel({dim: members})
                                  .sum(dim=dim)
                                  .assign_coords(**{dim: group})
                                  .transpose(*dim_order))

        # Reassemble to a single dataarray
        quantity = concat(*values, dim=dim)

    return quantity


def concat(*objs, **kwargs):
    """Concatenate Quantity *objs*.

    Any strings included amongst *args* are discarded, with a logged warning;
    these usually indicate that a quantity is referenced which is not in the
    Reporter.
    """
    objs = filter_concat_args(objs)
    if Quantity is AttrSeries:
        kwargs.pop('dim')
        return pd.concat(objs, **kwargs)
    elif Quantity is xr.DataArray:  # pragma: no cover
        return xr.concat(objs, **kwargs)


def disaggregate_shares(quantity, shares):
    """Disaggregate *quantity* by *shares*."""
    result = quantity * shares
    result.attrs['_unit'] = collect_units(quantity)[0]
    return result


def product(*quantities):
    """Return the product of any number of *quantities*."""
    if len(quantities) == 1:
        quantities = [quantities]

    # Iterator over (quantity, unit) tuples
    items = zip(quantities, collect_units(*quantities))

    # Initialize result values with first entry
    result, u_result = next(items)

    def _align_levels(ref, obj):
        """Work around https://github.com/pandas-dev/pandas/issues/25760

        Return a copy of *obj* with common levels in the same order as *ref*.

        TODO remove when Quantity is xr.DataArray, or above issues is closed.
        """
        if not isinstance(obj.index, pd.MultiIndex):
            return obj
        common = [n for n in ref.index.names if n in obj.index.names]
        unique = [n for n in obj.index.names if n not in common]
        return obj.reorder_levels(common + unique)

    # Iterate over remaining entries
    for q, u in items:
        if Quantity is AttrSeries:
            result = (result * _align_levels(result, q)).dropna()
        else:
            result = result * q
        u_result *= u

    result.attrs['_unit'] = u_result

    return result


def ratio(numerator, denominator):
    """Return the ratio *numerator* / *denominator*."""
    # Handle units
    u_num, u_denom = collect_units(numerator, denominator)

    result = numerator / denominator
    result.attrs['_unit'] = u_num / u_denom

    if Quantity is AttrSeries:
        result.dropna(inplace=True)

    return result


# Input and output
def load_file(path):
    """Read the file at *path* and return its contents.

    Some file formats are automatically converted into objects for direct use
    in reporting code:

    - *csv*: converted to :class:`xarray.DataArray`. CSV files must have a
      'value' column; all others are treated as indices.

    """
    # TODO optionally cache: if the same Reporter is used repeatedly, then the
    #      file will be read each time; instead cache the contents in memory.
    if path.suffix == '.csv':
        # TODO handle a wider variety of CSV files
        data = pd.read_csv(path)
        index_columns = data.columns.tolist()
        index_columns.remove('value')
        return xr.DataArray.from_series(data.set_index(index_columns)['value'])
    elif path.suffix in ('.xls', '.xlsx'):
        # TODO define expected Excel data input format
        raise NotImplementedError  # pragma: no cover
    elif path.suffix == '.yaml':
        # TODO define expected YAML data input format
        raise NotImplementedError  # pragma: no cover
    else:
        # Default
        return open(path).read()


def write_report(quantity, path):
    """Write a quantity to a file.

    Parameters
    ----------
    path : str or Path
        Path to the file to be written.
    """
    path = Path(path)

    if path.suffix == '.csv':
        quantity.to_dataframe().to_csv(path)
    elif path.suffix == '.xlsx':
        quantity.to_dataframe().to_excel(path, merge_cells=False)
    else:
        path.write_text(quantity)
