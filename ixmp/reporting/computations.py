"""Elementary computations for reporting."""
# Notes:
# - To avoid ambiguity, computations should not have default arguments. Define
#   default values for the corresponding methods on the Reporter class.
from pathlib import Path

import pandas as pd
import xarray as xr

from .utils import collect_units, AttrSeries, Quantity, concat

__all__ = [
    'aggregate',
    'disaggregate_shares',
    'make_dataframe',
    'load_file',
    'sum',
    'write_report',
]


# Carry unit attributes automatically
xr.set_options(keep_attrs=True)


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
        quantity = concat(values, dim=dim)

    return quantity


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

    # Iterate over remaining entries
    for q, u in items:
        result = result * q
        u_result *= u

    result.attrs['_unit'] = u_result

    if Quantity is AttrSeries:
        result.dropna(inplace=True)

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


# Conversion
def make_dataframe(*quantities):
    """Concatenate *quantities* into a single pd.DataFrame."""
    # TODO also rename
    raise NotImplementedError


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
        raise NotImplementedError
    elif path.suffix == '.yaml':
        # TODO define expected YAML data input format
        raise NotImplementedError
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
