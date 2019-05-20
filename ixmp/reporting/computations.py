"""Elementary computations for reporting."""
# TODO:
# - Accept pd.DataFrame user input by casting to xr.DataArray with a pd_to_xr()
#   method that is a no-op for xr objects.

import pandas as pd
import xarray as xr

from .utils import collect_units

__all__ = [
    'aggregate',
    'disaggregate_shares',
    'make_dataframe',
    'load_file',
    'write_report',
]


# Carry unit attributes automatically
xr.set_options(keep_attrs=True)


# Calculation
def aggregate(quantity, weights=None, dimensions=None):
    """Aggregate *quantity* over *dimensions*, with optional *weights*."""
    if weights is not None:
        result = ((quantity * weights).sum(dim=dimensions) /
                   weights.sum(dim=dimensions))
    else:
        result = quantity.sum(dim=dimensions)

    result.attrs['_unit'] = collect_units(result)[0]

    return result


def aggregate2(quantity, groups, keep):
    """Aggregate *quantity* by *groups*."""
    for dim, dim_groups in groups.items():
        values = []
        for group, members in dim_groups.items():
            values.append(quantity.sel({dim: members})
                                  .sum(dim=dim)
                                  .assign_coords(**{dim: group}))
        if keep:
            # Prepend the original values
            values.insert(0, quantity)

        # Reassemble to a single dataarray
        quantity = xr.concat(values, dim=dim)
    return quantity


def disaggregate_shares(quantity, shares):
    """Disaggregate *quantity* by *shares*."""
    return quantity * shares


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
        result *= q
        u_result *= u

    result.attrs['_unit'] = u_result

    return result


def ratio(numerator, denominator):
    """Return the ratio *numerator* / *denominator*."""
    # Handle units
    u_num, u_denom = collect_units(numerator, denominator)

    result = numerator / denominator
    result.attrs['_unit'] = u_num / u_denom

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
    """Write the report identified by *key* to the file at *path*."""
    if path.suffix == '.csv':
        quantity.to_dataframe().to_csv(path)
    elif path.suffix == '.xlsx':
        quantity.to_dataframe().to_excel(path, merge_cells=False)
    else:
        path.write_text(quantity)
