"""Elementary computations for reporting."""
# TODO:
# - Accept pd.DataFrame user input by casting to xr.DataArray with a pd_to_xr()
#   method that is a no-op for xr objects.

import pandas as pd
import xarray as xr


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
def aggregate(quantity, dimensions):
    """Aggregate *quantity* over *dimensions*."""
    return quantity.sum(dim=dimensions)


def disaggregate_shares(quantity, shares):
    """Disaggregate *quantity* by *shares*."""
    return quantity * shares


def product(*quantities):
    # TODO handle units intelligently (req. A9)
    raise NotImplementedError


def ratio(numerator, denominator):
    # TODO handle units intelligently (req. A9)
    raise NotImplementedError


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


def write_report(key, path):
    """Write the report identified by *key* to the file at *path*."""
    # TODO intelligently handle different formats of *report*, e.g. CSV
    path.write_text(key)
