"""Elementary computations for reporting."""
# TODO:
# - Accept pd.DataFrame user input by casting to xr.DataArray with a pd_to_xr
#   method that is a no-op for xr objects.

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


def aggregate(quantity, dimensions):
    """Aggregate *quantity* over *dimensions*."""
    return quantity.sum(dim=dimensions)


def disaggregate_shares(quantity, shares):
    """Disaggregate *quantity* by *shares*."""
    return quantity * shares


# Conversion
def make_dataframe(*quantities):
    """Concatenate *quantities* into a single pd.DataFrame."""
    # TODO also rename
    raise NotImplementedError


# Input and output
def load_file(path):
    """Read the file at *path* and return its contents."""
    # TODO automatically parse common file formats: yaml, csv, xls(x)
    # TODO optionally cache: if the same Reporter is used repeatedly, then the
    #      file will be read each time; instead cache the contents in memory.
    return open(path).read()


def write_report(key, path):
    """Write the report identified by *key* to the file at *path*."""
    # TODO intelligently handle different formats of *report*
    path.write_text(key)
