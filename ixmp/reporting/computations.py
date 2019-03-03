"""Elementary computations for reporting."""
# TODO:
# - Accept pd.DataFrame user input by casting to xr.DataArray with a pd_to_xr
#   method that is a no-op for xr objects.

import xarray as xr


xr.set_options(keep_attrs=True)


def aggregate(var, dims):
    return var.sum(dim=dims)


def disaggregate_shares(var, shares):
    return var * shares


# Conversion
def make_dataframe(*vars):
    """Concatenate *vars* into a single pd.DataFrame."""
    # TODO also rename
    raise NotImplementedError


# Input and output
def load_file(path):
    # TODO automatically parse common file formats: yaml, csv, xls(x)
    # TODO optionally cache: if the same Reporter is used repeatedly, then the
    #      file will be read each time; instead cache the contents in memory.
    return open(path).read()


def write_report(report, path):
    # TODO intelligently handle different formats of *report*
    path.write_text(report)
