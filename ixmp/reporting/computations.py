"""Elementary computations for reporting."""
# Notes:
# - To avoid ambiguity, computations should not have default arguments. Define
#   default values for the corresponding methods on the Reporter class.
import logging
from collections.abc import Mapping
from pathlib import Path
from warnings import filterwarnings

import pandas as pd
import pint

from .quantity import Quantity, assert_quantity
from .utils import (
    RENAME_DIMS,
    collect_units,
    dims_for_qty,
    filter_concat_args,
    get_reversed_rename_dims,
    parse_units,
)

__all__ = [
    "aggregate",
    "apply_units",
    "concat",
    "data_for_quantity",
    "disaggregate_shares",
    "load_file",
    "product",
    "ratio",
    "select",
    "sum",
    "write_report",
]


# sparse 0.9.1, numba 0.49.0, triggered by xarray import
for msg in [
    "No direct replacement for 'numba.targets' available",
    "An import was requested from a module that has moved location.",
]:
    filterwarnings(action="ignore", message=msg, module="sparse._coo.numba_extension")

import xarray as xr  # noqa: E402

log = logging.getLogger(__name__)

# Carry unit attributes automatically
xr.set_options(keep_attrs=True)


def add(*quantities, fill_value=0.0):
    """Sum across multiple *quantities*."""
    # TODO check units
    assert_quantity(*quantities)

    if Quantity.CLASS == "SparseDataArray":
        quantities = map(Quantity, xr.broadcast(*quantities))

    # Initialize result values with first entry
    items = iter(quantities)
    result = next(items)

    # Iterate over remaining entries
    for q in items:
        if Quantity.CLASS == "AttrSeries":
            result = result.add(q, fill_value=fill_value).dropna()
        else:
            result = result + q

    return result


def apply_units(qty, units, quiet=False):
    """Simply apply *units* to *qty*.

    Logs on level ``WARNING`` if *qty* already has existing units.

    Parameters
    ----------
    qty : .Quantity
    units : str or pint.Unit
        Units to apply to *qty*
    quiet : bool, optional
        If :obj:`True` log on level ``DEBUG``.
    """
    registry = pint.get_application_registry()

    existing = qty.attrs.get("_unit", None)
    existing_dims = getattr(existing, "dimensionality", {})
    new_units = registry.parse_units(units)

    if len(existing_dims):
        # Some existing dimensions: log a message either way
        if existing_dims == new_units.dimensionality:
            log.debug(f"Convert '{existing}' to '{new_units}'")
            # NB use a factor because pint.Quantity cannot wrap AttrSeries
            factor = registry.Quantity(1.0, existing).to(new_units).magnitude
            result = qty * factor
        else:
            msg = f"Replace '{existing}' with incompatible '{new_units}'"
            log.warning(msg)
            result = qty.copy()
    else:
        # No units, or dimensionless
        result = qty.copy()

    result.attrs["_unit"] = new_units

    return result


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
    config : dict of (str -> dict)
        The key 'filters' may contain a mapping from dimensions to iterables
        of allowed values along each dimension.
        The key 'units'/'apply' may contain units to apply to the quantity; any
        such units overwrite existing units, without conversion.

    Returns
    -------
    :class:`Quantity`
        Data for *name*.
    """
    log.debug(f"{name}: retrieve data")

    registry = pint.get_application_registry()

    # Only use the relevant filters
    idx_names = scenario.idx_names(name)
    filters = config.get("filters", None)
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
        log.warning(
            f"0 values for {ix_type} {repr(name)} using filters:\n"
            f"  {repr(filters)}\n  Subsequent computations may fail."
        )

    # Convert columns with categorical dtype to str
    data = data.astype(
        {
            dt[0]: str
            for dt in data.dtypes.items()
            if isinstance(dt[1], pd.CategoricalDtype)
        }
    )

    # List of the dimensions
    dims = dims_for_qty(data)

    # Remove the unit from the DataFrame
    try:
        attrs = {"_unit": parse_units(data.pop("unit"))}
    except KeyError:
        # 'equ' are returned without units
        attrs = {}
    except ValueError as e:
        if "mixed units" in e.args[0]:
            # Discard mixed units
            log.warning(f"{name}: {e.args[0]} discarded")
            attrs = {"_unit": registry.Unit("")}
        else:
            # Raise all other ValueErrors
            raise

    # Apply units
    try:
        new_unit = config["units"]["apply"][name]
    except KeyError:
        pass
    else:
        log.info(
            f"{name}: replace units {attrs.get('_unit', '(none)')} with " f"{new_unit}"
        )
        attrs["_unit"] = registry.Unit(new_unit)

    # Set index if 1 or more dimensions
    if len(dims):
        # First rename, then set index
        data = data.rename(columns=RENAME_DIMS).set_index(dims)

    # Convert to a Quantity, assign attrbutes and name
    qty = Quantity(
        data[column], name=name + ("-margin" if column == "mrg" else ""), attrs=attrs
    )

    try:
        # Remove length-1 dimensions for scalars
        qty = qty.squeeze("index", drop=True)
    except (KeyError, ValueError):
        # KeyError if "index" does not exist; ValueError if its length is > 1
        pass

    return qty


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
    attrs = quantity.attrs.copy()

    for dim, dim_groups in groups.items():
        # Optionally keep the original values
        values = [quantity] if keep else []

        # Aggregate each group
        for group, members in dim_groups.items():
            agg = (
                quantity.sel({dim: members}).sum(dim=dim).assign_coords(**{dim: group})
            )
            if Quantity.CLASS == "AttrSeries":
                # .transpose() is necesary for AttrSeries
                agg = agg.transpose(*quantity.dims)
            else:
                # Restore fill_value=NaN for compatibility
                agg = agg._sda.convert()
            values.append(agg)

        # Reassemble to a single dataarray
        quantity = concat(*values, dim=dim)

    # Preserve attrs
    quantity.attrs = attrs

    return quantity


def concat(*objs, **kwargs):
    """Concatenate Quantity *objs*.

    Any strings included amongst *args* are discarded, with a logged warning;
    these usually indicate that a quantity is referenced which is not in the
    Reporter.
    """
    objs = filter_concat_args(objs)
    if Quantity.CLASS == "AttrSeries":
        # Silently discard any "dim" keyword argument
        kwargs.pop("dim", None)
        return pd.concat(objs, **kwargs)
    else:
        # Correct fill-values
        return xr.concat(objs, **kwargs)._sda.convert()


def disaggregate_shares(quantity, shares):
    """Disaggregate *quantity* by *shares*."""
    result = quantity * shares
    result.attrs["_unit"] = collect_units(quantity)[0]
    return result


def product(*quantities):
    """Return the product of any number of *quantities*."""
    # Iterator over (quantity, unit) tuples
    items = zip(quantities, collect_units(*quantities))

    # Initialize result values with first entry
    result, u_result = next(items)

    # Iterate over remaining entries
    for q, u in items:
        if Quantity.CLASS == "AttrSeries":
            # Work around pandas-dev/pandas#25760; see attrseries.py
            result = (result * q.align_levels(result)).dropna()
        else:
            result = result * q
        u_result *= u

    result.attrs["_unit"] = u_result

    return result


def ratio(numerator, denominator):
    """Return the ratio *numerator* / *denominator*.

    Parameters
    ----------
    numerator : .Quantity
    denominator : .Quantity
    """
    # Handle units
    u_num, u_denom = collect_units(numerator, denominator)

    result = numerator / denominator
    result.attrs["_unit"] = u_num / u_denom

    if Quantity.CLASS == "AttrSeries":
        result.dropna(inplace=True)

    return result


def select(qty, indexers, inverse=False):
    """Select from *qty* based on *indexers*.

    Parameters
    ----------
    qty : .Quantity
    indexers : dict (str -> list of str)
        Elements to be selected from *qty*. Mapping from dimension names to
        labels along each dimension.
    inverse : bool, optional
        If :obj:`True`, *remove* the items in indexers instead of keeping them.
    """
    if inverse:
        new_indexers = {}
        for dim, labels in indexers.items():
            new_indexers[dim] = list(
                filter(lambda l: l not in labels, qty.coords[dim].data)
            )
        indexers = new_indexers

    return qty.sel(indexers)


def sum(quantity, weights=None, dimensions=None):
    """Sum *quantity* over *dimensions*, with optional *weights*.

    Parameters
    ----------
    quantity : .Quantity
    weights : .Quantity, optional
        If *dimensions* is given, *weights* must have at least these
        dimensions. Otherwise, any dimensions are valid.
    dimensions : list of str, optional
        If not provided, sum over all dimensions. If provided, sum over these
        dimensions.
    """
    if weights is None:
        weights, w_total = 1, 1
    else:
        w_total = weights.sum(dim=dimensions)

    result = (quantity * weights).sum(dim=dimensions) / w_total
    result.attrs["_unit"] = collect_units(quantity)[0]

    return result


# Input and output
def load_file(path, dims={}, units=None):
    """Read the file at *path* and return its contents as a :class:`.Quantity`.

    Some file formats are automatically converted into objects for direct use
    in reporting code:

    :file:`.csv`:
       Converted to :class:`.Quantity`. CSV files must have a 'value' column;
       all others are treated as indices, except as given by `dims`. Lines
       beginning with '#' are ignored.

    Parameters
    ----------
    path : pathlib.Path
        Path to the file to read.
    dims : collections.abc.Collection or collections.abc.Mapping, optional
        If a collection of names, other columns besides these and 'value' are
        discarded. If a mapping, the keys are the column labels in `path`, and
        the values are the target dimension names.
    units : str or pint.Unit
        Units to apply to the loaded Quantity.
    """
    # TODO optionally cache: if the same Reporter is used repeatedly, then the
    #      file will be read each time; instead cache the contents in memory.
    if path.suffix == ".csv":
        data = pd.read_csv(path, comment="#")

        # Index columns
        index_columns = data.columns.tolist()
        index_columns.remove("value")

        try:
            # Retrieve the unit column from the file
            units_col = data.pop("unit").unique()
            index_columns.remove("unit")
        except KeyError:
            pass  # No such column; use None or argument value
        else:
            # Use a unique value for units of the quantity
            if len(units_col) > 1:
                raise ValueError(
                    f"Cannot load {path} with non-unique units " + repr(units_col)
                )
            elif units and units not in units_col:
                raise ValueError(
                    f"Explicit units {units} do not match " f"{units_col[0]} in {path}"
                )
            units = units_col[0]

        if len(dims):
            # Use specified dimensions
            if not isinstance(dims, Mapping):
                # Convert a list, set, etc. to a dict
                dims = {d: d for d in dims}

            # - Drop columns not mentioned in *dims*
            # - Rename columns according to *dims*
            data = data.drop(columns=set(index_columns) - set(dims.keys())).rename(
                columns=dims
            )
            index_columns = list(dims.values())

        return Quantity(data.set_index(index_columns)["value"], units=units)
    elif path.suffix in (".xls", ".xlsx"):
        # TODO define expected Excel data input format
        raise NotImplementedError  # pragma: no cover
    elif path.suffix == ".yaml":
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

    if path.suffix == ".csv":
        quantity.to_dataframe().to_csv(path)
    elif path.suffix == ".xlsx":
        quantity.to_dataframe().to_excel(path, merge_cells=False)
    else:
        path.write_text(quantity)
