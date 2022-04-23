import logging
from itertools import zip_longest

import pandas as pd
import pint
from genno.core.quantity import Quantity
from genno.util import parse_units

from ixmp.reporting.util import RENAME_DIMS, dims_for_qty, get_reversed_rename_dims
from ixmp.utils import to_iamc_layout

log = logging.getLogger(__name__)


def data_for_quantity(ix_type, name, column, scenario, config):
    """Retrieve data from *scenario*.

    Parameters
    ----------
    ix_type : 'equ' or 'par' or 'var'
        Type of the ixmp object.
    name : str
        Name of the ixmp object.
    column : 'mrg' or 'lvl' or 'value'
        Data to retrieve. 'mrg' and 'lvl' are valid only for ``ix_type='equ'``,and
        'level' otherwise.
    scenario : ixmp.Scenario
        Scenario containing data to be retrieved.
    config : dict of (str -> dict)
        The key 'filters' may contain a mapping from dimensions to iterables of allowed
        values along each dimension. The key 'units'/'apply' may contain units to apply
        to the quantity; any such units overwrite existing units, without conversion.

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
    if isinstance(data, dict):  # pragma: no cover
        data = pd.DataFrame.from_records([data])

    # Warn if no values are returned.
    # TODO construct an empty Quantity with the correct dimensions *even if* no values
    #      are returned.
    if len(data) == 0:
        log.debug(f"0 values for {ix_type} {repr(name)} using filters: {repr(filters)}")
        log.debug("May be the cause of subsequent errors.")

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
    except KeyError:  # pragma: no cover
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
            f"{name}: replace units {attrs.get('_unit', '(none)')} with {new_unit}"
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


def map_as_qty(set_df: pd.DataFrame, full_set):
    """Convert *set_df* to a :class:`.Quantity`.

    For the MESSAGE sets named ``cat_*`` (see :ref:`message_ix:mapping-sets`)
    :meth:`ixmp.Scenario.set` returns a :class:`~pandas.DataFrame` with two columns:
    the *category* set (S1) elements and the *category member* set (S2, also required
    as the argument `full_set`) elements.

    map_as_qty converts such a DataFrame (*set_df*) into a Quantity with two
    dimensions. At the coordinates *(s₁, s₂)*, the value is 1 if *s₂* is mapped from
    *s₁*; otherwise 0.

    A category named 'all', containing all elements of `full_set`, is added
    automatically.

    See also
    --------
    .broadcast_map
    """
    set_from, set_to = set_df.columns
    names = [RENAME_DIMS.get(c, c) for c in set_df.columns]

    # Add an 'all' mapping
    set_df = pd.concat(
        [set_df, pd.DataFrame([("all", e) for e in full_set], columns=set_df.columns)]
    )

    # Add a value column
    set_df["value"] = 1

    return (
        set_df.set_index([set_from, set_to])["value"]
        .rename_axis(index=names)
        .pipe(Quantity)
    )


def store_ts(scenario, *data):
    """Store time series `data` on `scenario`.

    The data is stored using :meth:`.add_timeseries`; `scenario` is checked out and then
    committed.

    Parameters
    ----------
    scenario :
        Scenario on which to store data.
    data : pandas.DataFrame or pyam.IamDataFrame
        1 or more objects containing data to store. If :class:`pandas.DataFrame`, the
        data are passed through :func:`to_iamc_layout`.
    """
    # TODO tolerate invalid types/errors on elements of `data`, logging exceptions on
    #      level ERROR, then continue and commit anyway; add an optional parameter like
    #      continue_on_error=True to control this behaviour
    import pyam

    log.info(f"Store time series data on '{scenario.url}'")
    scenario.check_out()

    for order, df in enumerate(data):
        df = (
            df.as_pandas(meta_cols=False)
            if isinstance(df, pyam.IamDataFrame)
            else to_iamc_layout(df)
        )

        # Add the data
        log.info(f"  ← {len(df)} rows")
        scenario.add_timeseries(df)

    scenario.commit(f"Data added using {__name__}")


def update_scenario(scenario, *quantities, params=[]):
    """Update *scenario* with computed data from reporting *quantities*.

    Parameters
    ----------
    scenario : .Scenario
    quantities : .Quantity or pd.DataFrame
        If DataFrame, must be valid input to :meth:`.Scenario.add_par`.
    params : list of str, optional
        For every element of `quantities` that is a pd.DataFrame, the element of
        `params` at the same index gives the name of the parameter to update.
    """
    log.info(f"Update '{scenario.url}'")
    scenario.check_out()

    for order, (qty, par_name) in enumerate(zip_longest(quantities, params)):
        if not isinstance(qty, pd.DataFrame):
            # Convert a Quantity to a DataFrame
            par_name = qty.name
            new = qty.to_series().reset_index().rename(columns={par_name: "value"})
            new["unit"] = "{:~}".format(qty.attrs["_unit"])  # type: ignore [str-format]
            qty = new

        # Add the data
        log.info(f"  {repr(par_name)} ← {len(qty)} rows")
        scenario.add_par(par_name, qty)

    scenario.commit(f"Data added using {__name__}")
