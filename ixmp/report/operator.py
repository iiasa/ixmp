import logging
from collections.abc import Mapping
from itertools import zip_longest
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

import genno
import pandas as pd
import pint
from genno.util import parse_units

from ixmp.core.timeseries import TimeSeries
from ixmp.report import common
from ixmp.util import to_iamc_layout

from .util import dims_for_qty, get_reversed_rename_dims

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from ixmp.core.scenario import Scenario

log = logging.getLogger(__name__)


def data_for_quantity(
    ix_type: Literal["equ", "par", "var"],
    name: str,
    column: Literal["mrg", "lvl", "value"],
    scenario: "Scenario",
    config: Mapping[str, Mapping],
) -> "AnyQuantity":
    """Retrieve data from `scenario`.

    Parameters
    ----------
    ix_type :
        Type of the ixmp object.
    name : str
        Name of the ixmp object.
    column :
        Data to retrieve. 'mrg' and 'lvl' are valid only for ``ix_type='equ'``,and
        'level' otherwise.
    scenario : ixmp.Scenario
        Scenario containing data to be retrieved.
    config :
        Configuration. The key 'filters' may contain a mapping from dimensions to
        iterables of allowed values along each dimension. The key 'units'/'apply' may
        contain units to apply to the quantity; any such units overwrite existing units,
        without conversion.

    Returns
    -------
    .Quantity
        Data for `name`.
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
        data = data.rename(columns=common.RENAME_DIMS).set_index(dims)

    # Convert to a Quantity, assign attributes and name
    qty = genno.Quantity(
        data[column], name=name + ("-margin" if column == "mrg" else ""), attrs=attrs
    )

    try:
        # Remove length-1 dimensions for scalars
        qty = qty.squeeze("index", drop=True)
    except (KeyError, ValueError):
        # KeyError if "index" does not exist; ValueError if its length is > 1
        pass

    return qty


# Non-weak references to objects to keep them alive
_FROM_URL_REF: set[Any] = set()


def from_url(url: str, cls=TimeSeries) -> "TimeSeries":
    """Return a :class:`.ixmp.TimeSeries` or subclass instance, given its `url`.

    Parameters
    ----------
    cls : type, optional
        Subclass to instantiate and return; for instance, :class:`.Scenario`.
    """
    ts, mp = cls.from_url(url)
    assert ts is not None
    _FROM_URL_REF.add(ts)
    _FROM_URL_REF.add(mp)
    return ts


def get_ts(
    ts: "TimeSeries",
    filters: Optional[dict] = None,
    iamc: bool = False,
    subannual: Union[bool, str] = "auto",
) -> pd.DataFrame:
    """Retrieve timeseries data from `ts`.

    Corresponds to :meth:`.TimeSeries.timeseries`.

    Parameters
    ----------
    filters :
        Names and values for the `region`, `variable`, `unit`, and `year` keyword
        arguments to :meth:`.timeseries`.
    """
    filters = filters or dict()

    return ts.timeseries(iamc=iamc, subannual=subannual, **filters)


def map_as_qty(set_df: pd.DataFrame, full_set) -> "AnyQuantity":
    """Convert `set_df` to a :class:`.Quantity`.

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
    ~genno.operator.broadcast_map
    """
    set_from, set_to = set_df.columns
    names = [common.RENAME_DIMS.get(c, c) for c in set_df.columns]

    # Add an 'all' mapping
    set_df = pd.concat(
        [set_df, pd.DataFrame([("all", e) for e in full_set], columns=set_df.columns)]
    )

    # Add a value column
    set_df["value"] = 1

    return (
        set_df.set_index([set_from, set_to])["value"]
        .rename_axis(index=names)
        .pipe(genno.Quantity)
    )


def remove_ts(
    ts: "TimeSeries",
    data: Optional[pd.DataFrame] = None,
    after: Optional[int] = None,
) -> None:
    """Remove all time series data from `ts`.

    Note that data stored with :meth:`.add_timeseries` using :py:`meta=True` as a
    keyword argument cannot be removed using :meth:`.TimeSeries.remove_timeseries`, and
    thus also not with this operator.

    Parameters
    ----------
    data : pandas.DataFrame, optional
        Specific data to be removed. If not given, all time series data is removed.
    after : int, optional
        If given, only data with `year` labels equal to or greater than `after` are
        removed.
    """
    if data is None:
        data = ts.timeseries().drop("value", axis=1)

    N = len(data)
    count = f"{N}"

    if after:
        query = f"{after} <= year"
        data = data.query(query)
        count = f"{len(data)} of {N} ({query})"

    log.info(f"Remove {count} rows of time series data from {ts.url}")

    # TODO improve TimeSeries.transact() to allow timeseries_only=True; use here
    ts.check_out(timeseries_only=True)
    try:
        ts.remove_timeseries(data)
    except Exception:  # pragma: no cover
        ts.discard_changes()
    else:
        ts.commit(f"Remove time series data ({__name__}.remove_ts)")


def store_ts(scenario, *data, strict: bool = False) -> None:
    """Store time series `data` on `scenario`.

    The data is stored using :meth:`.add_timeseries`; `scenario` is checked out and then
    committed.

    Parameters
    ----------
    scenario :
        Scenario on which to store data.
    data : pandas.DataFrame or pyam.IamDataFrame
        1 or more objects containing data to store. If :class:`pandas.DataFrame`, the
        data are passed through :func:`.to_iamc_layout`.
    strict: bool
        If :data:`True` (default :data:`False`), raise an exception if any of `data` are
        not successfully added. Otherwise, log on level :ref:`ERROR <python:levels>` and
        continue.
    """
    import pyam

    log.info(f"Store time series data on '{scenario.url}'")
    scenario.check_out(timeseries_only=True)

    for order, df in enumerate(data):
        df = (
            df.as_pandas(meta_cols=False)
            if isinstance(df, pyam.IamDataFrame)
            else to_iamc_layout(df)
        )

        # Add the data
        try:
            scenario.add_timeseries(df)
        except Exception as e:
            log.error(f"Failed with {e!r}:\n{df}")
            if strict:
                raise
        else:
            log.info(f"  ← {len(df)} rows")

    scenario.commit(f"Data added using {__name__}")


def update_scenario(scenario, *quantities, params=[]) -> None:
    """Update *scenario* with computed data from reporting *quantities*.

    Parameters
    ----------
    scenario : Scenario
    quantities : .Quantity or pandas.DataFrame
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
            qty = (
                qty.to_series()
                .reset_index()
                .rename(columns={par_name: "value"})
                .assign(unit=f"{qty.units:~}")
            )

        # Add the data
        log.info(f"  {repr(par_name)} ← {len(qty)} rows")
        scenario.add_par(par_name, qty)

    scenario.commit(f"Data added using {__name__}")
