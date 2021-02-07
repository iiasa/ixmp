import logging

import pandas as pd
import pint

from genno.compat.ixmp.util import RENAME_DIMS, dims_for_qty, get_reversed_rename_dims
from genno.core.quantity import Quantity
from genno.util import parse_units

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
