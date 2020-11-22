import logging
from functools import lru_cache
from typing import Dict

import pandas as pd
import pint

from .key import Key

log = logging.getLogger(__name__)


#: Replacements to apply to quantity units before parsing by
#: :doc:`pint <pint:index>`. Mapping from original unit -> preferred unit.
REPLACE_UNITS = {
    "%": "percent",
}

#: Dimensions to rename when extracting raw data from Scenario objects.
#: Mapping from Scenario dimension name -> preferred dimension name.
RENAME_DIMS: Dict[str, str] = {}


def clean_units(input_string):
    """Tolerate messy strings for units.

    Handles two specific cases found in |MESSAGEix| test cases:

    - Dimensions enclosed in '[]' have these characters stripped.
    - The '%' symbol cannot be supported by pint, because it is a Python
      operator; it is translated to 'percent'.

    """
    input_string = input_string.strip("[]")
    for old, new in REPLACE_UNITS.items():
        input_string = input_string.replace(old, new)
    return input_string


def collect_units(*args):
    """Return an list of '_unit' attributes for *args*."""
    registry = pint.get_application_registry()

    for arg in args:
        if "_unit" in arg.attrs:
            # Convert units if necessary
            if isinstance(arg.attrs["_unit"], str):
                arg.attrs["_unit"] = registry.parse_units(arg.attrs["_unit"])
        else:
            log.debug("assuming {} is unitless".format(arg))
            arg.attrs["_unit"] = registry.parse_units("")

    return [arg.attrs["_unit"] for arg in args]


def dims_for_qty(data):
    """Return the list of dimensions for *data*.

    If *data* is a :class:`pandas.DataFrame`, its columns are processed;
    otherwise it must be a list.

    ixmp.reporting.RENAME_DIMS is used to rename dimensions.
    """
    if isinstance(data, pd.DataFrame):
        # List of the dimensions
        dims = data.columns.tolist()
    else:
        dims = list(data)

    # Remove columns containing values or units; dimensions are the remainder
    for col in "value", "lvl", "mrg", "unit":
        try:
            dims.remove(col)
        except ValueError:
            continue

    # Rename dimensions
    return [RENAME_DIMS.get(d, d) for d in dims]


def filter_concat_args(args):
    """Filter out str and Key from *args*.

    A warning is logged for each element removed.
    """
    for arg in args:
        if isinstance(arg, (str, Key)):
            log.warn("concat() argument {repr(arg)} missing; will be omitted")
            continue
        yield arg


@lru_cache(1)
def get_reversed_rename_dims():
    return {v: k for k, v in RENAME_DIMS.items()}


def parse_units(units_series):
    """Return a :class:`pint.Unit` for a :class:`pd.Series` of strings."""
    unit = pd.unique(units_series)

    if len(unit) > 1:
        raise ValueError(f"mixed units {list(unit)}")

    registry = pint.get_application_registry()

    # Helper method to return an intelligible exception
    def invalid(unit):
        chars = "".join(c for c in "-?$" if c in unit)
        msg = (
            f"unit {repr(unit)} cannot be parsed; contains invalid "
            f"character(s) {repr(chars)}"
        )
        return ValueError(msg)

    # Helper method to add unit definitions
    def define_unit_parts(expr):
        # Split possible compound units
        for part in expr.split("/"):
            try:
                registry.parse_units(part)
            except pint.UndefinedUnitError:
                # Part was unparseable; define it
                definition = f"{part} = [{part}]"
                log.info(f"Add unit definition: {definition}")

                # This line will fail silently for parts like 'G$' containing
                # characters like '$' that are discarded by pint
                registry.define(definition)

    # Parse units
    try:
        unit = clean_units(unit[0])
        unit = registry.parse_units(unit)
    except IndexError:
        # Quantity has no unit
        unit = registry.parse_units("")
    except pint.UndefinedUnitError:
        try:
            # Unit(s) do not exist; define them in the UnitRegistry
            define_unit_parts(unit)

            # Try to parse again
            unit = registry.parse_units(unit)
        except (pint.UndefinedUnitError, pint.RedefinitionError):
            # Handle the silent failure of define(), above; or
            # define_unit_parts didn't work
            raise invalid(unit)
    except AttributeError:
        # Unit contains a character like '-' that throws off pint
        # NB this 'except' clause must be *after* UndefinedUnitError, since
        #    that is a subclass of AttributeError.
        raise invalid(unit)

    return unit
