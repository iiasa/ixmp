from functools import lru_cache, partial
from typing import TYPE_CHECKING, Literal, Union

import pandas as pd
from genno import Key

from ixmp.report import common

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from ixmp.core.scenario import Scenario


def dims_for_qty(data: Union[list[str], pd.DataFrame]) -> list[str]:
    """Return the list of dimensions for *data*.

    If *data* is a :class:`pandas.DataFrame`, its columns are processed;
    otherwise it must be a list.

    :data:`.RENAME_DIMS` is used to rename dimensions.
    """
    # List of the dimensions
    dims = data.columns.tolist() if isinstance(data, pd.DataFrame) else list(data)

    # Remove columns containing values or units; dimensions are the remainder
    for col in "value", "lvl", "mrg", "unit":
        try:
            dims.remove(col)
        except ValueError:
            continue

    # Rename dimensions
    return [common.RENAME_DIMS.get(d, d) for d in dims]


def keys_for_quantity(
    ix_type: Literal["par", "equ", "var"], name: str, scenario: "Scenario"
) -> list[tuple[Key, partial["AnyQuantity"], str, str]]:
    """Return keys for *name* in *scenario*."""
    from .operator import data_for_quantity

    # Retrieve names of the indices of the ixmp item, without loading the data
    dims = dims_for_qty(scenario.idx_names(name))

    # Column for retrieving data
    column: Literal["mrg", "lvl", "value"] = "value" if ix_type == "par" else "lvl"

    # A computation to retrieve the data
    result = [
        (
            Key(name, dims),
            partial(data_for_quantity, ix_type, name, column),
            "scenario",
            "config",
        )
    ]

    # Add the marginal values at full resolution, but no aggregates
    if ix_type == "equ":
        result.append(
            (
                Key(f"{name}-margin", dims),
                partial(data_for_quantity, ix_type, name, "mrg"),
                "scenario",
                "config",
            )
        )

    return result


@lru_cache(1)
def get_reversed_rename_dims() -> dict[str, str]:
    return {v: k for k, v in common.RENAME_DIMS.items()}


def __getattr__(name: str) -> dict[str, str]:
    if name == "RENAME_DIMS":
        return common.RENAME_DIMS
    else:
        raise AttributeError(name)
