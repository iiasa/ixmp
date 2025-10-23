from typing import Any

import genno.config
from genno import (
    ComputationError,
    Computer,
    Key,
    KeyExistsError,
    MissingKeyError,
    Quantity,
    configure,
)

from ixmp.report import common

from .reporter import Reporter

__all__ = [
    # ixmp-specific
    "Reporter",
    # Re-exports from genno
    "ComputationError",
    "Key",
    "KeyExistsError",
    "MissingKeyError",
    "Quantity",
    "configure",
]


# TODO Remove once genno adds annotation
@genno.config.handles("filters", iterate=False)  # type: ignore[misc]
def filters(c: Computer, filters: dict[str, Any]) -> None:
    """Handle the entire ``filters:`` config section."""
    # Ensure a filters dict exists
    c.graph["config"].setdefault("filters", dict())

    if len(filters):
        # Store the new filters
        c.graph["config"]["filters"].update(filters)
    else:
        # Empty dictionary → clear all
        c.graph["config"]["filters"].clear()

    # Clear filters for specific dimensions only
    for key, value in filters.items():
        if value is None:
            c.graph["config"]["filters"].pop(key, None)


@genno.config.handles("rename_dims", iterate=False)  # type: ignore[misc]
def rename_dims(c: Computer, info: dict[str, str]) -> None:
    """Handle the entire ``rename_dims:`` config section."""
    common.RENAME_DIMS.update(info)


# keep=True is different vs. genno.config
@genno.config.handles("units", iterate=False, discard=False)  # type: ignore[misc]
def units(c: Computer, info: dict[str, str]) -> None:
    """Handle the entire ``units:`` config section."""
    genno.config.units(c, info)


def __getattr__(name: str) -> dict[str, str] | None:
    if name == "RENAME_DIMS":
        return common.RENAME_DIMS
    else:
        raise AttributeError(name)
