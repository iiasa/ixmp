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

from .reporter import Reporter
from .util import RENAME_DIMS

__all__ = [
    # ixmp-specific
    "RENAME_DIMS",
    "Reporter",
    # Re-exports from genno
    "ComputationError",
    "Key",
    "KeyExistsError",
    "MissingKeyError",
    "Quantity",
    "configure",
]


@genno.config.handles("filters", iterate=False)
def filters(c: Computer, filters: dict):
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


@genno.config.handles("rename_dims", iterate=False)
def rename_dims(c: Computer, info: dict):
    """Handle the entire ``rename_dims:`` config section."""
    RENAME_DIMS.update(info)


# keep=True is different vs. genno.config
@genno.config.handles("units", iterate=False, discard=False)
def units(c: Computer, info: dict):
    """Handle the entire ``units:`` config section."""
    genno.config.units(c, info)
