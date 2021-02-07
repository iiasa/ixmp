import logging

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

log = logging.getLogger(__name__)

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


@genno.config.handles("rename_dims", type_=dict, apply=False)
def rename_dims(c: Computer, info):
    """Handle one entry from the ``rename_dims:`` config section."""
    RENAME_DIMS.update(info)
