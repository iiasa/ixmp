import logging

from pkg_resources import DistributionNotFound, get_distribution

from ._config import config
from .backend import BACKENDS, ItemType
from .backend.dbapi import DatabaseBackend
from .backend.jdbc import JDBCBackend
from .core import IAMC_IDX, Platform, Scenario, TimeSeries
from .model import MODELS
from .model.base import ModelError
from .model.dantzig import DantzigModel
from .model.gams import GAMSModel
from .reporting import Reporter
from .utils import show_versions

__all__ = [
    "IAMC_IDX",
    "ItemType",
    "ModelError",
    "Platform",
    "Reporter",
    "Scenario",
    "TimeSeries",
    "config",
    "log",
    "show_versions",
]

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # Package is not installed
    __version__ = "999"

# Register Backends provided by ixmp
BACKENDS["dbapi"] = DatabaseBackend
BACKENDS["jdbc"] = JDBCBackend

# Register Models provided by ixmp
MODELS.update(
    {
        "default": GAMSModel,
        "gams": GAMSModel,
        "dantzig": DantzigModel,
    }
)


# Configure the 'ixmp' logger: write messages to stdout, defaulting to level WARNING
# and above
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.WARNING)
