import logging

from pkg_resources import DistributionNotFound, get_distribution

from ixmp._config import config
from ixmp.backend import BACKENDS, IAMC_IDX, ItemType
from ixmp.backend.jdbc import JDBCBackend
from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario, TimeSeries
from ixmp.model import MODELS
from ixmp.model.base import ModelError
from ixmp.model.dantzig import DantzigModel
from ixmp.model.gams import GAMSModel
from ixmp.reporting import Reporter
from ixmp.utils import show_versions

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
except DistributionNotFound:  # pragma: no cover
    # Package is not installed
    __version__ = "999"

# Register Backends provided by ixmp
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
