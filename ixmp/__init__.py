import logging

from ._config import config
from ._version import get_versions
from .backend import BACKENDS, ItemType
from .backend.jdbc import JDBCBackend
from .core import IAMC_IDX, Platform, Scenario, TimeSeries
from .model import MODELS
from .model.dantzig import DantzigModel
from .model.gams import GAMSModel
from .reporting import Reporter
from .utils import show_versions

__all__ = [
    'IAMC_IDX',
    'ItemType',
    'Platform',
    'Reporter',
    'Scenario',
    'TimeSeries',
    'config',
    'log',
    'show_versions',
]

__version__ = get_versions()['version']
del get_versions

# Register Backends provided by ixmp
BACKENDS['jdbc'] = JDBCBackend

# Register Models provided by ixmp
MODELS.update({
    'default': GAMSModel,
    'gams': GAMSModel,
    'dantzig': DantzigModel,
})


# Configure the 'ixmp' logger: write messages to std out, defaulting to level
# WARNING and above
log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.WARNING)
