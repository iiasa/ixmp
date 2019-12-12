from ._version import get_versions
from ixmp.core import (  # noqa: F401
    IAMC_IDX,
    Platform,
    TimeSeries,
    Scenario,
)
from ._config import config  # noqa: F401
from .backend import BACKENDS
from .backend.jdbc import JDBCBackend
from .model import MODELS
from .model.gams import GAMSModel
from ixmp.reporting import Reporter  # noqa: F401

__version__ = get_versions()['version']
del get_versions

# Register Backends provided by ixmp
BACKENDS['jdbc'] = JDBCBackend

# Register Models provided by ixmp
MODELS.update({
    'default': GAMSModel,
    'gams': GAMSModel,
})
