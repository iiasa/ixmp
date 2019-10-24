from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from ixmp.core import (  # noqa: E402,F401
    Platform,
    TimeSeries,
    Scenario,
)
from ixmp.reporting import Reporter  # noqa: F401
