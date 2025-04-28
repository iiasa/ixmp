import logging
import sys
from importlib.metadata import PackageNotFoundError, version

from ixmp._config import config
from ixmp.backend.common import IAMC_IDX, ItemType
from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario, TimeSeries
from ixmp.model.base import ModelError
from ixmp.report import Reporter
from ixmp.util import DeprecatedPathFinder, show_versions

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
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    # Package is not installed
    __version__ = "999"

# Install a finder that locates modules given their old/deprecated names
sys.meta_path.append(
    DeprecatedPathFinder(
        __package__,
        {
            r"reporting(\..*)?": r"report\1",
            "report.computations": "report.operator",
            r"utils(\..*)?": r"util\1",
        },
    )
)

# Configure the 'ixmp' logger: write messages to stdout, defaulting to level WARNING
# and above
log = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel(logging.WARNING)
log.addHandler(handler)
log.setLevel(logging.WARNING)


def __getattr__(name):
    if name == "utils":
        # Import via the old name to trigger DeprecatedPathFinder
        import ixmp.utils as util  # type: ignore [import-not-found]

        return util
    else:
        raise AttributeError(name)
