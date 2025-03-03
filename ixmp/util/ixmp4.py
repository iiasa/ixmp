# TODO Import this from typing when dropping Python 3.11
from typing import TYPE_CHECKING, Any, Optional

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from ixmp.core.scenario import Scenario


# These are based on existing calls within ixmp
class WriteFiltersKwargs(TypedDict, total=False):
    scenario: "Scenario | list[str]"
    model: list[str]
    variable: list[str]
    unit: list[str]
    region: list[str]
    default: bool
    export_all_runs: bool


class WriteKwargs(TypedDict, total=False):
    filters: WriteFiltersKwargs
    record_version_packages: list[str]


class ReadKwargs(TypedDict, total=False):
    filters: WriteFiltersKwargs
    firstyear: Optional[Any]
    lastyear: Optional[Any]
    add_units: bool
    init_items: bool
    commit_steps: bool
    check_solution: bool
    comment: str
    equ_list: list[str]
    var_list: list[str]
