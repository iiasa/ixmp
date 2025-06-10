"""Types for type hinting and checking of :mod:`ixmp` and downstream code."""

import os
from collections.abc import Sequence
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Literal,
    Optional,
    TypedDict,
    Union,
)

# TODO Import from typing when dropping support for Python 3.10 (NotRequired)
# TODO Use type x = ... instead of TypeAlias when dropping support for Python 3.11
from typing_extensions import NotRequired, TypeAlias

from ixmp.backend.common import ItemType

if TYPE_CHECKING:
    from ixmp.core.scenario import Scenario
    from ixmp.core.timeseries import TimeSeries
    from ixmp.util.ixmp4 import ContainerData


ItemTypeNames: TypeAlias = Literal["set", "par", "equ", "var"]
ItemTypeFlags: TypeAlias = Literal[
    ItemType.PAR, ItemType.SET, ItemType.EQU, ItemType.VAR
]
VersionType: TypeAlias = Optional[Union[int, Literal["new"]]]


# NOTE Use this form to enable the key 'class'
BackendInitKwargs = TypedDict(
    "BackendInitKwargs", {"class": NotRequired[Union[Literal["jdbc", "ixmp4"], str]]}
)


class JDBCBackendInitKwargs(TypedDict):
    driver: NotRequired[Optional[str]]
    path: NotRequired[Optional[Union[str, Path]]]
    url: NotRequired[Optional[str]]
    user: NotRequired[Optional[str]]
    password: NotRequired[Optional[str]]


class IXMP4BackendInitKwargs(TypedDict):
    ixmp4_name: str
    dsn: NotRequired[str]
    jdbc_compat: NotRequired[Union[bool, str]]


# NOTE This would ideally inherit from IXMP4BackendInitKwargs, but ixmp4_name cannot be
# required here since it's not required for JDBC.__init__()
class PlatformInitKwargs(BackendInitKwargs, JDBCBackendInitKwargs):
    jvmargs: NotRequired[Optional[Union[str, list[str]]]]
    dbprops: NotRequired[Optional[os.PathLike[str]]]
    cache: NotRequired[bool]
    log_level: NotRequired[Optional[Union[int, str]]]
    ixmp4_name: NotRequired[str]
    dsn: NotRequired[str]
    jdbc_compat: NotRequired[Union[bool, str]]


class InitializeItemsKwargs(TypedDict):
    ix_type: Literal["set", "par", "equ", "var"]
    idx_sets: NotRequired[Optional[list[str]]]
    idx_names: NotRequired[Optional[list[str]]]


class GamsModelInitKwargs(TypedDict, total=False):
    model_file: os.PathLike[str]
    case: str
    in_file: os.PathLike[str]
    out_file: os.PathLike[str]
    solve_args: list[str]
    gams_args: list[str]
    check_solution: bool
    comment: Optional[str]
    equ_list: Optional[list[str]]
    var_list: Optional[list[str]]
    quiet: bool
    use_temp_dir: bool
    name_: Optional[str]
    record_version_packages: Sequence[str]
    container_data: list["ContainerData"]


class ScenarioInitKwargs(TypedDict, total=False):
    cache: bool
    with_data: bool


class ScenarioIdentifiers(TypedDict):
    """Identifiers of a Scenario."""

    model: str
    scenario: str


class ScenarioInfo(ScenarioIdentifiers):
    version: NotRequired[Optional[Union[int, Literal["new"]]]]


class PlatformInfo(TypedDict):
    name: NotRequired[str]


class WriteFiltersKwargs(TypedDict, total=False):
    scenario: "Scenario | TimeSeries | list[str]"
    model: list[str]
    variable: list[str]
    unit: list[str]
    region: list[str]
    default: bool
    export_all_runs: bool


class RunKwargs(TypedDict):
    filters: WriteFiltersKwargs
    record_version_packages: NotRequired[Sequence[str]]
    container_data: NotRequired[list["ContainerData"]]


class WriteKwargs(TypedDict, total=False):
    filters: WriteFiltersKwargs
    record_version_packages: Sequence[str]
    container_data: list["ContainerData"]
    max_row: Optional[int]


class ReadKwargs(TypedDict, total=False):
    filters: WriteFiltersKwargs
    firstyear: Optional[int]
    lastyear: Optional[int]
    add_units: bool
    init_items: bool
    commit_steps: bool
    check_solution: bool
    comment: str
    equ_list: list[str]
    var_list: list[str]


class TSReadFileKwargs(TypedDict, total=False):
    firstyear: Optional[int]
    lastyear: Optional[int]


class SReadFileKwargs(TypedDict, total=False):
    add_units: bool
    init_items: bool
    commit_steps: bool
