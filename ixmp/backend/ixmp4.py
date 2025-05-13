import logging
from collections.abc import Generator, Iterable, MutableMapping, Sequence
from dataclasses import asdict, dataclass
from os import PathLike
from pathlib import Path
from typing import Any, Literal, Optional, Union, cast, overload

import ixmp4.conf
import numpy as np
import pandas as pd
from ixmp4 import DataPoint
from ixmp4 import Platform as ixmp4_platform
from ixmp4.core import Run
from ixmp4.core.exceptions import NotUnique, PlatformNotFound
from ixmp4.core.optimization.equation import Equation, EquationRepository
from ixmp4.core.optimization.indexset import IndexSet, IndexSetRepository
from ixmp4.core.optimization.parameter import Parameter, ParameterRepository
from ixmp4.core.optimization.scalar import Scalar, ScalarRepository
from ixmp4.core.optimization.table import Table, TableRepository
from ixmp4.core.optimization.variable import Variable, VariableRepository
from ixmp4.data.backend.base import Backend as ixmp4_backend

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypeAlias, Unpack

from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries

from .base import CachingBackend, ReadKwargs, WriteKwargs
from .common import ItemType
from .ixmp4_io import read_gdx_to_run, write_run_to_gdx

log = logging.getLogger(__name__)


# TODO Reconcile this with `as_str_list()`
def _ensure_filters_values_are_lists(filters: dict[str, Union[Any, list[Any]]]) -> None:
    """Convert singular values in `filters` to lists."""
    for k, v in filters.items():
        if isinstance(v, dict):
            filters[k] = list(map(str, v))
        elif not isinstance(v, list):
            filters[k] = [v]


def _align_dtypes_for_filters(
    filters: dict[str, list[Any]], data: pd.DataFrame
) -> None:
    """Convert `filters` values to types enabling `data.isin()`."""
    # TODO Is this really the proper way to handle these types?
    TYPE_MAP = {"object": str, "int64": int}

    for column_name in filters.keys():
        # Guard against empty filters like {'time': []}
        if bool(filters[column_name]):
            # Guard against modifying already correct types
            if not isinstance(
                filters[column_name][0], TYPE_MAP[str(data.dtypes[column_name])]
            ):
                filters[column_name] = [
                    TYPE_MAP[str(data.dtypes[column_name])](value)
                    for value in filters[column_name]
                ]


def _remove_empty_lists(filters: dict[str, list[Any]]) -> dict[str, list[Any]]:
    """Remove keys from `filters` whose values are empty lists."""
    result: dict[str, list[Any]] = {}

    for k, v in filters.items():
        if filters[k] == []:
            continue
        else:
            result[k] = v

    return result


@dataclass
class Options:
    """Valid configuration options for :class:`IXMP4Backend`.

    If :attr:`dsn` is not given, it is automatically populated as a file named
    :file:`{ixmp4_name}.sqlite3` under the :mod:`ixmp4` local data path, for instance
    :file:`$HOME/.local/share/ixmp4/databases/`.
    """

    #: Name (also 'key') of the backend/platform in :mod:`ixmp4` configuration.
    ixmp4_name: str

    #: :mod:`ixmp4` data source name.
    dsn: str = ""

    #: If :any:`True`, populate the :class:`.IXMP4Backend` / :class:`.Platform` with
    #: certain units and regions that are automatically added to any
    #: :class:`.JDBCBackend`. These include:
    #:
    #: - Units: "???", "GWa", "USD/km", "USD/kWa", "cases", "kg", "km". Of these, only
    #:   "cases" and "km" are used by the :mod:`message_ix` tutorials.
    #: - Regions: "World", in hierarchy "common".
    jdbc_compat: bool = False

    def __post_init__(self):
        if not self.dsn:
            # Construct a DSN based on the platform name
            path = ixmp4.conf.settings.storage_directory.joinpath(
                "databases", f"{self.ixmp4_name}.sqlite3"
            )
            self.dsn = f"sqlite:///{path}"
        # Handle str value, e.g. from the command line
        if isinstance(self.jdbc_compat, str):
            val = self.jdbc_compat.title()
            self.jdbc_compat = bool(
                {"0": False, "False": False, "No": False, "True": True}.get(val, val)
            )

    @classmethod
    def handle_config(cls, args: Sequence, kwargs: MutableMapping) -> dict[str, Any]:
        """Helper for :meth:`IXMP4Backend.handle_config`."""
        if len(args):
            raise ValueError(f"Unhandled positional args to IXMP4Backend: {args!r}")

        try:
            return asdict(cls(**kwargs))
        except TypeError:
            raise ValueError(
                "Expected at least 'ixmp4_name' keyword argument to IXMP4Backend; got"
                f" {kwargs}"
            )

    @property
    def platform_info(self) -> "ixmp4.conf.base.PlatformInfo":
        """Return an ixmp4.PlatformInfo instance.

        This ensures that there is an entry in the :mod:`ixmp4` configuration with name
        :attr:`ixmp4_name`.

        - If the entry does not exist, it is created.
        - If the entry does exist, its DSN must be the same as :attr:`dsn`.
        """
        try:
            # Retrieve the info for an existing platform
            result = ixmp4.conf.settings.toml.get_platform(key=self.ixmp4_name)
            assert result.name == self.ixmp4_name and result.dsn == self.dsn
        except PlatformNotFound:
            # Add platform info to ixmp4 configuration
            ixmp4.conf.settings.toml.add_platform(self.ixmp4_name, self.dsn)
            # add_platform() returns None, so use get_platform() to retrieve the object
            result = ixmp4.conf.settings.toml.get_platform(key=self.ixmp4_name)
        except AssertionError:  # pragma: no cover
            log.error(f"From ixmp4: {result}")
            log.error(f"From ixmp: name={self.ixmp4_name!r}, dsn={self.dsn!r}")
            raise

        return result


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`.

    Keyword arguments are passed to :class:`Options`.
    """

    _platform: "ixmp4_platform"
    _backend: "ixmp4_backend"
    _options: "Options"

    # Mapping from ixmp.TimeSeries object to the underlying ixmp4.Run object (or
    # subclasses of either)
    index: dict[TimeSeries, "Run"] = {}

    # Mapping from platform name to ixmp4 Backend; this enables repeated __init__()
    # calls for in-memory DBs
    backend_index: dict[str, "ixmp4_backend"] = {}

    def __init__(
        self,
        *,
        ixmp4_name: str,
        dsn: str = Options.dsn,
        jdbc_compat: bool = Options.jdbc_compat,
    ) -> None:
        from ixmp4.data.backend import SqliteTestBackend

        # Handle arguments
        self._options = opts = Options(
            ixmp4_name=ixmp4_name, dsn=dsn, jdbc_compat=jdbc_compat
        )

        try:
            # Get an existing ixmp4.Backend object
            self._backend = self.backend_index[opts.ixmp4_name]
        except KeyError:
            # Object does not exist → instantiate using PlatformInfo from `opts`
            self._backend = self.backend_index[opts.ixmp4_name] = SqliteTestBackend(
                opts.platform_info
            )

            # Ensure database is set up.
            # NOTE sqlalchemy catches existing tables to avoid superfluous CREATEs
            self._backend.setup()

        # Instantiate an ixmp4.Platform using this ixmp4.Backend; store a reference
        self._platform = ixmp4_platform(_backend=self._backend)

        if opts.jdbc_compat:
            for u in "???", "GWa", "USD/km", "USD/kWa", "cases", "kg", "km":
                try:
                    self.set_unit(u, "For compatibility with ixmp.JDBCBackend")
                except NotUnique:
                    # Already exists
                    # NB The exception class is actually UnitNotUnique, but this is
                    #    created dynamically and is not importable
                    pass
            try:
                self.set_node(name="World", hierarchy="common")
            except NotUnique:
                pass

    @property
    def _log_level(self) -> int:
        """Log level for compatibility messages.

        Either :data:`logging.WARNING`, if :attr:`.Options.jdbc_compat` is :any:`False`,
        or else :data:`logging.NOTSET`.
        """
        return logging.NOTSET if self._options.jdbc_compat else logging.WARNING

    # def __del__(self) -> None:
    #     self.close_db()

    def set_log_level(self, level: int) -> None:
        # Set the level of the 'ixmp.backend.ixmp4' logger. Messages are handled by the
        # 'ixmp' logger; see ixmp/__init__.py.
        log.setLevel(level)

    # def get_log_level(self):
    #     return super().get_log_level()

    # Platform methods
    @classmethod
    def handle_config(cls, args, kwargs):
        return Options.handle_config(args, kwargs)

    # def close_db(self) -> None:
    #     self._backend.close()

    # Modifying the Platform

    def add_scenario_name(self, name: str) -> None:
        self._platform.scenarios.create(name=name)

    # TODO clarify: ixmp4.Run doesn't have a name, but is the new ixmp.Scenario
    # should it have a name or are these scenario names okay?
    def get_scenario_names(self) -> Generator[str, None, None]:
        for scenario in self._platform.scenarios.list():
            yield scenario.name

    def add_model_name(self, name: str) -> None:
        self._platform.models.create(name=name)

    def get_model_names(self) -> Generator[str, None, None]:
        for model in self._platform.models.list():
            yield model.name

    def get_scenarios(
        self, default: bool, model: Optional[str], scenario: Optional[str]
    ):
        runs = self._platform.runs.list(
            default_only=default,
            model={"name": model} if model else None,
            scenario={"name": scenario} if scenario else None,
        )

        for run in runs:
            yield [
                run.model.name,
                run.scenario.name,
                # TODO What are we going to use for scheme in ixmp4?
                "IXMP4Run",
                run.is_default,
                # TODO Change this from being hardcoded
                False,
                # TODO Expose the creation, update and lock info in ixmp4
                # (if we get lock info)
                "Some user",
                "Some date",
                "Some user",
                "Some date",
                "Some user",
                "Some date",
                # TODO Should Runs get .docs?
                "Some docs",
                run.version,
            ]

    def set_unit(self, name: str, comment: str) -> None:
        self._platform.units.create(name=name).docs = comment

    def get_units(self) -> list[str]:
        return [unit.name for unit in self._platform.units.list()]

    def set_node(
        self,
        name: str,
        parent: Optional[str] = None,
        hierarchy: Optional[str] = None,
        synonym: Optional[str] = None,
    ) -> None:
        if parent:
            log.warning(f"Discarding parent parameter {parent}; unused in ixmp4.")
        if synonym:
            log.warning(f"Discarding synonym parameter {synonym}; unused in ixmp4.")
        if hierarchy is None:
            log.warning(
                "IXMP4Backend.set_node() requires to specify 'hierarchy'! "
                "Using 'None' as a (meaningless) default.",
            )
            hierarchy = "None"
        self._platform.regions.create(name=name, hierarchy=hierarchy)

    def get_nodes(self) -> list[tuple[str, None, str, str]]:
        return [
            (region.name, None, region.name, region.hierarchy)
            for region in self._platform.regions.list()
        ]

    # Modifying the Run object

    def _index_and_set_attrs(self, run: "Run", ts: TimeSeries) -> None:
        """Add `run` to index and update attributes of `ts`.

        Helper for `init()` and `get()`.
        """
        # Add to index
        self.index[ts] = run

        # Retrieve the version of the ixmp4.Run object
        v = run.version
        if ts.version is None:
            # The default version was requested; update the attribute
            ts.version = v
        elif v != ts.version:  # pragma: no cover
            # Something went wrong on the ixmp4 side
            raise RuntimeError(f"got version {v} instead of {ts.version}")

        if isinstance(ts, Scenario):
            # Retrieve the `scheme` attribute from the Run.meta dict
            ts.scheme = str(run.meta.get("_ixmp_scheme", "")) or ts.scheme

    def init(self, ts: TimeSeries, annotation: str) -> None:
        run = self._platform.runs.create(model=ts.model, scenario=ts.scenario)
        with run.transact("Store ixmp.TimeSeries annotation, ixmp.Scenario.scheme"):
            run.meta["_ixmp_annotation"] = annotation
            if isinstance(ts, Scenario):
                run.meta["_ixmp_scheme"] = ts.scheme
        self._index_and_set_attrs(run, ts)

    def clone(
        self,
        s: Scenario,
        platform_dest: Platform,
        model: str,
        scenario: str,
        annotation: str,
        keep_solution: bool,
        first_model_year: Optional[int] = None,
    ) -> Scenario:
        # TODO either do log.warning that annotation is unused or
        # run.docs = annotation
        # TODO Should this be supported?
        if first_model_year:
            log.warning(
                "ixmp4-backed Scenarios don't support cloning from "
                "`first_model_year` only!"
            )
        # TODO Is this enough? ixmp4 doesn't support cloning to a different platform at
        # the moment, but maybe we can imitate this here? (Access
        # platform_dest.backend._platform to create a new Run?)
        cloned_run = self.index[s].clone(
            model=model, scenario=scenario, keep_solution=keep_solution
        )
        # Instantiate same class as the original object
        cloned_s = s.__class__(
            platform_dest,
            model,
            scenario,
            version=cloned_run.version,
            scheme=s.scheme,
        )
        self._index_and_set_attrs(cloned_run, cloned_s)
        return cloned_s

    def get(self, ts: TimeSeries) -> None:
        v = int(ts.version) if ts.version else None
        run = self._platform.runs.get(model=ts.model, scenario=ts.scenario, version=v)
        self._index_and_set_attrs(run, ts)

    def check_out(self, ts: TimeSeries, timeseries_only: bool) -> None:
        log.log(
            self._log_level, "ixmp4 backed Scenarios/Runs don't need to be checked out!"
        )

    def discard_changes(self, ts: TimeSeries) -> None:
        log.warning(
            self._log_level,
            "ixmp4 backed Scenarios/Runs are changed immediately, can't "
            "discard changes. To avoid the need, be sure to start work on "
            "fresh clones.",
        )

    def commit(self, ts: TimeSeries, comment: str) -> None:
        log.log(
            self._log_level,
            "ixmp4 backed Scenarios/Runs are changed immediately, "
            "no need for a commit!",
        )

    def clear_solution(self, s: Scenario, from_year: Optional[int] = None) -> None:
        if from_year:
            log.warning(
                "ixmp4 does not support removing the solution only after a certain year"
            )
            # This is required for compatibility with JDBC
            if type(s) is not Scenario:
                raise TypeError(
                    "s_clear_solution(from_year=...) only valid for ixmp.Scenario; not "
                    "subclasses"
                )
        self.index[s].optimization.remove_solution()

    def set_as_default(self, ts: TimeSeries) -> None:
        self.index[ts].set_as_default()

    # Information about the Run

    def run_id(self, ts: TimeSeries) -> int:
        # TODO is the Run.version really what this function is after?
        return self.index[ts].version

    def is_default(self, ts: TimeSeries) -> bool:
        return self.index[ts].is_default

    def has_solution(self, s: Scenario) -> bool:
        return self.index[s].optimization.has_solution()

    # Handle optimization items

    Ixmp4ItemType: TypeAlias = Literal["scalar", "indexset", "set", "par", "equ", "var"]

    @overload
    def _get_repo(self, s: Scenario, type: Literal["scalar"]) -> ScalarRepository: ...

    @overload
    def _get_repo(
        self, s: Scenario, type: Literal["indexset"]
    ) -> IndexSetRepository: ...

    @overload
    def _get_repo(self, s: Scenario, type: Literal["set"]) -> TableRepository: ...

    @overload
    def _get_repo(self, s: Scenario, type: Literal["par"]) -> ParameterRepository: ...

    @overload
    def _get_repo(self, s: Scenario, type: Literal["equ"]) -> EquationRepository: ...

    @overload
    def _get_repo(self, s: Scenario, type: Literal["var"]) -> VariableRepository: ...

    # NOTE Type hints here ensure the function body is checked
    def _get_repo(
        self, s: Scenario, type: Ixmp4ItemType
    ) -> Union[
        IndexSetRepository,
        TableRepository,
        ScalarRepository,
        ParameterRepository,
        EquationRepository,
        VariableRepository,
    ]:
        """Return the repository of items of `type` belonging to `s`."""
        if type == "scalar":
            return self.index[s].optimization.scalars
        if type == "indexset":
            return self.index[s].optimization.indexsets
        if type == "set":
            return self.index[s].optimization.tables
        elif type == "par":
            return self.index[s].optimization.parameters
        elif type == "equ":
            return self.index[s].optimization.equations
        else:  # "var"
            return self.index[s].optimization.variables

    def init_item(
        self,
        s: Scenario,
        type: Literal["set", "par", "equ", "var"],
        name: str,
        idx_sets: Sequence[str],
        idx_names: Optional[Sequence[str]],
    ) -> None:
        # TODO how are scalars created? Scalars require a value in ixmp4
        # In base::item_get_elements, it sounds like "equ" and "var" can also target
        # scalars, whereas below, inspired from jdbc, I'm only linking "par" to scalars
        if type == "set" and len(idx_sets) == 0:
            indexset_repo = self._get_repo(s=s, type="indexset")
            indexset_repo.create(name=name)
        elif type == "par" and len(idx_sets) == 0:
            # NOTE ixmp4 requires scalars to get value and unit upon creation, so the
            # shim does that when `item_set_elements()` targets a scalar
            return None
        else:
            repo = self._get_repo(s=s, type=type)
            repo.create(
                name=name,
                constrained_to_indexsets=list(idx_sets),
                column_names=list(idx_names) if idx_names else None,
            )

    def list_items(self, s: Scenario, type: Literal["set", "par", "equ"]) -> list[str]:
        if type == "set":
            indexset_repo = self._get_repo(s=s, type="indexset")
            set_repo = self._get_repo(s=s, type=type)
            return [item.name for item in indexset_repo.list()] + [
                item.name for item in set_repo.list()
            ]
        else:
            repo = self._get_repo(s=s, type=type)
            return [item.name for item in repo.list()]

    def _find_item(
        self,
        s: Scenario,
        name: str,
        types: tuple[Ixmp4ItemType, ...] = (
            "scalar",
            "indexset",
            "set",
            "par",
            "equ",
            "var",
        ),
    ) -> tuple[
        Ixmp4ItemType,
        Union[
            "Scalar",
            "IndexSet",
            "Table",
            "Equation",
            "Variable",
            "Parameter",
        ],
    ]:
        """Find item `name` in Scenario `s`.

        Parameters
        ----------
        s : Scenario
            The Scenario in which to search for `name`.
        name : str
            The name to search for.
        types : tuple[`Ixmp4ItemType`, ...], optional
            If provided, search only specific types for `name`. For example,
            types=("indexset", "set"). Default: tuple of all types.

        Returns
        -------
        tuple[`Ixmp4ItemType`, Scalar | IndexSet | Table | Parameter | Equation | Variable]
            A tuple containing the string item type and the item itself.

        Raises
        ------
        KeyError
            If no item with `name` and type in `types` can be found in `s`.
        """  # noqa: E501
        # NOTE this currently assumes that `name` will only be present once among
        # Tables, Parameters, Equations, Variables. This is in line with the assumption
        # made in the Java backend, but ixmp4 enforces no such constraint.
        _type: Optional[Literal["scalar", "indexset", "set", "par", "equ", "var"]] = (
            None
        )
        _item: Optional[
            Union[
                "Scalar",
                "IndexSet",
                "Table",
                "Equation",
                "Variable",
                "Parameter",
            ]
        ] = None
        for type in types:
            repo = self._get_repo(s=s, type=type)
            item_list = repo.list(name=name)
            if (
                len(item_list) == 1
            ):  # ixmp4 enforces names to be unique among individual item classes
                _type = type
                _item = item_list[0]
                break

        if _item is None or _type is None:
            raise KeyError(f"No item called '{name}' found on this Scenario!")
        else:
            return (_type, _item)

    @overload
    def _get_item(self, s: Scenario, name: str, type: Literal["scalar"]) -> Scalar: ...

    @overload
    def _get_item(
        self, s: Scenario, name: str, type: Literal["indexset"]
    ) -> IndexSet: ...

    @overload
    def _get_item(self, s: Scenario, name: str, type: Literal["set"]) -> Table: ...

    @overload
    def _get_item(self, s: Scenario, name: str, type: Literal["par"]) -> Parameter: ...

    @overload
    def _get_item(self, s: Scenario, name: str, type: Literal["equ"]) -> Equation: ...

    @overload
    def _get_item(self, s: Scenario, name: str, type: Literal["var"]) -> Variable: ...

    def _get_item(
        self, s: Scenario, name: str, type: Ixmp4ItemType
    ) -> Union[
        "Scalar",
        "IndexSet",
        "Table",
        "Equation",
        "Variable",
        "Parameter",
    ]:
        """Retrieve item `name` with `type` from `s`.

        Returns
        -------
        Scalar | IndexSet | Table | Parameter | Equation | Variable
            Based on `type`.
        """
        return self._get_repo(s=s, type=type).get(name=name)

    def _get_indexset_or_table(
        self, s: Scenario, name: str
    ) -> Union["IndexSet", "Table"]:
        """Get `name` from Scenario `s`.

        Try first if `name` is an IndexSet. Get it as a Table if it isn't.
        """
        try:
            indexset_repo = self._get_repo(s=s, type="indexset")
            return indexset_repo.get(name=name)
        except IndexSet.NotFound:
            try:
                table_repo = self._get_repo(s=s, type="set")
                return table_repo.get(name=name)
            except Table.NotFound as e:
                raise KeyError from e

    def item_index(
        self, s: Scenario, name: str, sets_or_names: Literal["sets", "names"]
    ) -> list[str]:
        _, item = self._find_item(s=s, name=name)
        # NOTE Using isinstance allows adequate attribute access
        if (
            isinstance(item, IndexSet)
            or isinstance(item, Scalar)
            or (isinstance(item, (Variable, Equation)) and item.indexset_names is None)
        ):
            return cast(list[str], [])
        else:
            if sets_or_names == "names" and item.column_names is None:
                log.debug(
                    f"Requested {sets_or_names}, but these are None for item "
                    f"{item.name}, falling back on (Index)Set names!"
                )
            assert item.indexset_names  # Could only be None for Variables & Equations
            return (
                item.column_names
                if item.column_names and sets_or_names == "names"
                else item.indexset_names
            )

    def _add_data_to_set(
        self,
        s: Scenario,
        name: str,
        key: Union[str, list[str]],
        comment: Optional[str] = None,
    ) -> None:
        """Add data `key` to `name` in Scenario `s`.

        Parameters
        ----------
        s : Scenario
            The Scenario hosting the item.
        name : str
            The name of the item to add data to.
        key : str | list[str]
            The data to add.

            ATTENTION: if `key` is a str, we're assuming `name` means an IndexSet;
            if `key` is a list, we're assuming `name` means a Table.
        comment: str, optional
            A message to store with the data addition. Unused by ixmp4.
        """
        if comment:
            log.warning(
                "`comment` currently unused with ixmp4 when adding data to Tables."
            )
        # Assumption: if key is just one value, we're dealing with an IndexSet
        # NOTE E.g. westeros_addon_technologies in message_ix calls
        # `scenario.add_set("addon", "po_turbine")` for a 1-D Table called "addon". This
        # only works now because ixmp.Scenario.add_set() converts str keys for Tables to
        # [key] before adding them. We would need to replicate that or adapt the
        # decision logic here should we drop ixmp.Scenario.
        if isinstance(key, str):
            # NOTE ixmp_source silently ignores duplicate data; replicate here
            # This could be improved by adding data without loading the indexset first,
            # but this requires users to ensure their data are valid
            indexset = self.index[s].optimization.indexsets.get(name=name)
            if key not in indexset.data:
                indexset.add(key)
        else:
            table = self.index[s].optimization.tables.get(name=name)
            # TODO should we enforce in ixmp4 that when constrained_to_indexsets
            # contains duplicate values, column_names must be provided?
            keys = table.column_names or table.indexset_names
            data_to_add = pd.DataFrame({keys[i]: [key[i]] for i in range(len(key))})

            # Silently ignore duplicate data, see NOTE above
            data_to_add = data_to_add[~data_to_add.isin(values=table.data).all(axis=1)]

            table.add(data=data_to_add)

    def _create_scalar(
        self,
        s: Scenario,
        name: str,
        value: float,
        unit: Optional[str],
        comment: Optional[str] = None,
    ) -> None:
        """Create the Scalar `name` in Scenario `s`.

        Parameters
        ----------
        s : Scenario
            The Scenario hosting the Scalar.
        name : str
            The name of the Scalar.
        value : float
            The value of the Scalar.
        unit : str, optional
            The unit of the Scalar.
        comment: str, optional
            A message to explain what this Scalar means.
        """
        scalar = self.index[s].optimization.scalars.create(
            name=name, value=value, unit=unit
        )
        if comment:
            scalar.docs = comment

    def _add_data_to_parameter(
        self,
        s: Scenario,
        name: str,
        key: Union[str, list[str]],
        value: float,
        unit: str,
        comment: Optional[str] = None,
    ) -> None:
        """Add data `key` to the Parameter `name` in Scenario `s`.

        Parameters
        ----------
        s : Scenario
            The Scenario hosting the Parameter.
        name : str
            The name of the Parameter.
        key : str | list[str]
            The data to add to the Parameter.
        value : float
            The value of the Parameter.
        unit : str
            The unit of the Parameter.
        comment: str, optional
            A message to store with the data addition. Unused by ixmp4.
        """
        if comment:
            log.warning(
                "`comment` currently unused with ixmp4 when adding data to Parameters."
            )
        parameter = self.index[s].optimization.parameters.get(name=name)
        # TODO there's got to be a better way for handling possible lists
        if isinstance(key, str):
            key = [key]

        keys = parameter.column_names or parameter.indexset_names
        data_to_add: dict[str, Union[list[float], list[str]]] = {
            keys[i]: [key[i]] for i in range(len(key))
        }
        data_to_add["values"] = [value]
        data_to_add["units"] = [unit]

        parameter.add(data=data_to_add)

    def item_set_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        elements: Iterable[tuple[Any, Optional[float], Optional[str], Optional[str]]],
    ) -> None:
        for key, value, unit, comment in elements:
            if type == "set":
                self._add_data_to_set(s=s, name=name, key=key, comment=comment)
            else:
                if key is None:
                    assert isinstance(value, float), (
                        "Creating a Scalar requires a value!"
                    )
                    self._create_scalar(
                        s=s, name=name, value=value, unit=unit, comment=comment
                    )
                else:
                    assert isinstance(value, float), (
                        "Adding data to a Parameter requires a value!"
                    )
                    assert isinstance(unit, str), (
                        "Adding data to a Parameter requires a unit!"
                    )
                    self._add_data_to_parameter(
                        s=s, name=name, key=key, value=value, unit=unit, comment=comment
                    )

    def _get_set_data(
        self, s: Scenario, name: str, filters: Optional[dict[str, list[Any]]] = None
    ) -> Union[pd.Series, pd.DataFrame]:
        """Get the data stored in `name` in `s`.

        Parameters
        ----------
        s : Scenario
            The Scenario hosting the item.
        name : str
            The name of the item.
        filters : dict[str, list[Any]], optional
            Filters to apply to the data. If present, return only matching data.
            Default: None.
        """
        item = self._get_indexset_or_table(s=s, name=name)

        if isinstance(item, Table):
            columns = item.column_names or item.indexset_names
            df = pd.DataFrame(item.data, columns=columns)
            return (
                df[df.isin(values=filters)[filters.keys()].all(axis=1)]
                if filters
                else df
            )
        else:
            series = pd.Series(item.data)
            return series[series.isin(values=filters[name])] if filters else series

    def item_get_elements(
        self,
        s: Scenario,
        type: Literal["equ", "par", "set", "var"],
        name: str,
        filters: Optional[dict[str, Any]] = None,
    ) -> Union[dict[str, Any], pd.Series, pd.DataFrame]:
        if type == "set":
            if filters:
                _ensure_filters_values_are_lists(filters=filters)
            return self._get_set_data(s=s, name=name, filters=filters)
        # TODO this is not handling scalars at the moment, but maybe try with type,
        # except NotFound, try scalar?
        else:
            item = self._get_item(s=s, name=name, type=type)

            # TODO What about Scalars? How does message_ix load them?

            data = pd.DataFrame(item.data)

            # NOTE the ixmp.report(er) expects these names, unfortunately; see
            # ixmp.report.util::keys_for_quantity()
            renames = (
                {"values": "value", "units": "unit"}
                if type == "par"
                else {"levels": "lvl", "marginals": "mrg"}
            )

            if data.empty:
                # Set correct columns
                columns = item.column_names or item.indexset_names
                unindexed_columns = (
                    ["value", "unit"] if type == "par" else ["lvl", "mrg"]
                )
                if columns:
                    columns.extend(unindexed_columns)

                # NOTE bool(item.indexsets) == True for all Parameters (otherwise, we're
                # talking Scalars). Empty scalar Equations/Variables expect float-NaNs.
                return (
                    pd.DataFrame(
                        columns=columns if bool(columns) else unindexed_columns
                    )
                    if bool(item.indexset_names)
                    else {key: np.nan for key in renames.values()}
                )

            # Apply filters if requested
            if filters:
                # isin() won't consider int(700) to be in ['700'], etc
                _ensure_filters_values_are_lists(filters=filters)
                _align_dtypes_for_filters(filters=filters, data=data)
                filters = _remove_empty_lists(filters=filters)

                if filters:
                    data = data[
                        data.isin(values=filters)[filters.keys()].all(axis=1)
                    ].reset_index(drop=True)

            # Rename columns
            data.rename(columns=renames, inplace=True)

            # For scalar items, return dict for compatibility with JDBC
            if not bool(item.indexset_names):
                return {key: data[key].values[0] for key in renames.values()}

            return data

    def item_delete_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        keys: Iterable[Sequence[str]],
    ) -> None:
        if type == "set":
            item = self._get_indexset_or_table(s=s, name=name)

            if isinstance(item, IndexSet):
                # NOTE We might have to expose IndexSet._data_type to cast correctly
                data = pd.DataFrame(keys, columns=[item.name])
                item.remove(data=cast(list[str], data[item.name].to_list()))
            else:
                # TODO can we assume that keys follow same order as indexsets/columns?
                columns = item.column_names or item.indexset_names
                data = pd.DataFrame(keys, columns=columns)
                item.remove(data=data)
        else:
            parameter = self._get_item(s=s, name=name, type="par")
            columns = parameter.column_names or parameter.indexset_names
            data = pd.DataFrame(keys, columns=columns)
            parameter.remove(data=data)

    def delete_item(
        self,
        s: Scenario,
        type: Literal["set", "par", "equ", "indexset"],
        name: str,
    ) -> None:
        # NOTE We need to allow 'set' and 'indexset' for type for backward compatibility
        if type == "set":
            # type will always be either "indexset" or "set"
            type, _ = self._find_item(s=s, name=name, types=("indexset", "set"))  # type: ignore[assignment]

        repo = self._get_repo(s=s, type=type)
        repo.delete(item=name)

    # NOTE The name 'cat_`name`' is used for backward compatibility with the JDBC, where
    # such names are hardcoded. 'cat' means 'category' and should be expanded for
    # clarity in the future.
    def cat_set_elements(
        self,
        ms: Scenario,
        name: str,
        cat: str,
        keys: Union[str, Sequence[str]],
        is_unique: bool,
    ) -> None:
        """Add data to a category mapping.

        For the ixmp4.Table or IndexSet `name`, define a category as a new IndexSet
        called 'type_`name`' (if it doesn't exist already) and add `cat` to it. Then,
        define a new Table 'cat_`name`' storing one column for `keys` and one for
        'categories'.

        Parameters
        ----------
        name : str
            Name of the category mapping Table.
        cat : str
            Name of the category within `name`.
        keys : iterable of str or list of str
            Keys to add to `cat`.
        is_unique : bool
            If :obj:`True`:

            - `keys` **must** contain only one key.
            - The Backend **must** remove any existing member of `cat`, so that it has
              only one element.
        """
        column_name: Optional[str] = None

        # Categories can be based on IndexSets directly or on 1-d Tables
        try:
            # Most should be based on IndexSets, try that first
            indexset = self.index[ms].optimization.indexsets.get(name=name)
            indexset_name = indexset.name
        except IndexSet.NotFound:
            # We're dealing with a Table
            table = self.index[ms].optimization.tables.get(name=name)

            # Ensure the Table's dimensions are correct before setting variables
            assert len(table.indexset_names) == 1
            indexset_name = table.indexset_names[0]
            column_name = table.name

        # Special treatment for 'technology' for historical reasons:
        if name == "technology":
            name = "tec"
        # NOTE Of the default items, all categories except 'cat_addon' are based on
        # IndexSets. 'cat_addon' is based on the 1-d Table 'addon', which is why it also
        # doesn't follow the naming convention. Usually, the column names are
        # 'type_<name>' and '<name>', but for 'cat_addon', it's 'type_addon' and
        # 'technology_addon'.
        elif name == "addon":
            column_name = "technology_addon"

        # Get or create the 'type_name' indexset and 'cat_name' table
        try:
            category_indexset = self.index[ms].optimization.indexsets.get(
                name=f"type_{name}"
            )
        except IndexSet.NotFound:
            category_indexset = self.index[ms].optimization.indexsets.create(
                name=f"type_{name}"
            )

        try:
            category_table = self.index[ms].optimization.tables.get(name=f"cat_{name}")
        except Table.NotFound:
            category_table = self.index[ms].optimization.tables.create(
                name=f"cat_{name}",
                constrained_to_indexsets=[indexset_name, category_indexset.name],
                column_names=[column_name, category_indexset.name]
                if column_name
                else None,
            )

        # Convert for convenience
        if isinstance(keys, str):
            keys = [keys]

        # Ensure proper treatment when is_unique is True
        if is_unique:
            if len(keys) > 1:
                raise ValueError("One can only add one element to a unique category!")
            # Ensure data contains no data except that which we're going to add
            # NOTE if category_table contains data linked to elements existing now, this
            # will lead to DataValidationErrors when adding data to the table. Also,
            # ixmp4 might safeguard against this when implementing remove() functions
            if category_indexset.data:
                # Remove all existing data so that only the single provided element will
                # be stored in the indexset
                category_indexset.remove(category_indexset.data)

        # Add data to both objects
        if cat not in category_indexset.data:
            category_indexset.add(data=cat)
        data = {column_name: keys} if column_name else {indexset_name: keys}
        data[category_indexset.name] = [cat] * len(keys)

        category_table.add(data=data)

    # TODO In cat_set_elements, we change e.g. cat_technology to cat_tec. Do we need the
    # same here or do we expect user code to call this with name == "tec" if they're
    # interested in "technology"?
    def cat_get_elements(self, ms: Scenario, name: str, cat: str) -> list[str]:
        # NOTE ixmp_source treats "all" this way
        if cat == "all":
            return list(
                map(str, self.index[ms].optimization.indexsets.get(name=name).data)
            )

        # Special treatment for 'technology' for historical reasons:
        if name == "technology":
            name = "tec"
        data = pd.DataFrame(
            self.index[ms].optimization.tables.get(name=f"cat_{name}").data
        )

        if data.empty:
            return []

        # This assumes there are only two columns: 'type_{name}' and the category name
        columns = data.columns.to_list()
        columns.remove(f"type_{name}")

        return data[data[f"type_{name}"] == cat][columns[0]].astype(str).to_list()

    def cat_list(self, ms: Scenario, name: str) -> list[str]:
        # Special treatment for 'technology' for historical reasons:
        if name == "technology":
            name = "tec"
        category_indexset = self.index[ms].optimization.indexsets.get(f"type_{name}")
        return [str(item) for item in category_indexset.data]

    def set_data(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        data: dict[int, float],
        unit: str,
        subannual: str,
        meta: bool,
    ) -> None:
        log.warning("Parameter `meta` for set_data() currently unused by ixmp4!")
        log.warning("Parameter `subannual` for set_data() currently unused by ixmp4!")

        # Construct dataframe as ixmp4 expects it
        years = data.keys()
        values = data.values()
        number_of_years = len(years)
        regions = [region] * number_of_years
        variables = [variable] * number_of_years
        units = [unit] * number_of_years
        _data = list(zip(years, values, regions, variables, units))

        # Add timeseries dataframe
        # TODO Is this really the only type we're interested in?
        run = self.index[ts]
        with run.transact(f"set_data() for Run {run.id}"):
            run.iamc.add(
                pd.DataFrame(
                    _data, columns=["step_year", "value", "region", "variable", "unit"]
                ),
                type=DataPoint.Type.ANNUAL,
            )

    def get_data(
        self,
        ts: TimeSeries,
        region: Sequence[str],
        variable: Sequence[str],
        unit: Sequence[str],
        year: Sequence[str],
    ) -> Generator[tuple, Any, None]:
        data = self.index[ts].iamc.tabulate(
            region={"name__in": region} if len(region) else None,
            variable={"name__in": variable} if len(variable) else None,
            unit={"name__in": unit} if len(unit) else None,
        )

        # Protect against empty data
        if not data.empty:
            # TODO depending on data["type"], a different column name will contain the
            # "year" values:
            # step_year if type is ANNUAL or CATEGORICAL
            # step_category if type is CATEGORICAL
            # step_datetime if type is DATETIME
            # Assuming that just one of these is present, identify it here:
            year_columns = {
                "year",
                "step_year",
                "step_category",
                "step_datetime",
            } & set(data.columns)
            assert len(year_columns) == 1, (
                f"Unexpected number of year columns found: {year_columns}"
            )
            year_column = list(year_columns)[0]

            # Guard against empty year filter
            if len(year):
                data = data.loc[data[year_column].isin(year)]

            # We seem to only deal with "Year" for now (maybe until supporting
            # timeslices)
            subannual = "Year" if year_column in {"year", "step_year"} else "subannual"
            data = data.assign(subannual=subannual)

            # Select only the columns we're interested in
            # TODO the ixmp4 docstrings sounds like region, variable, and unit are not
            # part of the df?
            data = data[
                ["region", "variable", "unit", "subannual", year_column, "value"]
            ]

        # TODO Why would we iterate and yield tuples instead of returning the whole df?
        for row in data.itertuples(index=False, name=None):
            yield row

    # Handle timeslices

    # def set_timeslice(self, name: str, category: str, duration: float) -> None:
    #     return super().set_timeslice(name, category, duration)

    # Handle I/O

    def write_file(
        self, path: PathLike, item_type: ItemType, **kwargs: Unpack[WriteKwargs]
    ) -> None:
        """Write Platform, TimeSeries, or Scenario data to file.

        IXMP4Backend supports writing to:

        - ``path='*.gdx', item_type=ItemType.SET | ItemType.PAR``.
        - ``path='*.csv', item_type=TS``. The `default` keyword argument is
          **required**.

        Other parameters
        ----------------
        filters : dict of dict of str
            Restrict items written. The following filters may be used:

            - model : list of str
            - scenario : list of str | Scenario
            - variable : list of str
            - region: list of str
            - unit: list of str
            - default : bool
                If :obj:`True`, only data from TimeSeries versions with
                :meth:`.TimeSeries.set_as_default` are written.
            - export_all_runs: bool
                Whether to export all existing model+scenario run combinations.

        See also
        --------
        .Backend.write_file
        """
        try:
            # Call the default implementation, e.g. for .xlsx
            super().write_file(path, item_type, **kwargs)
        except NotImplementedError:
            pass
        else:
            return

        # NOTE Would like to use proper paths in signature already, but comply with base
        #  class for now
        _path = Path(path)

        ts, filters = self._handle_rw_filters(kwargs["filters"])
        if _path.suffix == ".gdx" and item_type is ItemType.SET | ItemType.PAR:
            # NOTE if we keep the TypedDicts and can't pop items, we might have to
            # adjust more checks like this. Alternatively, use explicit keyword
            # parameters for functions or convert kwargs to dicts in here to enable pop.
            if len(filters) > 1:  # pragma: no cover
                raise NotImplementedError("write to GDX with filters")
            elif not isinstance(ts, Scenario):  # pragma: no cover
                raise ValueError("write to GDX requires a Scenario object")

            write_run_to_gdx(
                run=self.index[ts],
                file_name=_path,
                record_version_packages=kwargs["record_version_packages"],
                container_data=kwargs["container_data"],
            )
        elif _path.suffix == ".csv" and item_type is ItemType.TS:
            models = filters.pop("model")
            # NOTE this is what we get for not differentiating e.g. scenario vs
            # scenarios in filters...
            scenarios = set(cast(list[str], filters.pop("scenario")))
            variables = filters.pop("variable")
            units = filters.pop("unit")
            regions = filters.pop("region")
            default = filters.pop("default")
            # TODO (How) Should we include this?
            # export_all_runs = filters.pop("export_all_runs")

            data = self._backend.runs.tabulate(
                model={"name__in": models},
                scenario={"name__in": scenarios},
                iamc={
                    "region": {"name__in": regions},
                    "variable": {"name__in": variables},
                    "unit": {"name__in": units},
                },
                default_only=default,
            )
            data.to_csv(path_or_buf=_path)

        else:
            raise NotImplementedError

    def read_file(
        self, path: PathLike, item_type: ItemType, **kwargs: Unpack[ReadKwargs]
    ) -> None:
        """Read Platform, TimeSeries, or Scenario data from file.

        IXMP4Backend supports reading from:

        - ``path='*.gdx', item_type=ItemType.MODEL``. The keyword arguments
          `check_solution`, `comment`, `equ_list`, and `var_list` are **required**.

        Other parameters
        ----------------
        check_solution : bool
            If True, raise an exception if the GAMS solver did not reach optimality.
            (Only for MESSAGE-scheme Scenarios.)
        comment : str
            Comment added to Scenario when importing the solution.
        equ_list : list of str
            Equations to be imported.
        var_list : list of str
            Variables to be imported.
        filters : dict of dict of str
            Restrict items read.

        See also
        --------
        .Backend.read_file
        """
        try:
            # Call the default implementation, e.g. for .xlsx
            super().read_file(path, item_type, **kwargs)
        except NotImplementedError:
            pass
        else:
            return

        _path = Path(path)

        # TODO handle case when filters is not present
        ts, _ = self._handle_rw_filters(kwargs["filters"])
        # Convert to normal dict to allow removal of keys
        _kwargs = {k: v for (k, v) in kwargs.items() if k != "filters"}

        if _path.suffix == ".gdx" and item_type is ItemType.MODEL:
            kw = {"check_solution", "comment", "equ_list", "var_list"}

            if not isinstance(ts, Scenario):
                raise ValueError("read from GDX requires a Scenario object")
            elif set(_kwargs.keys()) != kw:
                raise ValueError(
                    f"keyword arguments {_kwargs.keys()} do not match required {kw}"
                )

            check_solution = bool(_kwargs.pop("check_solution"))
            comment = str(_kwargs.pop("comment"))

            # message_ix/ixmp4 uses these lists to set the default items to read, so
            # these will always be true
            equ_list = cast(list[str], _kwargs.pop("equ_list"))
            var_list = cast(list[str], _kwargs.pop("var_list"))

            read_gdx_to_run(
                run=self.index[ts],
                result_file=_path,
                equ_list=equ_list,
                var_list=var_list,
                comment=comment,
                check_solution=check_solution,
            )

        else:
            raise NotImplementedError(path, item_type)

    # The below methods of base.Backend are not yet implemented
    def _ni(self, *args, **kwargs):
        raise NotImplementedError

    delete = _ni
    delete_geo = _ni
    delete_meta = _ni
    get_doc = _ni
    get_geo = _ni
    get_meta = _ni
    get_timeslices = _ni
    last_update = _ni
    remove_meta = _ni
    set_doc = _ni
    set_geo = _ni
    set_meta = _ni
    set_timeslice = _ni
