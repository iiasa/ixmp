import builtins
import logging
import re
from collections.abc import Iterable, Iterator, MutableMapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from itertools import chain
from os import PathLike
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from urllib.parse import urlsplit

import ixmp4
import ixmp4.data.backend
import numpy as np
import pandas as pd
from ixmp4.conf.settings import Settings
from ixmp4.core import Run
from ixmp4.core.exceptions import (
    NotUnique,
    OptimizationDataValidationError,
    PlatformNotFound,
)
from ixmp4.core.optimization import (
    Equation,
    IndexSet,
    Parameter,
    Scalar,
    Table,
    Variable,
)
from ixmp4.core.region import Region
from ixmp4.data.meta.exceptions import InvalidRunMeta
from ixmp4.db import get_alembic_controller
from ixmp4.transport import DirectTransport, HttpxTransport
from platformdirs import user_state_path

# TODO Import this from typing when dropping Python 3.11
# TODO Use type x = ... instead of TypeAlias when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp.core.item import Equation as IXMPEquation
from ixmp.core.item import Item, Set
from ixmp.core.item import Parameter as IXMPParameter
from ixmp.core.item import Variable as IXMPVariable
from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries
from ixmp.util import as_str_list

from .base import CachingBackend
from .common import IAMC_IDX, ItemType
from .ixmp4_io import maybe_transact, read_gdx_to_run, write_run_to_gdx

if TYPE_CHECKING:
    from ixmp4.conf.platforms import PlatformConnectionInfo
    from ixmp4.conf.settings import Settings
    from ixmp4.core.optimization.equation import EquationServiceFacade
    from ixmp4.core.optimization.indexset import IndexSetServiceFacade
    from ixmp4.core.optimization.parameter import ParameterServiceFacade
    from ixmp4.core.optimization.scalar import ScalarServiceFacade
    from ixmp4.core.optimization.table import TableServiceFacade
    from ixmp4.core.optimization.variable import (
        VariableServiceFacade as OptVariableServiceFacade,
    )
    from ixmp4.data.iamc.datapoint.filter import FacadeDataPointFilter

    from ixmp.types import (
        Filters,
        IXMP4IamcDomainService,
        IXMP4ModelData,
        IXMP4ModelDataType,
        IXMP4ServiceFacade,
        ParData,
        ReadKwargs,
        SetData,
        SolutionData,
        WriteKwargs,
    )


log = logging.getLogger(__name__)


#: Mapping from common ixmp.backend "ix_type" strings to concrete ixmp4 classes.
CLASS_FOR_IX_TYPE: dict[str, tuple["IXMP4ModelDataType", ...]] = {
    "equ": (Equation,),
    "par": (Scalar, Parameter),
    "set": (IndexSet, Table),
    "var": (Variable,),
}

#: String prefix used to encode the 'meta' attribute of time series data points.
META = "__META__|"

#: Mapping from keys of ixmp4 {Equation,Parameter,Variable}.data and column names used
#: by ixmp.Scenario.
RENAME_COLS = {"values": "value", "units": "unit", "levels": "lvl", "marginals": "mrg"}

UNITS_NOT_SET = "_NOTSET"


def _align_dtypes_for_filters(
    filters: dict[str, list[Any]], data: pd.DataFrame
) -> None:
    """Convert `filters` values to types enabling `data.isin()`."""
    # TODO Is this really the proper way to handle these types?
    TYPE_MAP = {"object": str, "int64": int, "str": str}

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


def _remove_empty_lists(
    filters: MutableMapping[str, list[Any]],
) -> dict[str, list[Any]]:
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

    # Settings() is a pydantic class, so mypy can only understand that the omitted
    # fields have defaults using the pydantic plugin for mypy.
    #: Settings for the :mod:`ixmp4` platform.
    ixmp4_settings: "Settings" = field(default_factory=lambda: Settings())  # type: ignore [call-arg]

    #: :mod:`ixmp4` data source name.
    dsn: str = ""

    #: If :any:`True`, populate the :class:`.IXMP4Backend` / :class:`.Platform` with
    #: certain units and regions that are automatically added to any
    #: :class:`.JDBCBackend`. These include:
    #:
    #: - Units: "???", "GWa", "USD/km", "USD/kWa", "cases", "kg", "km". Of these, only
    #:   "cases" and "km" are used by the :mod:`message_ix` tutorials.
    #: - Regions: "World", in hierarchy "common".
    jdbc_compat: bool | str = False

    def __post_init__(self) -> None:
        if not self.dsn:
            path = self.ixmp4_settings.get_database_path(self.ixmp4_name)
            self.dsn = f"sqlite:///{path}"
        # Handle str value, e.g. from the command line
        if isinstance(self.jdbc_compat, str):
            val = self.jdbc_compat.title()
            self.jdbc_compat = bool(
                {"0": False, "False": False, "No": False, "True": True}.get(val, val)
            )

    @classmethod
    def handle_config(
        cls, args: Sequence[Any], kwargs: MutableMapping[str, Any]
    ) -> dict[str, Any]:
        """Helper for :meth:`IXMP4Backend.handle_config`."""
        if len(args):
            raise ValueError(f"Unhandled positional args to IXMP4Backend: {args!r}")

        try:
            result = asdict(cls(**kwargs))
            return result
        except TypeError:
            raise ValueError(
                "Expected at least 'ixmp4_name' keyword argument to IXMP4Backend; got"
                f" {kwargs}"
            )

    @property
    def init_units(self) -> list[str]:
        """Unit codes present on a new :class:`IXMP4Backend`."""
        return [UNITS_NOT_SET] + (
            ["???", "GWa", "USD/km", "USD/kWa", "cases", "kg", "km"]
            if self.jdbc_compat
            else []
        )

    @property
    def platform_info(self) -> "PlatformConnectionInfo":
        """Return an ixmp4.PlatformInfo instance.

        This ensures that there is an entry in the :mod:`ixmp4` configuration with name
        :attr:`ixmp4_name`.

        - If the entry does not exist, it is created.
        - If the entry does exist, its DSN must be the same as :attr:`dsn`.
        """
        toml_platforms = self.ixmp4_settings.get_toml_platforms()

        result: "PlatformConnectionInfo"
        try:
            # Retrieve the info for an existing platform
            result = toml_platforms.get_platform(self.ixmp4_name)
            assert result.name == self.ixmp4_name and result.dsn == self.dsn
        except PlatformNotFound:
            # Add platform info to ixmp4 configuration
            toml_platforms.add_platform(self.ixmp4_name, self.dsn)
            # add_platform() returns None, so use get_platform() to retrieve the object
            result = toml_platforms.get_platform(self.ixmp4_name)
        except AssertionError:  # pragma: no cover
            log.error(f"From ixmp4: {result}")
            log.error(f"From ixmp: name={self.ixmp4_name!r}, dsn={self.dsn!r}")
            raise

        return result


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`.

    Keyword arguments are passed to :class:`Options`.
    """

    _platform: "ixmp4.Platform"

    #: An instance of a :mod:`ixmp4` Backend class.
    _backend: "ixmp4.data.backend.Backend"
    _options: "Options"

    # Mapping from ixmp.TimeSeries object and/or run ID to the underlying ixmp4.Run
    # object (or subclasses of either).
    index: dict[TimeSeries, "Run"] = {}

    # Mapping from platform name to ixmp4 Backend; this enables repeated __init__()
    # calls for in-memory DBs
    backend_index: dict[str, "ixmp4.data.backend.Backend"] = {}

    def __init__(
        self,
        *,
        ixmp4_name: str,
        dsn: str = Options.dsn,
        jdbc_compat: bool | str = Options.jdbc_compat,
        ixmp4_settings: "Settings | None" = None,
        cache: bool = True,
    ) -> None:
        super().__init__(cache_enabled=cache)

        # - Handle arguments.
        # - If ixmp4_settings are not provided, create a new Settings() instance using
        #   current environment variables.
        self._options = opts = Options(
            ixmp4_name=ixmp4_name,
            dsn=dsn,
            jdbc_compat=jdbc_compat,
            ixmp4_settings=ixmp4_settings or Settings(),  # type: ignore [call-arg]
        )

        # Get an existing ixmp4.Backend object
        if backend := self.backend_index.get(opts.ixmp4_name, None):
            self._backend = backend
        else:
            # Object does not exist -> instantiate using PlatformInfo from `opts`
            transport = self._get_transport(opts.platform_info)
            self._backend = self.backend_index[opts.ixmp4_name] = (
                ixmp4.data.backend.Backend(transport)
            )

        # Instantiate an ixmp4.Platform using this ixmp4.Backend; store a reference
        self._platform = ixmp4.Platform(
            self._backend, settings=self._options.ixmp4_settings
        )

        if opts.jdbc_compat:
            try:
                self.set_node(name="World", hierarchy="common")
            except NotUnique:
                pass

        for u in opts.init_units:
            try:
                self.set_unit(u, "For compatibility with ixmp.JDBCBackend")
            except NotUnique:
                # Already exists
                # NB The exception class is actually UnitNotUnique, but this is created
                #    dynamically and is not importable
                pass

    def _get_transport(
        self, ci: "PlatformConnectionInfo", http_credentials: str = "default"
    ) -> HttpxTransport | DirectTransport:
        """Return an ixmp4 Transport instance according to the connection info."""
        settings = self._options.ixmp4_settings
        if ci.dsn.startswith("http"):  # pragma: no cover
            cred_dict = settings.get_credentials().get(http_credentials)
            return HttpxTransport.from_url(
                ci.dsn,
                settings=settings.client,
                auth=settings.get_client_auth(cred_dict),
            )
        else:
            transport = DirectTransport.from_dsn(
                ci.dsn, check_alembic_version=settings.check_alembic_version
            )

            # Ensure the ixmp4 database is in a consistent state
            assert transport.session.bind is not None
            db_url = transport.session.bind.engine.url.render_as_string(
                hide_password=False
            )

            # Identify the path of a lockfile
            lockfile_dir = user_state_path("ixmp").joinpath("ixmp4-migration-lock")
            lockfile_dir.mkdir(parents=True, exist_ok=True)
            lockfile = lockfile_dir.joinpath(urlsplit(db_url).path.rpartition("/")[2])

            while lockfile.exists():
                sleep(0.5)

            lockfile.touch()
            get_alembic_controller(db_url).upgrade_database("head")
            lockfile.unlink()

            return transport

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
    def handle_config(
        cls, args: Sequence[Any], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        return Options.handle_config(args, kwargs)

    # def close_db(self) -> None:
    #     self._backend.close()  # identical to self._platform.backend.close()

    # def open_db(self) -> None:
    #     ...

    # Modifying the Platform

    def add_scenario_name(self, name: str) -> None:
        self._platform.scenarios.create(name=name)

    # TODO clarify: ixmp4.Run doesn't have a name, but is the new ixmp.Scenario:
    # Should it have a name or are these scenario names okay?
    def get_scenario_names(self) -> Iterator[str]:
        for scenario in self._platform.scenarios.list():
            yield scenario.name

    def add_model_name(self, name: str) -> None:
        self._platform.models.create(name=name)

    def get_model_names(self) -> Iterator[str]:
        for model in self._platform.models.list():
            yield model.name

    def get_scenarios(
        self, default: bool, model: str | None, scenario: str | None
    ) -> Iterator[list[bool | int | str]]:
        filter = ixmp4.Run.Filter(default_only=default)
        if model is not None:
            filter["model"] = {"name": model}
        if scenario is not None:
            filter["scenario"] = {"name": scenario}

        runs = self._platform.runs.list(**filter)
        for run in runs:
            yield [
                str(run.model.name),
                str(run.scenario.name),
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
        parent: str | None = None,
        hierarchy: str | None = None,
        synonym: str | None = None,
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
        # Retrieve the version of the ixmp4.Run object
        v = run.version
        if ts.version is None:
            # The default version was requested; update the attribute
            ts.version = v
        elif v != ts.version:  # pragma: no cover
            # Something went wrong on the ixmp4 side
            raise RuntimeError(f"got version {v} instead of {ts.version}")

        # Add to index
        self.index[ts] = run

        if isinstance(ts, Scenario):
            # Retrieve the `scheme` attribute from the Run.meta dict
            run_scheme = str(run.meta.get("_ixmp_scheme", ""))
            if ts.scheme is not None and ts.scheme != run_scheme:
                raise RuntimeError(
                    f"Load existing {ts.__class__} with scheme {ts.scheme}"
                )
            ts.scheme = str(run.meta.get("_ixmp_scheme", "")) or ts.scheme

    def init(self, ts: TimeSeries, annotation: str) -> None:
        """Implementation of :meth:`.base.Backend.init`.

        - The :py:`annotation=...` parameter to :py:`TimeSeries.__init__` is stored on
          :attr:`ixmp4.core.run.Run.meta` with the key "_ixmp_annotation".
        - :attr:`.Scenario.scheme` is stored on with the key "_ixmp_scheme".
        """
        run = self._platform.runs.create(model=ts.model, scenario=ts.scenario)
        with run.transact("Initialize ixmp.TimeSeries"):
            # Store ixmp.TimeSeries annotation
            run.meta["_ixmp_annotation"] = annotation

            # Store ixmp.Scenario.scheme
            if isinstance(ts, Scenario):
                run.meta["_ixmp_scheme"] = ts.scheme

                # NOTE ixmp_source always defines these for MESSAGE-scheme Scenarios
                if self._options.jdbc_compat and ts.scheme == "MESSAGE":
                    run.optimization.indexsets.create(name="technology")
                    run.optimization.indexsets.create(name="year")

        # Acquire lock to make ts/run editable
        run._lock()

        self._index_and_set_attrs(run, ts)

    def clone(
        self,
        s: Scenario,
        platform_dest: Platform,
        model: str,
        scenario: str,
        annotation: str | None,
        keep_solution: bool,
        first_model_year: int | None = None,
    ) -> Scenario:
        # TODO either do log.warning that annotation is unused or
        # run.docs = annotation
        # NOTE ixmp4 won't reproduce this stack-violation behaviour
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
            platform_dest, model, scenario, version=cloned_run.version, scheme=s.scheme
        )
        self._index_and_set_attrs(cloned_run, cloned_s)

        if not keep_solution:
            self.clear_solution(cloned_s)

        # Clone to version 1 of a given (model name, scenario name) becomes the initial
        # default. This matches behaviour of the Java code underlying JDBCBackend.
        if cloned_run.version == 1:
            cloned_run.set_as_default()

        return cloned_s

    def _get_run(
        self, model: str | None, scenario: str | None, version: int | None
    ) -> Run:
        """Retrieve a Run from the ixmp4 Platform.

        This method:

        - Ensures that `model` and `scenario` are not :any:`None`.
        - If `version` is not found (for instance, is a value like -1), retrieve the
          *highest* existing version for (model, scenario).
        - Returns an existing Run instance from :attr:`index`, if it already exists.
        - Adds the resulting Run to :attr:`index` by its :attr:`.Run.id`.
        """
        try:
            assert model is not None and scenario is not None
        except AssertionError:
            raise ValueError(
                "Invalid arguments. Valid combination: (model, scenario, version)"
            )

        try:
            run = self._platform.runs.get(
                model=model, scenario=scenario, version=version
            )
        except Run.NoDefaultVersion:
            # Retrieve that maximum version as a default version
            # NOTE This could be done in the database; if performance requires it, add
            #      as a feature in ixmp4
            all_runs = self._platform.runs.list(
                model=model, scenario=scenario, default_only=False
            )
            run = max(all_runs, key=lambda r: r.version)

        return run

    def get(self, ts: TimeSeries) -> None:
        version = int(ts.version) if ts.version else None
        run = self._get_run(ts.model, ts.scenario, version)
        self._index_and_set_attrs(run, ts)

    def check_out(self, ts: TimeSeries, timeseries_only: bool) -> None:
        # TODO: public API in ixmp4
        if timeseries_only:
            log.log(self._log_level, "ixmp4 only allows full or no access to Runs!")
        try:
            self.index[ts]._lock()
        except Run.IsLocked:
            raise RuntimeError("Already checked out")

    def discard_changes(self, ts: TimeSeries) -> None:
        # TODO: public API in ixmp4
        run = self.index[ts]
        # ._dto.lock_transaction holds the tx_id at which the lock was acquired
        if run._dto.lock_transaction is None or not run.owns_lock:
            raise RuntimeError("Discarding changes requires prior call to `check_out`.")
        run._service.revert(run.id, run._dto.lock_transaction)
        run._refresh()

    def commit(self, ts: TimeSeries, comment: str) -> None:
        run = self.index[ts]
        # Convert error for compatibility with JDBC (e.g. util.maybe_commit())
        try:
            run.checkpoints.create(message=comment)
        except Run.LockRequired as e:
            raise RuntimeError from e

        run._unlock()

    def clear_solution(self, s: Scenario, from_year: int | None = None) -> None:
        if from_year:
            # This is required for compatibility with JDBC
            if type(s) is not Scenario:
                raise TypeError(
                    "s_clear_solution(from_year=...) only valid for ixmp.Scenario; not "
                    "subclasses"
                )
        run = self.index[s]

        with maybe_transact(run, "Clear solution for ixmp.Scenario"):
            run.optimization.remove_solution()

            # Identify non-'meta' time series data to remove
            # TODO Do this by calling an upstream method directly
            df = run.iamc.tabulate(year__gte=from_year or 0)
            mask = ~df["variable"].str.startswith(META)
            # Remove these time series data
            run.iamc.remove(df[mask])

        self.cache_invalidate(ts=s)

    def set_as_default(self, ts: TimeSeries) -> None:
        self.index[ts].set_as_default()

    # Information about the Run

    def run_id(self, ts: TimeSeries) -> int:
        # TODO is the Run.version really what this function is after?
        return self.index[ts].version

    def is_checked_out(self, ts: TimeSeries) -> bool:
        return self.index[ts].owns_lock

    def is_default(self, ts: TimeSeries) -> bool:
        # Force refresh, for/ the case that another (model, scenario) but different
        # version has been set as default since the Run instance in self.index was last
        # refreshed
        self.index[ts]._refresh()
        return self.index[ts].is_default

    def has_solution(self, s: Scenario) -> bool:
        return self.index[s].optimization.has_solution()

    def last_update(self, ts: TimeSeries) -> str | None:
        # FIXME This should be exposed by the Run object without accessing ._dto
        last_update = self.index[ts]._dto.updated_at or datetime.now(timezone.utc)
        return last_update.strftime("%Y-%m-%d %H:%M:%S.%f")

    def set_meta(
        self,
        meta: dict[str, bool | float | int | str],
        model: str | None = None,
        scenario: str | None = None,
        version: int | None = None,
    ) -> None:
        run = self._get_run(model, scenario, version)

        new_meta = dict(run.meta) | meta

        with maybe_transact(run, "Update meta"):
            try:
                run.meta = new_meta
            except (KeyError, InvalidRunMeta) as e:
                if "<class" in str(e):
                    raise ValueError(
                        "Cannot use values provided due to incompatible types! \n "
                        f"Provided: {meta} \n Acceptable types: int, float, str, bool"
                    ) from None
                else:
                    raise

    def get_meta(
        self,
        model: str | None = None,
        scenario: str | None = None,
        version: int | None = None,
        # NOTE: strict is ignored here because ixmp4 does
        # not have more than one "level" of docs
        strict: bool = False,
    ) -> dict[str, Any]:
        return dict(self._get_run(model, scenario, version).meta)

    def remove_meta(
        self,
        names: list[str],
        model: str | None,
        scenario: str | None,
        version: int | None,
    ) -> None:
        run = self._get_run(model, scenario, version)

        # TODO: This could have a nicer api in ixmp4
        meta_df = pd.DataFrame({"key": names})
        meta_df["run__id"] = run.id
        self._backend.meta.bulk_delete(df=meta_df)

    def _get_domain_repo(self, domain: str) -> "IXMP4IamcDomainService":
        # TODO Limit domain annotation with Literal
        if domain == "model":
            return self._platform.models
        elif domain == "region":
            return self._platform.regions
        elif domain == "scenario":
            return self._platform.scenarios
        elif domain == "timeseries":
            return self._platform.iamc.variables
        raise ValueError(
            f"No such domain: {domain}, existing domains: "
            "model, region, scenario, timeseries"
        )

    def set_doc(
        self, domain: str, docs: dict[str, str] | Iterable[tuple[str, str]]
    ) -> None:
        domain_repo = self._get_domain_repo(domain=domain)

        # Convert a list of tuple into a dict
        if not isinstance(docs, dict):
            docs = {name: docstring for (name, docstring) in docs}

        # Add `META` prefix to every item
        docs |= {f"{META}{name}": docstring for name, docstring in docs.items()}

        # Loop over items matching the keys of `docs`
        # TODO Avoid this loop by adding bulk methods to ixmp4's DocsRepos
        for item in domain_repo.list(name__in=list(docs.keys())):
            item.docs = docs[item.name]

    def get_doc(self, domain: str, name: str | None = None) -> str | dict[str, str]:
        items = self._get_domain_repo(domain=domain).list()

        if name is None:
            return {item.name: (item.docs or "") for item in items}
        else:
            for item in items:
                if item.name.split(META)[-1] == name:
                    return item.docs or ""
            raise ValueError(f"No {domain!r} named {name!r}")

    # Handle optimization items

    @overload
    def _get_repo(self, s: Scenario, type: type[Scalar]) -> "ScalarServiceFacade": ...

    @overload
    def _get_repo(
        self, s: Scenario, type: type[IndexSet]
    ) -> "IndexSetServiceFacade": ...

    @overload
    def _get_repo(self, s: Scenario, type: type[Table]) -> "TableServiceFacade": ...

    @overload
    def _get_repo(
        self, s: Scenario, type: type[Parameter]
    ) -> "ParameterServiceFacade": ...

    @overload
    def _get_repo(
        self, s: Scenario, type: type[Equation]
    ) -> "EquationServiceFacade": ...

    @overload
    def _get_repo(
        self, s: Scenario, type: type[Variable]
    ) -> "OptVariableServiceFacade": ...

    # NOTE Type hints here ensure the function body is checked
    def _get_repo(self, s: Scenario, type: type) -> "IXMP4ServiceFacade":
        """Return the repository of items of `type` belonging to `s`."""
        if type is Scalar:
            return self.index[s].optimization.scalars
        elif type is IndexSet:
            return self.index[s].optimization.indexsets
        elif type is Table:
            return self.index[s].optimization.tables
        elif type is Parameter:
            return self.index[s].optimization.parameters
        elif type is Equation:
            return self.index[s].optimization.equations
        elif type is Variable:
            return self.index[s].optimization.variables
        else:  # pragma: no cover
            raise RuntimeError(type)

    def init_item(
        self,
        s: Scenario,
        type: str,
        name: str,
        idx_sets: Sequence[str],
        idx_names: Sequence[str] | None,
    ) -> None:
        # Identify an IXMP4 model data type. If the item has dimensions, the final (or
        # only) element of a CLASS_FOR_IX_TYPE entry. If dimensionless, then the first
        # entry, e.g. Scalar or IndexSet.
        idx = -1 if idx_sets else 0
        ixmp4_type = CLASS_FOR_IX_TYPE[type][idx]

        # TODO Rename 'type' parameters!

        # Retrieve the repository of such items
        repo = self._get_repo(s=s, type=ixmp4_type)

        try:
            # NOTE We can't do
            # if isinstance(repo, IndexSetService):
            # because mypy says this needs "@runtime_checkable protocols"
            if idx == 0 and type == "set":
                # IndexSet
                repo.create(name=name)  # type: ignore[call-arg]
            # elif isinstance(repo, BEScalarRepository):
            elif idx == 0 and type == "par":
                # Scalar
                # ixmp4 v0.10 required that a value and unit be supplied on creation
                # ixmp4 v0.16 requires that the value not be NaN
                repo.create(name=name, value=-np.inf, unit="_NOTSET")  # type: ignore[call-arg]

            else:
                repo.create(
                    name=name,
                    constrained_to_indexsets=list(idx_sets),
                    column_names=list(idx_names) if idx_names else None,
                )  # type: ignore[call-arg]
        except Run.LockRequired:
            raise RuntimeError("This Scenario cannot be edited, do a checkout first!")
        except NotUnique:
            raise ValueError(f"{repr(name)} already exists")

    def list_items(self, s: Scenario, type: str) -> list[str]:
        run_id = self.index[s].id
        types = CLASS_FOR_IX_TYPE[type]
        # NOTE: bug in ixmp4 returns items for all runs via list by default
        items: Iterable["IXMP4ModelData"] = chain(
            *[self._get_repo(s=s, type=t).list(run__id=run_id) for t in types]
        )
        return [item.name for item in items]

    def _find_item(
        self,
        s: Scenario,
        name: str,
        types: tuple[type["IXMP4ModelData"], ...] = (),
    ) -> "IXMP4ModelData":
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
        IXMP4ModelData
            the item.

        Raises
        ------
        KeyError
            If no item with `name` and type in `types` can be found in `s`.
        """  # noqa: E501

        # NOTE this assumes that `name` is unique across all *Repository corresponding
        # to `types`, in line with JDBCBackend/ixmp_source. ixmp4 only enforces that
        # `name` is unique within each particular repository, e.g. allows that there is
        # both a Parameter and Variable named "foo"

        for cls in types or (Equation, IndexSet, Parameter, Scalar, Table, Variable):
            repo = self._get_repo(s=s, type=cls)
            try:
                return repo.get_by_name(name)
            except cls.NotFound:
                pass

        raise KeyError(f"No item named {name!r} in this Scenario")

    def _get_indexset_or_table(self, s: Scenario, name: str) -> "IndexSet | Table":
        """Get `name` from Scenario `s`.

        Try first if `name` is an IndexSet. Get it as a Table if it isn't.
        """
        try:
            indexset_repo = self._get_repo(s=s, type=IndexSet)
            return indexset_repo.get_by_name(name=name)
        except IndexSet.NotFound:
            try:
                table_repo = self._get_repo(s=s, type=Table)
                return table_repo.get_by_name(name=name)
            except Table.NotFound as e:
                raise KeyError from e

    def item_index(
        self, s: Scenario, name: str, sets_or_names: Literal["sets", "names"]
    ) -> list[str]:
        item = self._find_item(s=s, name=name)
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
        key: str | list[str],
        comment: str | None = None,
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

        run = self.index[s]

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
            indexset = run.optimization.indexsets.get_by_name(name=name)
            if key not in indexset.data:
                indexset.add_data(data=key)
        else:
            table = run.optimization.tables.get_by_name(name=name)
            # TODO should we enforce in ixmp4 that when constrained_to_indexsets
            # contains duplicate values, column_names must be provided?
            keys = table.column_names or table.indexset_names
            data_to_add = pd.DataFrame({keys[i]: [key[i]] for i in range(len(key))})

            # Silently ignore duplicate data, see NOTE above
            data_to_add = data_to_add[~data_to_add.isin(values=table.data).all(axis=1)]

            try:
                self._backend.optimization.tables.add_data(
                    id=table.id, data=data_to_add
                )
            except OptimizationDataValidationError as e:
                # NOTE The ixmp4 error doesn't allow creating exactly the same error
                # string, but we should switch to its dedicated error, anyway.
                raise ValueError(
                    f"The Table '{name}' is not allowed to have (at least one of) the "
                    f"elements '{key}' based on its IndexSets!"
                ) from e

    def _create_scalar(
        self,
        s: Scenario,
        name: str,
        value: float,
        unit: str | None,
        comment: str | None = None,
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
        if unit is None:
            log.info("Setting Scalar as dimensionless")
            unit = self._backend.units.get_or_create("").name

        scalar = self.index[s].optimization.scalars.create(
            name=name, value=value, unit=unit
        )
        if comment:
            scalar.docs = comment

    def _add_data_to_parameter(
        self,
        s: Scenario,
        name: str,
        key: str | list[str],
        value: float,
        unit: str,
        comment: str | None = None,
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
        parameter = self.index[s].optimization.parameters.get_by_name(name=name)
        # TODO there's got to be a better way for handling possible lists
        if isinstance(key, str):
            key = [key]

        keys = parameter.column_names or parameter.indexset_names
        data_to_add: dict[str, list[float] | list[str]] = {
            keys[i]: [key[i]] for i in range(len(key))
        }
        data_to_add["values"] = [value]
        data_to_add["units"] = [unit]

        self._backend.optimization.parameters.add_data(
            id=parameter.id, data=data_to_add
        )

    def item_set_elements(
        self,
        s: Scenario,
        type: type[Item],
        name: str,
        elements: Iterable[tuple[Any, float | None, str | None, str | None]],
    ) -> None:
        if type is Set:
            for key, value, unit, comment in elements:
                self._add_data_to_set(s=s, name=name, key=key, comment=comment)
        elif type is IXMPParameter:
            for key, value, unit, comment in elements:
                if not bool(key):
                    run = self.index[s]
                    scalar = run.optimization.scalars.get_by_name(name=name)
                    # NOTE We could implement a core-layer Scalar.update() in ixmp4 and
                    # handle this there
                    try:
                        run.require_lock()
                    except Run.LockRequired:
                        raise RuntimeError(
                            "The Scalar cannot be edited, check out the Scenario first!"
                        )

                    scalar.update(value=value, unit_name=str(unit))
                    if comment is not None:
                        scalar.docs = comment
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
        else:
            # NB(PNK): Simplified equivalent to _add_data_to_parameter()
            # Convert an ixmp.core.Item subclass to an IXMP4 type
            type_ = cast(
                builtins.type[Equation | Variable],
                {IXMPEquation: Equation, IXMPVariable: Variable}[type],
            )
            # Retrieve the item from its respective repository
            item = self._get_repo(s, type_).get_by_name(name)
            # Key dimensions of the item
            dims = item.column_names or item.indexset_names or []
            i_d = list(enumerate(dims))
            # Convert `elements` to the data structure required by ixmp4

            # Container for data
            data: dict[str, list[Any]] = {"levels": [], "marginals": []} | {
                d: [] for d in dims
            }
            for key, level, marginal, comment in elements:
                for i, d in i_d:
                    data[d].append(key[i])
                data["levels"].append(level)
                data["marginals"].append(marginal)

            # NB `repo` is a {Equation,Variable}Repository object associated with a
            #    particular Run. It does not have a method .add_data(). For this,
            #    instead use a "backend repository" object.
            item.add_data(data=data)

        self.cache_invalidate(ts=s, ix_type=type.ix_type, name=name)

    def _get_set_data(
        self,
        s: Scenario,
        name: str,
        filters: dict[str, list[Any]] | None = None,
    ) -> "pd.Series[int] | pd.Series[str] | pd.DataFrame":
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

    @overload
    def item_get_elements(
        self, s: Scenario, ix_type: Literal["set"], name: str, filters: "Filters" = None
    ) -> "SetData": ...

    @overload
    def item_get_elements(
        self, s: Scenario, ix_type: Literal["par"], name: str, filters: "Filters" = None
    ) -> "ParData": ...

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["equ", "var"],
        name: str,
        filters: "Filters" = None,
    ) -> "SolutionData": ...

    def item_get_elements(
        self, s: Scenario, ix_type: str, name: str, filters: "Filters" = None
    ) -> "SetData | ParData | SolutionData":
        clean_filters: dict[str, list[str]] | None = None
        clean_filters = (
            {dim: as_str_list(ele) for dim, ele in filters.items()}
            if filters is not None
            else filters
        )

        # Try returning a cached value
        cached_value = self.maybe_get_cache(
            ts=s, ix_type=ix_type, name=name, filters=clean_filters
        )
        if cached_value is not None:
            # NOTE ixmp4 might return a different order; enforce same for compatibility
            return (
                cached_value.reset_index(drop=True)
                if isinstance(cached_value, pd.DataFrame)
                else cached_value
            )

        data: "SetData | ParData | SolutionData"

        if ix_type == "set":
            data = self._get_set_data(s=s, name=name, filters=clean_filters)
        else:
            # Retrieve the ixmp4 data object
            types = CLASS_FOR_IX_TYPE[ix_type]
            item = self._find_item(s=s, name=name, types=types)
            assert not isinstance(item, IndexSet)

            if isinstance(item, Scalar):
                # TODO DRY this; only call .cache() once below
                data = {"value": item.value, "unit": item.unit.name}
                self.cache(
                    ts=s, ix_type=ix_type, name=name, filters=clean_filters, value=data
                )
                return data

            # Columns/dict keys expected in result
            columns = (item.column_names or item.indexset_names or []) + (
                ["value", "unit"] if Parameter in types else ["lvl", "mrg"]
            )
            # Number of dimensions; 0 if scalar
            N_dim = len(columns) - 2

            # - Convert the ixmp4 item's data from dict to pd.DataFrame.
            # - Rename value columns from ixmp4 to ixmp names.
            # - Reorder columns according to the dimension order.
            data = (
                pd.DataFrame(item.data)
                .rename(columns=RENAME_COLS)
                .reindex(columns=columns)
            )
            if data.empty:
                # Ensure expected columns even if no data is present
                data = pd.DataFrame(columns=columns)

            # For scalar items, return dict for compatibility with JDBC
            if N_dim == 0 and set(types) & {Equation, Variable}:
                # TODO DRY this; only call .cache() once below
                data = (
                    {"lvl": np.nan, "mrg": np.nan}
                    if data.empty
                    else {"lvl": data["lvl"].values[0], "mrg": data["mrg"].values[0]}
                )
                self.cache(
                    ts=s, ix_type=ix_type, name=name, filters=clean_filters, value=data
                )
                return data

            # Apply filters if requested
            if clean_filters:
                # isin() won't consider int(700) to be in ['700'], etc
                _align_dtypes_for_filters(filters=clean_filters, data=data)
                clean_filters = _remove_empty_lists(filters=clean_filters)

                data = data[
                    data.isin(values=clean_filters)[clean_filters.keys()].all(axis=1)
                ].reset_index(drop=True)

        # Store cache
        self.cache(ts=s, ix_type=ix_type, name=name, filters=clean_filters, value=data)

        return data

    def item_delete_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        keys: Iterable[Sequence[str]],
    ) -> None:
        clear_whole_cache = False
        if type == "set":
            item = self._get_indexset_or_table(s=s, name=name)

            if isinstance(item, IndexSet):
                # NOTE We might have to expose IndexSet._data_type to cast correctly
                data = pd.DataFrame(keys, columns=[item.name])
                self._backend.optimization.indexsets.remove_data(
                    id=item.id, data=data[item.name].astype(str).to_list()
                )
                clear_whole_cache = True
            else:
                # TODO can we assume that keys follow same order as indexsets/columns?
                columns = item.column_names or item.indexset_names
                data = pd.DataFrame(keys, columns=columns)
                self._backend.optimization.tables.remove_data(id=item.id, data=data)
        else:
            parameter = self._get_repo(s=s, type=Parameter).get_by_name(name)
            columns = parameter.column_names or parameter.indexset_names
            data = pd.DataFrame(keys, columns=columns)
            self._backend.optimization.parameters.remove_data(
                id=parameter.id, data=data
            )

        # Since `name` may be an IndexSet, clear the cache entirely. This ensures that
        # e.g. parameter elements for Parameters indexed by `name` are also refreshed
        # on the next call to item_get_elements().
        args = (s,) if clear_whole_cache else (s, type, name)
        self.cache_invalidate(*args)

    def delete_item(
        self, s: Scenario, type: Literal["set", "par", "equ"], name: str
    ) -> None:
        # Locate the item. If `type` maps to >1 IXMP4 model data type, try each repo.
        item = self._find_item(s=s, name=name, types=CLASS_FOR_IX_TYPE[type])
        # Access the repository containing objects of `item`s type; delete
        item.delete()
        self.cache_invalidate(ts=s, ix_type=type, name=name)

    # NOTE The name 'cat_`name`' is used for backward compatibility with the JDBC, where
    # such names are hardcoded. 'cat' means 'category' and should be expanded for
    # clarity in the future.
    def cat_set_elements(
        self,
        ms: Scenario,
        name: str,
        cat: str,
        keys: str | Sequence[str],
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
        # Convert for convenience
        if isinstance(keys, str):
            keys = [keys]

        run = self.index[ms]

        # Name of the column corresponding to `indexset`
        column_name: str | None = None

        # Categories can be based on IndexSets directly or on 1-d Tables
        try:
            # Most should be based on IndexSets, try that first
            indexset = run.optimization.indexsets.get_by_name(name)
            indexset_name = indexset.name
        except IndexSet.NotFound:
            # We're dealing with a Table
            table = run.optimization.tables.get_by_name(name)

            # Ensure the Table's dimensions are correct before setting variables
            assert len(table.indexset_names) == 1
            indexset_name = table.indexset_names[0]
            column_name = table.name

        # Special handling for certain values of `name`, to align with JDBCBackend
        if name == "technology":
            # Use 'tec', 'type_tec', 'cat_tec', etc.
            name = "tec"
        elif name == "addon":
            # In MESSAGE-scheme scenarios 'cat_addon' is indexed by the 1D set 'addon',
            # which is in turn indexed by 'technology' (other 'cat_*' are indexed
            # directly by indexsets). Usually, the column names are 'type_<name>' and
            # '<name>', but for 'cat_addon', it's 'type_addon' and 'technology_addon'.
            column_name = "technology_addon"

        # Get or create the 'type_name' indexset
        type_name = f"type_{name}"
        category_indexset: IndexSet
        try:
            category_indexset = run.optimization.indexsets.get_by_name(name=type_name)
        except IndexSet.NotFound:
            category_indexset = run.optimization.indexsets.create(name=type_name)

        # Get or create the 'cat_name' table
        cat_name = f"cat_{name}"
        category_table: Table
        try:
            category_table = run.optimization.tables.get_by_name(name=cat_name)
        except Table.NotFound:
            category_table = run.optimization.tables.create(
                name=cat_name,
                constrained_to_indexsets=[indexset_name, category_indexset.name],
                column_names=[column_name, category_indexset.name]
                if column_name
                else None,
            )

        # Ensure proper treatment when is_unique is True
        if is_unique:
            if len(keys) > 1:
                raise ValueError(
                    f"Can only add one element to {type_name}={cat!s} with "
                    f"is_unique=True; got {keys}"
                )
            # Ensure data contains no data except that which we're going to add
            # NOTE if category_table contains data linked to elements existing now, this
            # will lead to DataValidationErrors when adding data to the table. Also,
            # ixmp4 might safeguard against this when implementing remove() functions
            if category_indexset.data:
                # Remove all existing data so that only the single provided element will
                # be stored in the indexset
                category_indexset.remove_data(category_indexset.data)

        # Add both the category and "all" to the category index set
        # NB We can narrow type because the `category_indexset` may only contain str
        if to_add := {"all", cat} - set(cast(list[str], category_indexset.data)):
            category_indexset.add_data(list(to_add))

        # Convert to dict of the form expected by ixmp4; add data
        category_table.add_data(
            {
                column_name or indexset_name: keys,
                category_indexset.name: [cat] * len(keys),
            }
        )

        # Invalidate caches
        for name_ in name, type_name, cat_name:
            self.cache_invalidate(ts=ms, ix_type="set", name=name_)

    # TODO In cat_set_elements, we change e.g. cat_technology to cat_tec. Do we need the
    # same here or do we expect user code to call this with name == "tec" if they're
    # interested in "technology"?
    def cat_get_elements(self, ms: Scenario, name: str, cat: str) -> list[str]:
        # NOTE ixmp_source treats "all" this way
        if cat == "all":
            return list(
                map(
                    str,
                    self.index[ms].optimization.indexsets.get_by_name(name=name).data,
                )
            )

        # Special treatment for 'technology' for historical reasons:
        if name == "technology":
            name = "tec"
        data = pd.DataFrame(
            self.index[ms].optimization.tables.get_by_name(name=f"cat_{name}").data
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
        category_indexset = self.index[ms].optimization.indexsets.get_by_name(
            f"type_{name}"
        )
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
        """Implementation of :meth:`.base.Backend.set_data`.

        - `meta` is stored by adding the prefix :data:`META` to the `variable` name.
        """
        # Construct a pd.Dataframe in the form expected by ixmp4
        df = pd.Series(data).reset_index().set_axis(["step_year", "value"], axis=1)

        # Values to pass to DataFrame.assign()
        assign = dict(
            region=region, variable=(META if meta else "") + variable, unit=unit
        )

        # NOTE We don't handle DataPoint.Type.DATETIME yet
        # NOTE subannual == "Year" per default and some string otherwise
        if subannual != "Year":
            assign.update(step_category=subannual)
            data_type = ixmp4.iamc.DataPoint.Type.CATEGORICAL
        else:
            data_type = ixmp4.iamc.DataPoint.Type.ANNUAL

        # Retrieve a reference to the Run
        run = self.index[ts]

        # Possibly lock
        _owns_lock = run.owns_lock
        if not _owns_lock:
            run._lock()

        try:
            # Add IAMC-structured data
            run.iamc.add(df.assign(**assign), type=data_type)
        except Region.NotFound:
            raise ValueError(f"region = {region}")

        if not _owns_lock:
            run._unlock()

    def get_data(
        self,
        ts: TimeSeries,
        region: Sequence[str],
        variable: Sequence[str],
        unit: Sequence[str],
        year: Sequence[int] | Sequence[str],
    ) -> Iterator[tuple[str, str, str, int, float]]:
        filter = ixmp4.iamc.DataPoint.Filter()
        if len(region):
            filter["region"] = {"name__in": list(region)}
        if variable:
            # Filter both `variable` names and the same names with the meta prefix
            filter["variable"] = {
                "name__in": list(variable) + list(map(f"{META}{{}}".format, variable))
            }
        if len(unit):
            filter["unit"] = {"name__in": list(unit)}

        # - Retrieve the data.
        # - Remove the meta prefix, which does not affect the behaviour of this method
        #   and is not to be returned.
        data = (
            self.index[ts]
            .iamc.tabulate(**filter)
            .replace("^" + re.escape(META), "", regex=True)
        )

        # Protect against empty data
        if not data.empty:
            # Guard against empty year filter
            if len(year):
                data = data.loc[data["year"].isin(year)]

            if "subannual" not in data.columns:
                data = data.assign(subannual="Year")
            else:
                data = data.replace({"subannual": {None: "Year"}})

            # Select only the columns we're interested in
            data = data[["region", "variable", "unit", "subannual", "year", "value"]]

        # TODO Why would we iterate and yield tuples instead of returning the whole df?
        for row in data.itertuples(index=False, name=None):
            yield row

    def delete(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        years: Iterable[int],
        unit: str,
    ) -> None:
        # Retrieve existing time series data matching `region`, `variable`, `years`,
        # `unit`, and `subannual`.
        # NOTE Here we *do not* include `META`-prefixed forms of the `variable` name.
        #      The expected behaviour is that these data cannot be deleted.
        df_to_delete = self._backend.iamc.datapoints.tabulate(
            join_run_id=True,  # Needed to filter on run, below
            join_parameters=True,  # Needed to filter on region, variable, and unit
            run={"id": self.index[ts].id, "default_only": False},
            region={"name": region},
            variable={"name": variable},
            unit={"name": unit},
            type="ANNUAL" if subannual == "Year" else subannual,
            # NB This may not work if type is something other than "ANNUAL"; then the
            #    step_category column may be present instead.
            step_year__in=list(years),
        )

        # Delete these data
        self._backend.iamc.datapoints.bulk_delete(df_to_delete)

    # Handle I/O

    def write_file(
        self, path: PathLike[str], item_type: ItemType, **kwargs: Unpack["WriteKwargs"]
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
            # Convert `filters` argument to keyword arguments for tabulate()
            default = filters.pop("default")

            # TODO (How) Should we include this?
            # export_all_runs = filters.pop("export_all_runs")

            _kwargs: "FacadeDataPointFilter" = dict(run={"default_only": default})

            for filter_name in IAMC_IDX:
                # NOTE this is what we get for not differentiating e.g. scenario vs
                # scenarios in filters...
                value = filters.pop(filter_name)  # type: ignore
                filter = value if filter_name != "scenario" else set(value)

                # ixmp4's "name__in" with an empty list will exclude all data
                if bool(filter):
                    _kwargs[filter_name] = {"name__in": filter}  # type: ignore

            expected_columns = [
                "MODEL",
                "SCENARIO",
                "VERSION",
                "VARIABLE",
                "UNIT",
                "REGION",
                "META",
                "SUBANNUAL",
                "YEAR",
                "VALUE",
            ]

            # - Retrieve data, including id, time_series__id, and type columns.
            # - Rename columns to upper case.
            # - Populate a 'META' column with boolean values.
            # - Rename 'STEP_YEAR' to the expected 'YEAR'
            data = (
                self._backend.iamc.datapoints.tabulate(
                    join_parameters=True,
                    join_runs=True,
                    **_kwargs,  # type: ignore
                )
                .rename(columns=lambda c: c.upper())
                .assign(META=lambda df: df.VARIABLE.str.startswith(META))
                .rename(columns={"STEP_YEAR": "YEAR"})
            )

            # Guard against entirely empty data selection
            if data.empty:
                data = pd.DataFrame(
                    columns=["ID", "TIME_SERIES__ID", "TYPE"] + expected_columns
                )

            # NOTE We don't handle step_datetime here

            # Handle 'subannual' values
            if "STEP_CATEGORY" not in data.columns:
                data["STEP_CATEGORY"] = None

            # - Populate the SUBANNUAL column using either STEP_CATEGORY or TYPE.
            # - Select and sort columns to match behaviour of JDBCBackend.
            #   Alternatively, check_like=True might work in test/assert_frame_equal.
            # - Write to CSV.
            data.assign(
                SUBANNUAL=lambda df: df.STEP_CATEGORY.combine_first(df.TYPE).replace(
                    {"ANNUAL": "Year"}
                )
            )[expected_columns].to_csv(path_or_buf=_path, index=False)
        else:
            raise NotImplementedError

    def read_file(
        self, path: PathLike[str], item_type: ItemType, **kwargs: Unpack["ReadKwargs"]
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

            self.cache_invalidate(ts)
        else:
            raise NotImplementedError(path, item_type)

    # The below methods of base.Backend are not yet implemented
    def _ni(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    # Handle geo data
    # NOTE While these functions are defined in the JDBC, they are not called by user
    # code or in our test suite. ixmp4 doesn't support them due to this lack of use.

    delete_geo = _ni
    get_geo = _ni
    set_geo = _ni

    # Handle timeslices
    # NOTE On JDBC, timeslices were defined on a Platform, whereas in ixmp4, timeslices
    # are defined when adding timeseries data to a Run that includes some. Thus,
    # set_timeslice() can't work in its true form, while get_timeslices() *can* return
    # timeslices defined for all Runs on this platform -- though these timeslices are
    # not necessarily used in the Run one is studying, they can be used within needing
    # to register them first.
    # NOTE Also that this is defined on a DB level for every instance in ixmp_source:
    # {"name": "Year", "category": "Common", "duration": 1.0}. Due to the requirement of
    # linking to a Run, we can't do the same in ixmp4.

    def set_timeslice(self, name: str, category: str, duration: float) -> None:
        # NOTE timeslices are called "Categorical Datapoints" in ixmp4. New ones are
        # registered automatically when added in iamc-datapoints, but this is also the
        # only way to register them.
        log.info(
            "Timeslices are added automatically as Categorical Datapoints in ixmp4!"
        )
        pass

    def get_timeslices(self) -> Iterator[tuple[str, str, float]]:
        # NOTE meksor suggests running something like
        # # SELECT DISTINCT step_category FROM datapoints WHERE step_category != NULL
        # in ixmp4 to retrieve the data wanted here. This only returns the 'name',
        # though, so I'm using this query which returns all data.
        # However, ixmp4 doesn't store 'category' as intended, and 'step_year' is marked
        # as Integer in the DB, so this information is only correct when
        # 'name' == 'category' and type(duration) == int

        datapoint_df = self._platform.iamc.tabulate(run={"default_only": False})

        for row in datapoint_df.itertuples():
            name = getattr(row, "step_category", None)
            category = name
            duration = getattr(row, "step_year", None)
            if name is not None and duration is not None:
                # These conversions should be noops except for duration, see above
                yield (str(name), str(category), float(duration))
