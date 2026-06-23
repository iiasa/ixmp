import gc
import logging
import os
import re
from collections import ChainMap
from collections.abc import (
    Callable,
    Generator,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence,
)
from copy import copy
from functools import lru_cache
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, overload
from weakref import WeakKeyDictionary

import jpype
import numpy as np
import pandas as pd

# TODO Import from typing when dropping support for Python 3.11
from typing_extensions import Unpack, override

from ixmp.backend.base import CachingBackend
from ixmp.backend.common import FIELDS, CrossPlatformClone, ItemType
from ixmp.core.item import CLASS as ITEM_CLASS
from ixmp.core.item import Equation, Item, Parameter, Set, Variable
from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries
from ixmp.util import as_str_list
from ixmp.util.pandas import STRING_DTYPE

from .jvm import (
    handle_jexception,
    java,
    raise_jexception,
    start_jvm,
    to_jlist,
    to_pylist,
    unwrap,
    wrap,
)
from .options import DRIVER, Options

if TYPE_CHECKING:
    from ixmp.types import (
        Filters,
        ParData,
        ReadKwargs,
        SetData,
        SolutionData,
        VersionType,
        WriteKwargs,
    )

__all__ = [
    "DRIVER",
    "JDBCBackend",
    "Options",
]

log = logging.getLogger(__name__)

_EXCEPTION_VERBOSE = os.environ.get("IXMP_JDBC_EXCEPTION_VERBOSE", "0") == "1"

#: Whether to collect garbage aggressively when instances of TimeSeries die.
#: See :meth:`JDBCBackend.gc`.
_GC_AGGRESSIVE = True

#: Map of Python to Java log levels
#: https://logging.apache.org/log4j/2.x/log4j-api/apidocs/org/apache/logging/log4j/Level.html
LOG_LEVELS = {
    "CRITICAL": "FATAL",
    "ERROR": "ERROR",
    "WARNING": "WARN",
    "INFO": "INFO",
    "DEBUG": "DEBUG",
    "NOTSET": "ALL",
}


@lru_cache
def _fixed_index_sets(scheme: str) -> Mapping[str, list[str]]:
    """Return index sets for items that are fixed in the Java code.

    See :meth:`JDBCBackend.init_item`. The return value is cached so the method is only
    called once.
    """
    if scheme == "MESSAGE":
        return {
            k: to_pylist(v)
            for k, v in java.ixmp.modelspecs.MESSAGEspecs.getIndexDimMap().items()
        }
    else:
        return {}


def _domain_enum(domain: str) -> str:
    domain_enum = java.ixmp.dto.DocumentationKey.DocumentationDomain
    try:
        # NOTE in truth, _domain seems to only be a compatible Java type
        _domain: str = domain_enum.valueOf(domain.upper())
        return _domain
    except java.lang.IllegalArgumentException:
        domains = ", ".join([d.name().lower() for d in domain_enum.values()])
        raise ValueError(f"No such domain: {domain}, existing domains: {domains}")


class JDBCBackend(CachingBackend):
    """Backend using JPype/JDBC to connect to Oracle and HyperSQL databases.

    This backend is based on the third-party `JPype <https://jpype.readthedocs.io>`_
    Python package that allows interaction with Java code.


    Parameters
    ----------
    jvmargs : str, optional
        Java Virtual Machine arguments. See :func:`.start_jvm` and
        :attr:`.Options.jvmargs`.
    dbprops : os.PathLike
        Path to a database properties file containing connection information.
        See :meth:`.Options.from_file`.
    cache : bool
        Passed to :class:`CachingBackend` py:`cache_enabled=...` to cache Python objects
        after conversion from Java objects.
    log_level :
        Initial log level. See :meth:`set_log_level`.

    Other parameters
    ----------------
    kwargs :
         including `driver`, `path`, `url`, `user`, `password`, `extra_properties`.
         Passed to :class:`~.backend.jdbc.options.Options`; see its documentation.
    """

    _options: Options

    #: Reference to the :py:`at.ac.iiasa.ixmp.Platform` Java object.
    jobj: jpype.JObject = None  # type: ignore [no-any-unimported]

    #: Mapping from :class:`.TimeSeries` (Python) instances to the underlying/
    #: corresponding Java :py:`at.ac.iiasa.ixmp.TimeSeries` object (or subclasses of
    #: either).
    jindex: MutableMapping[TimeSeries | Scenario, jpype.JObject] = (  # type: ignore[no-any-unimported]
        WeakKeyDictionary()
    )

    def __init__(
        self,
        jvmargs: str | list[str] | None = None,
        dbprops: os.PathLike[str] | None = None,
        cache: bool = True,
        log_level: int | str | None = None,
        **kwargs: Any,
    ) -> None:
        # Extract a log_level keyword argument before _create_properties(). By default,
        # use the same level as the 'ixmp' logger, whatever that has been set to.
        ixmp_logger = logging.getLogger("ixmp")
        log_level = log_level or ixmp_logger.getEffectiveLevel()

        # Handle arguments, create a Config object, and store for later reference
        try:
            self._options = (
                Options.from_file(dbprops, jvmargs or "")
                if dbprops
                else Options(**kwargs, jvmargs=jvmargs or "")
            )
        except TypeError as e:
            raise TypeError(e.args[0].replace("Options.__init__", "JDBCBackend"))

        # Start the JVM
        start_jvm(self._options.jvmargs)

        # Invoke the parent constructor to initialize the cache
        super().__init__(cache_enabled=cache)

        log.info(f"launching ixmp.Platform connected to {self._options.full_url}")

        try:
            # Instantiate the Java Platform object
            self.jobj = java.ixmp.Platform("Python", self._options.properties)
        except java.lang.NoClassDefFoundError as e:  # pragma: no cover
            raise NameError(
                f"{e}\nCheck that dependencies of ixmp.jar are "
                f"included in {Path(__file__).parents[2] / 'lib'}"
            )
        except java.lang.Exception as e:  # pragma: no cover
            # Handle Java exceptions
            jclass = e.__class__.__name__
            if jclass.endswith("HikariPool.PoolInitializationException"):
                # See https://github.com/python/mypy/issues/6019 for why we need a dict
                # here
                redacted = kwargs | dict(user="(HIDDEN)", password="(HIDDEN)")
                msg = f"unable to connect to database:\n{redacted!r}"
            elif jclass.endswith("FlywayException"):
                msg = "when initializing database:"
                if "applied migration" in e.args[0]:
                    msg += (
                        "\n\nThe schema of the database does not match the schema of "
                        "this version of ixmp. To resolve, either install the version "
                        "of ixmp used to create the database, or delete it and retry."
                    )
            else:
                raise_jexception(e)
            raise RuntimeError(f"{msg}\n(Java: {jclass})")

        # Set the log level
        self.set_log_level(log_level)

    def __del__(self) -> None:
        self.close_db()

    @classmethod
    def gc(cls) -> None:
        """Collect garbage."""
        if _GC_AGGRESSIVE:
            # log.debug('Collect garbage')
            try:
                java.lang.System.gc()
            except jpype.JVMNotRunning:
                pass
            gc.collect()
        # else:
        #     log.debug('Skip garbage collection')

    # Platform methods
    @classmethod
    def handle_config(
        cls, args: Sequence[Any], kwargs: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle configuration arguments from file or the command line.

        See :meth:`.backend.jdbc.Options.handle_config`
        """
        return Options.handle_config(args, **kwargs)

    def set_log_level(self, level: int | str) -> None:
        # Set the level of the 'ixmp.backend.jdbc' logger. Messages are handled by the
        # 'ixmp' logger; see ixmp/__init__.py.
        log.setLevel(level)

        # Translate to Java log level and set
        if isinstance(level, int):
            level = logging.getLevelName(level)
        self.jobj.setLogLevel(LOG_LEVELS[level])

    def get_log_level(self) -> str:
        levels = {v: k for k, v in LOG_LEVELS.items()}
        return levels.get(self.jobj.getLogLevel(), "UNKNOWN")

    def set_doc(
        self, domain: str, docs: dict[str, str] | Iterable[tuple[str, str]]
    ) -> None:
        dd = _domain_enum(domain)
        jdata = java.util.LinkedHashMap()
        if isinstance(docs, dict):
            docs = list(docs.items())
        for k, v in docs:
            jdata.put(str(k), str(v))
        self.jobj.setDoc(dd, jdata)

    def get_doc(self, domain: str, name: str | None = None) -> str | dict[str, str]:
        dd = _domain_enum(domain)
        if name is None:
            doc = self.jobj.getDoc(dd)
            return {entry.getKey(): entry.getValue() for entry in doc.entrySet()}
        else:
            doc = self.jobj.getDoc(dd, str(name))
            assert isinstance(doc, str)
            return doc

    def open_db(self) -> None:
        """(Re-)open the database connection."""
        self.jobj.openDB()

    def close_db(self) -> None:
        """Close the database connection.

        A HyperSQL database can only be used by one :class:`Backend` instance at a time.
        Any existing connection must be closed before a new one can be opened.
        """
        try:
            self.jobj.closeDB()
        except (AttributeError, ImportError):
            # self.jobj is None, e.g. cleanup after __init__ fails
            pass
        except Exception as e:  # pragma: no cover
            # JVM has already shut down, e.g. on program exit. At this point, the
            # `jpype` global is None, so we cannot check its type in the above block
            if str(e) != "Java Virtual Machine is not running":
                print(str(e))

    def get_auth(self, user: str, models: Iterable[str], kind: str) -> dict[str, bool]:
        model_access = self.jobj.checkModelAccess(user, kind, to_jlist(models))
        # NOTE Can't isinstance()-check parametrized generic yet; java HasMap is
        # returned in truth, but unusable as return type (becomes Any)
        assert isinstance(model_access, Mapping)
        return cast(dict[str, bool], model_access)

    def set_node(
        self,
        name: str,
        parent: str | None = None,
        hierarchy: str | None = None,
        synonym: str | None = None,
    ) -> None:
        if parent and hierarchy and not synonym:
            self.jobj.addNode(name, parent, hierarchy)
        elif synonym and not (parent or hierarchy):
            self.jobj.addNodeSynonym(synonym, name)

    def get_nodes(self) -> Generator[tuple[str, str | None, str, str]]:
        for r in self.jobj.listNodes("%"):
            n, p, h = r.getName(), r.getParent(), r.getHierarchy()
            yield (n, None, p, h)
            yield from [(s, n, p, h) for s in (r.getSynonyms() or [])]

    def get_timeslices(self) -> Generator[tuple[str, str, float], Any, None]:
        for r in self.jobj.getTimeslices():
            name, category, duration = (r.getName(), r.getCategory(), r.getDuration())
            yield name, category, duration

    def set_timeslice(self, name: str, category: str, duration: float) -> None:
        self.jobj.addTimeslice(name, category, java.lang.Double(duration))

    def add_model_name(self, name: str) -> None:
        self.jobj.addModel(str(name))

    def add_scenario_name(self, name: str) -> None:
        self.jobj.addScenario(str(name))

    def get_model_names(self) -> Generator[str, None, None]:
        for model in self.jobj.listModels():
            yield str(model)

    def get_scenario_names(self) -> Generator[str, None, None]:
        for scenario in self.jobj.listScenarios():
            yield str(scenario)

    def get_scenarios(
        self, default: bool, model: str | None, scenario: str | None
    ) -> Generator[list[bool | int | str], Any, None]:
        # List<Map<String, Object>>
        with handle_jexception():
            scenarios = self.jobj.getScenarioList(default, model, scenario)

        for s in scenarios:
            data = []
            for _field in FIELDS["get_scenarios"]:
                data.append(int(s[_field]) if _field == "version" else s[_field])
            yield data

    def set_unit(self, name: str, comment: str) -> None:
        try:
            self.jobj.addUnitToDB(name, comment)
        except Exception as e:  # pragma: no cover
            if "Error assigning an unit-key-id mapping" in str(e) and "" == str(name):
                # ixmp_source does not support adding "" with Oracle
                log.warning(f"…skip {repr(name)} (ixmp.JDBCBackend with driver=oracle)")
            else:
                raise_jexception(e)

    def get_units(self) -> list[str]:
        return to_pylist(self.jobj.getUnitList())

    @override
    def read_file(
        self,
        path: Path,  # type: ignore[override]
        item_type: ItemType,
        **kwargs: Unpack["ReadKwargs"],
    ) -> None:
        """Read Platform, TimeSeries, or Scenario data from file.

        JDBCBackend supports reading from:

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

        ts, filters = self._handle_rw_filters(kwargs.pop("filters", {}))
        if path.suffix == ".gdx" and item_type is ItemType.MODEL:
            kw = {"check_solution", "comment", "equ_list", "var_list"}

            if not isinstance(ts, Scenario):  # pragma: no cover
                raise ValueError("read from GDX requires a Scenario object")
            elif set(kwargs.keys()) != kw:
                raise ValueError(
                    f"keyword arguments {kwargs.keys()} do not match required {kw}"
                )

            args = (
                str(path.parent),
                path.name,
                kwargs.pop("comment"),
                to_jlist(kwargs.pop("var_list")),
                to_jlist(kwargs.pop("equ_list")),
                kwargs.pop("check_solution"),
            )

            # NOTE This test seems unnecessary with the 'elif' clause above
            if len(kwargs):
                raise ValueError(f"extra keyword arguments {kwargs}")

            with handle_jexception():
                self.jindex[ts].readSolutionFromGDX(*args)

            self.cache_invalidate(ts)
        else:
            raise NotImplementedError(path, item_type)

    @override
    def write_file(
        self,
        path: os.PathLike[str],
        item_type: ItemType,
        **kwargs: Unpack["WriteKwargs"],
    ) -> None:
        """Write Platform, TimeSeries, or Scenario data to file.

        JDBCBackend supports writing to:

        - ``path='*.gdx', item_type=ItemType.SET | ItemType.PAR``.
        - ``path='*.csv', item_type=TS``. The `default` keyword argument is
          **required**.

        Other parameters
        ----------------
        filters : dict of dict of str
            Restrict items written. The following filters may be used:

            - model : str
            - scenario : str
            - variable : list of str
            - default : bool. If :obj:`True`, only data from TimeSeries
              versions with :meth:`.TimeSeries.set_as_default` are written.

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

        _path = Path(path)

        ts, filters = self._handle_rw_filters(kwargs.pop("filters", {}))
        if _path.suffix == ".gdx" and item_type is ItemType.SET | ItemType.PAR:
            if len(filters) > 1:  # pragma: no cover
                raise NotImplementedError("write to GDX with filters")
            elif not isinstance(ts, Scenario):  # pragma: no cover
                raise ValueError("write to GDX requires a Scenario object")

            # include_var_equ=False -> do not include variables/equations in GDX
            self.jindex[ts].toGDX(str(_path.parent), _path.name, False)
        elif _path.suffix == ".csv" and item_type is ItemType.TS:
            models = set(filters.pop("model"))
            # NOTE this is what we get for not differentiating e.g. scenario vs
            # scenarios in filters...
            scenarios = set(cast(list[str], filters.pop("scenario")))
            variables = filters.pop("variable")
            units = filters.pop("unit")
            regions = filters.pop("region")
            default = filters.pop("default")
            export_all_runs = filters.pop("export_all_runs")

            scen_list = self.jobj.getScenarioList(default, None, None)
            # TODO replace with passing list of models/scenarios to the method above
            run_ids = [
                s["run_id"]
                for s in scen_list
                if (len(scenarios) == 0 or s["scenario"] in scenarios)
                and (len(models) == 0 or s["model"] in models)
            ]
            self.jobj.exportTimeseriesData(
                to_jlist(run_ids),
                to_jlist(variables),
                to_jlist(units),
                to_jlist(regions),
                str(_path),
                export_all_runs,
            )
        else:
            raise NotImplementedError

    # Timeseries methods

    def _index_and_set_attrs(self, jobj: jpype.JObject, ts: TimeSeries) -> None:  # type: ignore[no-any-unimported]
        """Add *jobj* to index and update attributes of *ts*.

        Helper for init and get.
        """
        # Add to index
        self.jindex[ts] = jobj

        # Retrieve the version of the Java object
        v = jobj.getVersion()
        if ts.version is None:
            # The default version was requested; update the attribute
            ts.version = v
        elif v != ts.version:  # pragma: no cover
            # Something went wrong on the Java side
            raise RuntimeError(f"got version {v} instead of {ts.version}")

        if isinstance(ts, Scenario):
            # Also retrieve the scheme
            s = jobj.getScheme()

            if ts.scheme and s != ts.scheme:  # pragma: no cover
                # Something went wrong on the Java side
                raise RuntimeError(f"got scheme {s} instead of {ts.scheme}")

            ts.scheme = s

    def _validate_meta_args(
        self,
        model: str | None,
        scenario: str | None,
        version: int | str | None,
    ) -> None:
        """Validate arguments for getting/setting/deleting meta"""
        valid = False
        if model and not scenario and version is None:
            valid = True
        elif scenario and not model and version is None:
            valid = True
        elif model and scenario and version is None:
            valid = True
        elif model and scenario and version is not None:
            valid = True
        if not valid:
            raise ValueError(
                "Invalid arguments. Valid combinations are: (model), (scenario), "
                "(model, scenario), (model, scenario, version)"
            )

    def init(self, ts: TimeSeries, annotation: str) -> None:
        klass = ts.__class__.__name__

        # Final arguments: scheme only for Scenarios
        args = [ts.scheme, annotation] if klass == "Scenario" else [annotation]

        # Call either newTimeSeries or newScenario
        method = getattr(self.jobj, "new" + klass)
        with handle_jexception():
            jobj = method(ts.model, ts.scenario, *args)

        self._index_and_set_attrs(jobj, ts)

    def get(self, ts: TimeSeries) -> None:
        args: list[int | str] = [ts.model, ts.scenario]
        if ts.version is not None:
            # Load a TimeSeries of specific version
            args.append(ts.version)

        # either getTimeSeries or getScenario
        method = getattr(self.jobj, "get" + ts.__class__.__name__)

        # Re-raise as a ValueError for bad model or scenario name, or other with
        # with _handle_jexception():
        try:
            # Either the 2- or 3- argument form, depending on args
            jobj = method(*args)
        except SystemError:
            # JPype 1.5.0 with Python 3.12: "<built-in method __subclasscheck__ of
            # _jpype._JClass object at …> returned a result with an exception set"
            # At least transmute to a ValueError
            raise ValueError("model, scenario, or version not found")
        except BaseException as e:
            raise_jexception(e)

        self._index_and_set_attrs(jobj, ts)

    def del_ts(self, ts: TimeSeries) -> None:
        super().del_ts(ts)

        # Aggressively free memory
        self.gc()
        self.jindex.pop(ts, None)

    def check_out(self, ts: TimeSeries, timeseries_only: bool) -> None:
        with handle_jexception():
            self.jindex[ts].checkOut(timeseries_only)

    def commit(self, ts: TimeSeries, comment: str) -> None:
        try:
            self.jindex[ts].commit(comment)
        except java.lang.Exception as e:
            arg = e.args[0]
            if isinstance(arg, str) and "this Scenario is not checked out" in arg:
                raise RuntimeError(arg)
            else:  # pragma: no cover
                raise_jexception(e)
        if ts.version == 0:
            ts.version = self.jindex[ts].getVersion()

    def discard_changes(self, ts: TimeSeries) -> None:
        self.jindex[ts].discardChanges()

    def set_as_default(self, ts: TimeSeries) -> None:
        self.jindex[ts].setAsDefaultVersion()

    def is_default(self, ts: TimeSeries) -> bool:
        return bool(self.jindex[ts].isDefault())

    def last_update(self, ts: TimeSeries) -> str | None:
        timestamp = self.jindex[ts].getLastUpdateTimestamp()
        if timestamp is not None:
            return cast(str, timestamp.toString())
        else:
            return timestamp  # None

    def run_id(self, ts: TimeSeries) -> int:
        id = self.jindex[ts].getRunId()
        assert isinstance(id, int)
        return id

    def preload(self, ts: TimeSeries) -> None:
        self.jindex[ts].preloadAllTimeseries()

    def get_data(
        self,
        ts: TimeSeries,
        region: Sequence[str],
        variable: Sequence[str],
        unit: Sequence[str],
        year: Sequence[int] | Sequence[str],
    ) -> Generator[tuple[str, str, str, int, float], Any, None]:
        # Convert the selectors to Java lists
        r = to_jlist(region)
        v = to_jlist(variable)
        u = to_jlist(unit)
        y = to_jlist(year)

        # Field types
        ftype = {"year": int, "value": float}

        # Iterate over returned rows
        for row in self.jindex[ts].getTimeseries(r, v, u, None, y):
            # Get the value of each field and maybe convert its type
            yield tuple(
                ftype.get(f, str)(getattr(row, "get" + f.capitalize())())
                for f in FIELDS["ts_get"]
            )

    def get_geo(
        self, ts: TimeSeries
    ) -> Generator[tuple[str, str, int, str, str, str, bool], Any, None]:
        # NB the return type of getGeoData() requires more processing than
        #    getTimeseries. It also accepts no selectors.

        # Field types
        ftype: dict[str, Callable[..., Any]] = {
            "meta": int,
            "year": lambda obj: obj,  # Pass through; handled later
        }

        # Returned names in Java data structure do not match API column names
        jname = {
            "meta": "meta",
            "region": "nodeName",
            "subannual": "subannual",
            "unit": "unitName",
            "variable": "keyString",
            "year": "yearlyData",
        }

        # Iterate over rows from the Java backend
        for row in self.jindex[ts].getGeoData():
            data1 = {
                f: ftype.get(f, str)(row.get(jname.get(f, f)))
                for f in FIELDS["ts_get_geo"]
                if f != "value"
            }

            # At this point, the 'year' key is a not a single value, but a year ->
            # value mapping with multiple entries
            yv_entries = data1.pop("year").entrySet()

            # Construct a chain map: look up in data1, then data2
            data2 = {"year": None, "value": None}
            cm = ChainMap(data1, data2)

            for yv in yv_entries:
                # Update data2
                data2["year"] = yv.getKey()
                data2["value"] = yv.getValue()

                # Construct a row with a single value
                yield tuple(cm[f] for f in FIELDS["ts_get_geo"])

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
        # Oracle is unable to handle ±∞ (issue #442)
        if self._options.driver is DRIVER.oracle and any(map(np.isinf, data.values())):
            raise ValueError(
                f"± infinity (at region={region}, variable={variable}) cannot be stored"
                " in an Oracle database using JDBCBackend"
            )

        # Convert *data* to a Java data structure. Explicitly cast the key (period) to
        # Integer so JPype does not produce invalid java.lang.Long.
        jdata = java.util.LinkedHashMap(
            {java.lang.Integer(k): v for k, v in data.items()}
        )

        try:
            self.jindex[ts].addTimeseries(
                region, variable, subannual, jdata, unit, meta
            )
        except java.ixmp.exceptions.IxException as e:
            match = re.search("node '([^']*)' does not exist in the database", str(e))
            if match:
                raise ValueError(f"region = {match.group(1)}") from None
            else:
                raise

    def set_geo(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        year: int,
        value: str,
        unit: str,
        meta: bool,
    ) -> None:
        self.jindex[ts].addGeoData(
            region, variable, subannual, java.lang.Integer(year), value, unit, meta
        )

    def delete(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        years: Iterable[int],
        unit: str,
    ) -> None:
        years = to_jlist(years, java.lang.Integer)
        self.jindex[ts].removeTimeseries(region, variable, subannual, years, unit)

    def delete_geo(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        years: Iterable[int],
        unit: str,
    ) -> None:
        years = to_jlist(years, java.lang.Integer)
        self.jindex[ts].removeGeoData(region, variable, subannual, years, unit)

    # Scenario methods

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
        # Raise exceptions for limitations of JDBCBackend
        if not isinstance(platform_dest._backend, type(self)):
            raise CrossPlatformClone(  # pragma: no cover
                f"Clone between {self.__class__} and {platform_dest._backend.__class__}"
            )
        elif platform_dest._backend is not self:
            package = s.__class__.__module__.split(".")[0]
            msg = f"Clone of {package}.Scenario between JDBCBackend with"
            if keep_solution is False:
                raise NotImplementedError(f"{msg} `keep_solution=False`")
            elif "message_ix" in msg and first_model_year is not None:
                raise NotImplementedError(f"{msg} first_model_year != None")

        # Prepare arguments
        args = [platform_dest._backend.jobj, model, scenario, annotation, keep_solution]
        if first_model_year:
            args.append(first_model_year)

        # Reference to the cloned Java object
        jclone = self.jindex[s].clone(*args)

        # Instantiate same class as the original object
        return s.__class__(
            platform_dest,
            model,
            scenario,
            version=jclone.getVersion(),
            scheme=jclone.getScheme(),
        )

    def has_solution(self, s: Scenario) -> bool:
        result = self.jindex[s].hasSolution()
        assert isinstance(result, bool)
        return result

    def list_items(self, s: Scenario, type: str) -> list[str]:
        return to_pylist(getattr(self.jindex[s], f"get{type.title()}List")())

    def init_item(
        self,
        s: Scenario,
        type: str,
        name: str,
        idx_sets: Sequence[str],
        idx_names: Sequence[str] | None,
    ) -> None:
        # Check `idx_sets` against values hard-coded in ixmp_source
        try:
            sets = _fixed_index_sets(s.scheme)[name]
        except KeyError:
            pass
        else:
            if idx_sets == sets:
                # Match → provide empty lists for idx_sets and idx_names. ixmp_source
                # raises an exception if any values—even correct ones—are given.
                idx_sets = idx_names = []
            else:
                raise NotImplementedError(
                    f"Initialize {type} {name!r} with dimensions {idx_sets} != {sets}"
                )

        # Convert to Java data structures
        java_idx_sets = to_jlist(idx_sets) if len(idx_sets) else None
        java_idx_names = to_jlist(idx_names) if idx_names else java_idx_sets

        # Retrieve the method that initializes the Item, something like "initializePar"
        func = getattr(self.jindex[s], f"initialize{type.title()}")

        # The constructor returns a reference to the Java Item, but these aren't exposed
        # by Backend, so don't return here
        try:
            func(name, java_idx_sets, java_idx_names)
        except java.lang.Exception as e:
            if "already exists" in e.args[0]:
                raise ValueError(f"{repr(name)} already exists")
            else:
                raise_jexception(e)

    def delete_item(
        self, s: Scenario, type: Literal["set", "par", "equ"], name: str
    ) -> None:
        try:
            getattr(self.jindex[s], f"remove{type.title()}")(name)
        except jpype.JException as e:
            if "There exists no" in e.args[0]:
                raise KeyError(name)
            else:  # pragma: no cover
                raise_jexception(e)
        self.cache_invalidate(s, type, name)

    def item_index(
        self, s: Scenario, name: str, sets_or_names: Literal["sets", "names"]
    ) -> list[str]:
        jitem = self._get_item(s, Item(name), load=False)
        return list(getattr(jitem, f"getIdx{sets_or_names.title()}")())

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["set"],
        name: str,
        filters: "Filters" = None,
    ) -> "SetData": ...

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["par"],
        name: str,
        filters: "Filters" = None,
    ) -> "ParData": ...

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["equ", "var"],
        name: str,
        filters: "Filters" = None,
    ) -> "SolutionData": ...

    # FIXME reduce complexity 16 → ≤13
    def item_get_elements(  # noqa: C901
        self, s: Scenario, ix_type: str, name: str, filters: "Filters" = None
    ) -> "SetData | ParData | SolutionData":
        if filters:
            # Convert filter elements to strings
            filters = {dim: as_str_list(ele) for dim, ele in filters.items()}

        # Try returning a cached value
        cached_value = self.maybe_get_cache(
            ts=s, ix_type=ix_type, name=name, filters=filters
        )
        if cached_value is not None:
            return cached_value

        # Retrieve the item
        item = self._get_item(s, ITEM_CLASS[ix_type](name), load=True)
        idx_names = list(item.getIdxNames())
        idx_sets = list(item.getIdxSets())

        # Get list of elements, using filters if provided
        if filters is not None:
            jFilter = java.util.HashMap()

            for idx_name, values in filters.items():
                # Retrieve the elements of the index set as a list
                idx_set_name = idx_sets[idx_names.index(idx_name)]
                idx_set = self.item_get_elements(s, "set", idx_set_name)
                assert isinstance(idx_set, pd.Series)
                elements = idx_set.tolist()

                # Filter for only included values and store
                filtered_elements: Iterable[float | int | str] = filter(
                    lambda e: e in values, elements
                )
                jFilter.put(idx_name, to_jlist(filtered_elements))

            jList = item.getElements(jFilter)
        else:
            jList = item.getElements()

        result: "SetData" | "ParData" | "SolutionData"

        if item.getDim() > 0:
            # Mapping set or multi-dimensional equation, parameter, or variable
            columns = copy(idx_names)

            # Prepare dtypes for index columns
            dtypes: dict[str, type[float] | type[int] | type[str]] = {}
            for idx_name, idx_set in zip(columns, idx_sets):
                # NB using categoricals could be more memory-efficient, but requires
                #    adjustment of tests/documentation. See
                #    https://github.com/iiasa/ixmp/issues/228
                # dtypes[idx_name] = CategoricalDtype(
                #     self.item_get_elements(s, 'set', idx_set))
                dtypes[idx_name] = str

            # Prepare dtypes for additional columns
            if ix_type == "par":
                columns.extend(["value", "unit"])
                dtypes.update(value=float, unit=str)
                # Same as above
                # dtypes['unit'] = CategoricalDtype(self.jobj.getUnitList())
            elif ix_type in ("equ", "var"):
                columns.extend(["lvl", "mrg"])
                dtypes.update(lvl=float, mrg=float)

            # Copy vectors from Java into pd.Series to form DataFrame columns
            columns = []

            def _get(method: str, name: str, *args: Any) -> None:
                # NB [:] causes JPype to use a faster code path
                java_array = getattr(item, f"get{method}")(*args, jList)[:]

                # Use numpy buffer protocol for numeric types (much faster)
                # String types must iterate element-by-element (JPype limitation)
                if dtypes[name] in (float, int):
                    java_array = np.array(java_array)

                columns.append(pd.Series(java_array, dtype=dtypes[name], name=name))

            # Index columns
            for i, idx_name in enumerate(idx_names):
                _get("Col", idx_name, i)

            # Data columns
            if ix_type == "par":
                _get("Values", "value")
                _get("Units", "unit")
            elif ix_type in ("equ", "var"):
                _get("Levels", "lvl")
                _get("Marginals", "mrg")

            result = pd.concat(columns, axis=1)
        elif ix_type == "set":
            # Index sets
            # dtype=object is to silence a warning in pandas 1.0
            result = pd.Series(item.getCol(0, jList)[:], dtype=STRING_DTYPE)
        elif ix_type == "par":
            # Scalar parameter
            result = dict(
                value=float(item.getScalarValue().floatValue()),
                unit=str(item.getScalarUnit()),
            )
        elif ix_type in ("equ", "var"):
            # Scalar equation or variable
            result = dict(
                lvl=float(item.getScalarLevel().floatValue()),
                mrg=float(item.getScalarMarginal().floatValue()),
            )

        # Store cache
        self.cache(s, ix_type, name, filters, result)

        return result

    def item_set_elements(
        self,
        s: Scenario,
        type: type[Equation | Parameter | Set | Variable],
        name: str,
        elements: Iterable[tuple[Any, float | None, str | None, str | None]],
    ) -> None:
        if type not in {Parameter, Set}:  # pragma: no cover
            raise NotImplementedError(f"Set elements of {type=} on JDBCBackend")

        jobj = self._get_item(s, type(name))

        try:
            for key, value, unit, comment in elements:
                # Prepare arguments
                args = [to_jlist(key)] if key else []
                if type is Parameter:
                    args.extend([java.lang.Double(value), unit])
                if comment:
                    args.append(comment)

                # Activates one of 5 signatures for addElement:
                # - set: (key)
                # - set: (key, comment)
                # - par: (key, value, unit, comment)
                # - par: (key, value, unit)
                # - par: (value, unit, comment)
                jobj.addElement(*args)
        except java.ixmp.exceptions.IxException as e:
            if any(s in e.args[0] for s in ("does not have an element", "The unit")):
                # Re-raise as Python ValueError
                raise ValueError(e.args[0]) from None
            elif "cannot be edited" in e.args[0]:
                raise RuntimeError(e.args[0])
            else:  # pragma: no cover
                raise_jexception(e)

        self.cache_invalidate(s, type.ix_type, name)

    def item_delete_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        keys: Iterable[Iterable[str]],
    ) -> None:
        item = ITEM_CLASS[type](name)
        jitem = self._get_item(s, item, load=False)

        # Process keys in batches to balance performance and memory usage
        # Batch size chosen to limit memory consumption while reducing method calls
        BATCH_SIZE = 1000000

        # Use islice for memory-efficient iteration without materializing all keys
        # TODO: Replace islice with itertools.batched() once Python 3.12 is min vers
        # ArrayList + explicit loop performs best for large batches
        ArrayList = jpype.JClass("java.util.ArrayList")

        keys_iter = iter(keys)
        while batch := list(islice(keys_iter, BATCH_SIZE)):
            if len(batch) == 1:
                # Extract single key from batch list: batch[0] is ["i1", "j1"]
                # to_jlist(batch[0]) creates LinkedList("i1", "j1") as expected
                # to_jlist(batch) would incorrectly create LinkedList([["i1", "j1"]])
                jitem.removeElement(to_jlist(batch[0]))
            elif type == "par":
                # Preallocate ArrayList capacity to avoid repeated memory allocations
                key_vectors = ArrayList(len(batch))
                for key in batch:
                    key_vectors.add(to_jlist(key))
                jitem.removeElements(key_vectors)
            else:
                for key in batch:
                    jitem.removeElement(to_jlist(key))

        # Since `name` may be an index set, clear the cache entirely. This ensures that
        # e.g. parameter elements for parameters indexed by `name` are also refreshed
        # on the next call to item_get_elements.
        args = (s,) if isinstance(item, Set) else (s, type, name)
        self.cache_invalidate(*args)

    def get_meta(
        self,
        model: str | None = None,
        scenario: str | None = None,
        version: "VersionType" = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.lang.Long(version)

        with handle_jexception():
            meta = self.jobj.getMeta(model, scenario, version, strict)

        return {entry.getKey(): unwrap(entry.getValue()) for entry in meta.entrySet()}

    def set_meta(
        self,
        meta: dict[str, bool | float | int | str],
        model: str | None = None,
        scenario: str | None = None,
        version: int | None = None,
    ) -> None:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.lang.Long(version)

        jmeta = java.util.HashMap()
        for k, v in meta.items():
            jmeta.put(str(k), wrap(v))

        with handle_jexception():
            self.jobj.setMeta(model, scenario, version, jmeta)

    def remove_meta(
        self,
        names: list[str],
        model: str | None = None,
        scenario: str | None = None,
        version: int | None = None,
    ) -> None:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.lang.Long(version)
        self.jobj.removeMeta(model, scenario, version, to_jlist(names))

    def clear_solution(self, s: Scenario, from_year: int | None = None) -> None:
        if from_year:
            if type(s) is not Scenario:
                raise TypeError(
                    "s_clear_solution(from_year=...) only valid for ixmp.Scenario; not "
                    "subclasses"
                )
            self.jindex[s].removeSolution(from_year)
        else:
            self.jindex[s].removeSolution()

        self.cache_invalidate(s)

    # MsgScenario methods

    def cat_list(self, ms: Scenario, name: str) -> list[str]:
        return to_pylist(self.jindex[ms].getTypeList(name))

    def cat_get_elements(self, ms: Scenario, name: str, cat: str) -> list[str]:
        return to_pylist(self.jindex[ms].getCatEle(name, cat))

    def cat_set_elements(
        self,
        ms: Scenario,
        name: str,
        cat: str,
        keys: str | Sequence[str],
        is_unique: bool,
    ) -> None:
        self.jindex[ms].addCatEle(name, cat, to_jlist(keys), is_unique)

    # Helpers; not part of the Backend interface

    def _get_item(self, s: Scenario, item: Item, load: bool = True) -> Any:
        """Return the Java object for item `name` of `ix_type`.

        Parameters
        ----------
        load : bool, optional
            If `ix_type` is 'par', 'var', or 'equ', the elements of the item are loaded
            from the database before :meth:`_item` returns. If :const:`False`, the
            elements can be loaded later using ``item.loadItemElementsfromDB()``.
        """
        # getItem is not overloaded to accept a second bool argument
        args = [item.name] + ([load] if item.ix_type != "item" else [])
        try:
            type_name = item.ix_type.title()
            return getattr(self.jindex[s], f"get{type_name}")(*args)
        except java.ixmp.exceptions.IxException as e:
            # Regex for similar but not consistent messages from Java code
            msg = f"No (item|{type_name}) '?{item.name}'? exists in this Scenario!"
            if re.match(msg, e.args[0]):
                # Re-raise as a Python KeyError
                raise KeyError(item.name) from None
            else:  # pragma: no cover
                raise_jexception(e)
