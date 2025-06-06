import gc
import logging
import os
import platform
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
from contextlib import contextmanager
from copy import copy
from functools import lru_cache
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import (
    Any,
    Literal,
    Optional,
    Union,
    Unpack,
    cast,
    overload,
    override,
)
from weakref import WeakKeyDictionary

import jpype
import numpy as np
import pandas as pd

from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries
from ixmp.types import (
    ItemTypeNames,
    JDBCBackendInitKwargs,
    ReadKwargs,
    VersionType,
    WriteKwargs,
)
from ixmp.util import as_str_list, filtered

from .base import CachingBackend
from .common import FIELDS, ItemType

log = logging.getLogger(__name__)

_EXCEPTION_VERBOSE = os.environ.get("IXMP_JDBC_EXCEPTION_VERBOSE", "0") == "1"

#: Whether to collect garbage aggressively when instances of TimeSeries die.
#: See :meth:`JDBCBackend.gc`.
_GC_AGGRESSIVE = True

# Map of Python to Java log levels
# https://logging.apache.org/log4j/2.x/log4j-api/apidocs/org/apache/logging/log4j/Level.html
LOG_LEVELS = {
    "CRITICAL": "FATAL",
    "ERROR": "ERROR",
    "WARNING": "WARN",
    "INFO": "INFO",
    "DEBUG": "DEBUG",
    "NOTSET": "ALL",
}

# Java classes, loaded by start_jvm(). These become available as e.g. java.IxException
# or java.HashMap.
java = SimpleNamespace()

JAVA_CLASSES = [
    "at.ac.iiasa.ixmp.dto.TimesliceDTO",
    "at.ac.iiasa.ixmp.exceptions.IxException",
    "at.ac.iiasa.ixmp.modelspecs.MESSAGEspecs",
    "at.ac.iiasa.ixmp.objects.Scenario",
    "at.ac.iiasa.ixmp.Platform",
    "java.lang.Double",
    "java.lang.Exception",
    "java.lang.Integer",
    "java.lang.NoClassDefFoundError",
    "java.lang.IllegalArgumentException",
    "java.lang.Long",
    "java.lang.Runtime",
    "java.lang.System",
    "java.math.BigDecimal",
    "java.util.HashMap",
    "java.util.LinkedHashMap",
    "java.util.LinkedList",
    "java.util.ArrayList",
    "java.util.Properties",
    "at.ac.iiasa.ixmp.dto.DocumentationKey",
]


DRIVER_CLASS = {
    "oracle": "oracle.jdbc.driver.OracleDriver",
    "hsqldb": "org.hsqldb.jdbcDriver",
}


def _create_properties(
    driver: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
    url: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> Any:
    """Create a database Properties from arguments."""
    properties = java.Properties()

    # Handle arguments
    try:
        # FIXME Improve handling of None driver value
        properties.setProperty("jdbc.driver", DRIVER_CLASS[driver])  # type:ignore[index]
    except KeyError:
        raise ValueError(f"unrecognized/unsupported JDBC driver {repr(driver)}")

    if driver == "oracle":
        if url is None or path is not None:
            raise ValueError("use JDBCBackend(driver='oracle', url=…)")

        full_url = f"jdbc:oracle:thin:@{url}"
    elif driver == "hsqldb":
        if path is None and url is None:
            raise ValueError("use JDBCBackend(driver='hsqldb', path=…)")

        if url is not None:
            if url.startswith("jdbc:hsqldb:"):
                full_url = url
            else:
                raise ValueError(url)
        else:
            # path can not also be None due to the check above, convince type checker
            assert path is not None
            # Convert Windows paths to use forward slashes per HyperSQL JDBC URL spec
            url_path = str(PurePosixPath(Path(path).resolve())).replace("\\", "")
            full_url = f"jdbc:hsqldb:file:{url_path}"
        user = user or "ixmp"
        password = password or "ixmp"

    properties.setProperty("jdbc.url", full_url)
    properties.setProperty("jdbc.user", user)
    properties.setProperty("jdbc.pwd", password)

    return properties


def _read_properties(file: Path) -> dict[str, str]:
    """Read database properties from *file*, returning :class:`dict`."""
    properties = dict()
    for line in file.read_text().split("\n"):
        match = re.search(r"([^\s]+)\s*=\s*(.+)\s*", line)
        if match is not None:
            properties[match.group(1)] = match.group(2)
    return properties


def _raise_jexception(exc: Any, msg: str = "unhandled Java exception: ") -> None:
    """Convert Java/JPype exceptions to ordinary Python RuntimeError."""
    # Try to re-raise as a ValueError for bad model or scenario name
    arg = exc.args[0] if isinstance(exc.args[0], str) else ""
    if match := re.search(r"getting '([^']*)' in table '([^']*)'", arg):
        param = match.group(2).lower()
        if param in {"model", "scenario"}:
            raise ValueError(f"{param}={repr(match.group(1))}") from None

    # Other exceptions
    if _EXCEPTION_VERBOSE:
        msg += "\n\n" + exc.stacktrace()
    else:
        msg += exc.message()

    raise RuntimeError(msg) from None


@contextmanager
def _handle_jexception() -> Generator[None, Any, None]:
    """Context manager form of :func:`_raise_jexception`."""
    try:
        yield
    except java.Exception as e:
        _raise_jexception(e)


@lru_cache
def _fixed_index_sets(scheme: str) -> Mapping[str, list[str]]:
    """Return index sets for items that are fixed in the Java code.

    See :meth:`JDBCBackend.init_item`. The return value is cached so the method is only
    called once.
    """
    if scheme == "MESSAGE":
        return {k: to_pylist(v) for k, v in java.MESSAGEspecs.getIndexDimMap().items()}
    else:
        return {}


def _domain_enum(domain: str) -> str:
    domain_enum = java.DocumentationKey.DocumentationDomain
    try:
        # NOTE in truth, _domain seems to only be a compatible Java type
        _domain: str = domain_enum.valueOf(domain.upper())
        return _domain
    except java.IllegalArgumentException:
        domains = ", ".join([d.name().lower() for d in domain_enum.values()])
        raise ValueError(f"No such domain: {domain}, existing domains: {domains}")


@overload
def _unwrap(v: list[Union[bool, float, str]]) -> list[Union[bool, float, str]]: ...


@overload
def _unwrap(v: Union[bool, float, str]) -> Union[bool, float, str]: ...


def _unwrap(v: Any) -> Union[bool, float, str, list[Union[bool, float, str]]]:
    """Unwrap meta numeric value or list of values (BigDecimal -> Double)."""
    if isinstance(v, java.BigDecimal):
        _v: float = v.doubleValue()
        return _v
    elif isinstance(v, java.ArrayList):
        return [_unwrap(elt) for elt in v]
    else:
        # NOTE In truth, this value might only be a compatible Java type
        else_v: Union[bool, str] = v
        return else_v


def _wrap(value: Any) -> Union[bool, float, int, str, list[Union[bool, float, str]]]:
    if isinstance(value, (str, bool)):
        return value
    elif isinstance(value, (int, float)):
        # NOTE In truth, BigDecimal seems to return a Java type compatible with both
        # float and int
        _value: Union[float, int] = java.BigDecimal(value)
        return _value
    elif isinstance(value, (Sequence, Iterable)):
        jlist = java.ArrayList()
        jlist.addAll([_wrap(elt) for elt in value])
        return cast(list[Union[bool, float, str]], jlist)
    else:
        raise ValueError(f"Cannot use value {value} as metadata")


class JDBCBackend(CachingBackend):
    """Backend using JPype/JDBC to connect to Oracle and HyperSQL databases.

    This backend is based on the third-party `JPype <https://jpype.readthedocs.io>`_
    Python package that allows interaction with Java code.

    Parameters
    ----------
    driver : 'oracle' or 'hsqldb'
        JDBC driver to use.
    path : os.PathLike, optional
        Path to the HyperSQL database.
    url : str, optional
        Partial or complete JDBC URL for the Oracle or HyperSQL database, e.g.
        ``database-server.example.com:PORT:SCHEMA``. See :ref:`configuration`.
    user : str, optional
        Database user name.
    password : str, optional
        Database user password.
    cache : bool, optional
        If :obj:`True` (the default), cache Python objects after conversion from Java
        objects.
    jvmargs : str, optional
        Java Virtual Machine arguments. See :func:`.start_jvm`.
    dbprops : os.PathLike, optional
        With ``driver='oracle'``, the path to a database properties file containing
        `driver`, `url`, `user`, and `password` information.
    """

    # NB Much of the code of this backend is in Java, in the iiasa/ixmp_source GitHub
    #    repository.
    #
    #    Among other abstractions, this backend:
    #
    #    - Handles any conversion between Java and Python types that is not done
    #      automatically by JPype.
    #    - Catches Java exceptions such as ixmp.exceptions.IxException, and re-raises
    #      them as appropriate Python exceptions.
    #
    #    Limitations:
    #
    #    - s_clone() is only supported when target_backend is JDBCBackend.

    #: Reference to the at.ac.iiasa.ixmp.Platform Java object.
    jobj: jpype.JObject = None  # type: ignore[no-any-unimported]

    #: Mapping from ixmp.TimeSeries object to the underlying at.ac.iiasa.ixmp.Scenario
    #: object (or subclasses of either).
    jindex: MutableMapping[Union[TimeSeries, Scenario], jpype.JObject] = (  # type: ignore[no-any-unimported]
        WeakKeyDictionary()
    )

    def __init__(
        self,
        jvmargs: Optional[Union[str, list[str]]] = None,
        dbprops: Optional[os.PathLike[str]] = None,
        cache: bool = True,
        log_level: Optional[Union[int, str]] = None,
        **kwargs: Unpack[JDBCBackendInitKwargs],
    ) -> None:
        properties: Optional[Any] = None

        # Handle arguments
        if dbprops:
            # Use an existing file
            _dbprops = Path(dbprops)
            if _dbprops.exists() and _dbprops.is_file():
                # Existing properties file
                properties = _read_properties(_dbprops)
                if "jdbc.url" not in properties:
                    raise ValueError("Config file contains no database URL")
            else:
                raise FileNotFoundError(_dbprops)

        start_jvm(jvmargs)

        # Invoke the parent constructor to initialize the cache
        super().__init__(cache_enabled=cache)

        # Extract a log_level keyword argument before _create_properties(). By default,
        # use the same level as the 'ixmp' logger, whatever that has been set to.
        ixmp_logger = logging.getLogger("ixmp")
        log_level = log_level or ixmp_logger.getEffectiveLevel()

        # Create a database properties object
        if properties:
            # ...using file contents
            new_props = java.Properties()
            [new_props.setProperty(k, v) for k, v in properties.items()]
            properties = new_props
        else:
            # ...from arguments
            try:
                properties = _create_properties(**kwargs)
            except TypeError as e:
                msg = e.args[0].replace("_create_properties", "JDBCBackend")
                raise TypeError(msg)

        # We seem to assume this
        assert properties is not None

        log.info(
            "launching ixmp.Platform connected to {}".format(
                properties.getProperty("jdbc.url")
            )
        )

        # Store a copy of the properties for later introspection
        self._properties = properties

        try:
            self.jobj = java.Platform("Python", properties)
        except java.NoClassDefFoundError as e:  # pragma: no cover
            raise NameError(
                f"{e}\nCheck that dependencies of ixmp.jar are "
                f"included in {Path(__file__).parents[2] / 'lib'}"
            )
        except java.Exception as e:  # pragma: no cover
            # Handle Java exceptions
            jclass = e.__class__.__name__
            if jclass.endswith("HikariPool.PoolInitializationException"):
                redacted = copy(kwargs)
                # See https://github.com/python/mypy/issues/6019 for why we need a dict
                # here
                redacted.update({"user": "(HIDDEN)", "password": "(HIDDEN)"})
                msg = f"unable to connect to database:\n{repr(redacted)}"
            elif jclass.endswith("FlywayException"):
                msg = "when initializing database:"
                if "applied migration" in e.args[0]:
                    msg += (
                        "\n\nThe schema of the database does not match the schema of "
                        "this version of ixmp. To resolve, either install the version "
                        "of ixmp used to create the database, or delete it and retry."
                    )
            else:
                _raise_jexception(e)
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
                java.System.gc()
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
        """Handle platform/backend config arguments.

        `args` will overwrite any `kwargs`, and may be one of:

        - ("oracle", url, user, password, [jvmargs]) for an Oracle database.
        - ("hsqldb", path, [jvmargs]) for a file-backed HyperSQL database.
        - ("hsqldb",) with "url" supplied via `kwargs`, e.g. "jdbc:hsqldb:mem://foo" for
          an in-memory database.
        """
        args = list(args)
        info = copy(kwargs)

        # First argument: driver
        try:
            info["driver"] = args.pop(0)
        except IndexError:
            raise ValueError(
                f"≥1 positional argument required for class=jdbc: driver; got: {args}, "
                + str(kwargs)
            )

        if info["driver"] == "oracle":
            if len(args) < 3:
                raise ValueError(
                    "3 or 4 arguments expected for driver=oracle: url, user, password, "
                    f"[jvmargs]; got: {str(args)}"
                )
            info["url"], info["user"], info["password"], *jvmargs = args

        elif info["driver"] == "hsqldb":
            try:
                info["path"] = Path(args.pop(0)).resolve()
            except IndexError:
                if "url" not in info:
                    raise ValueError(
                        "must supply either positional path or url= keyword argument "
                        "for driver=hsqldb"
                    )
            jvmargs = args

        else:
            raise ValueError(
                f"driver={info['driver']}; expected one of {set(DRIVER_CLASS)}"
            )

        if len(jvmargs) > 1:
            raise ValueError(
                f"Unrecognized extra argument(s) for driver={info['driver']}: "
                f"{jvmargs[1:]}"
            )
        elif len(jvmargs):
            info["jvmargs"] = jvmargs[0]

        return info

    def set_log_level(self, level: Union[int, str]) -> None:
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
        self, domain: str, docs: Union[dict[str, str], Iterable[tuple[str, str]]]
    ) -> None:
        dd = _domain_enum(domain)
        jdata = java.LinkedHashMap()
        if isinstance(docs, dict):
            docs = list(docs.items())
        for k, v in docs:
            jdata.put(str(k), str(v))
        self.jobj.setDoc(dd, jdata)

    def get_doc(
        self, domain: str, name: Optional[str] = None
    ) -> Union[str, dict[str, str]]:
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
        except java.IxException as e:  # pragma: no cover
            log.warning(str(e))
        except (AttributeError, jpype.JVMNotRunning):
            # - self.jobj is None, e.g. cleanup after __init__ fails
            # - JVM has already shut down, e.g. on program exit
            pass

    def get_auth(self, user: str, models: Iterable[str], kind: str) -> dict[str, bool]:
        model_access = self.jobj.checkModelAccess(user, kind, to_jlist(models))
        # NOTE Can't isinstance()-check parametrized generic yet; java HasMap is
        # returned in truth, but unusable as return type (becomes Any)
        assert isinstance(model_access, Mapping)
        return cast(dict[str, bool], model_access)

    def set_node(
        self,
        name: str,
        parent: Optional[str] = None,
        hierarchy: Optional[str] = None,
        synonym: Optional[str] = None,
    ) -> None:
        if parent and hierarchy and not synonym:
            self.jobj.addNode(name, parent, hierarchy)
        elif synonym and not (parent or hierarchy):
            self.jobj.addNodeSynonym(synonym, name)

    def get_nodes(self) -> Generator[tuple[str, Optional[str], str, str]]:
        for r in self.jobj.listNodes("%"):
            n, p, h = r.getName(), r.getParent(), r.getHierarchy()
            yield (n, None, p, h)
            yield from [(s, n, p, h) for s in (r.getSynonyms() or [])]

    def get_timeslices(self) -> Generator[tuple[str, str, float], Any, None]:
        for r in self.jobj.getTimeslices():
            name, category, duration = (r.getName(), r.getCategory(), r.getDuration())
            yield name, category, duration

    def set_timeslice(self, name: str, category: str, duration: float) -> None:
        self.jobj.addTimeslice(name, category, java.Double(duration))

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
        self, default: bool, model: Optional[str], scenario: Optional[str]
    ) -> Generator[list[Union[bool, int, str]], Any, None]:
        # List<Map<String, Object>>
        with _handle_jexception():
            scenarios = self.jobj.getScenarioList(default, model, scenario)

        for s in scenarios:
            data = []
            for field in FIELDS["get_scenarios"]:
                data.append(int(s[field]) if field == "version" else s[field])
            yield data

    def set_unit(self, name: str, comment: str) -> None:
        try:
            self.jobj.addUnitToDB(name, comment)
        except Exception as e:  # pragma: no cover
            if "Error assigning an unit-key-id mapping" in str(e) and "" == str(name):
                # ixmp_source does not support adding "" with Oracle
                log.warning(f"…skip {repr(name)} (ixmp.JDBCBackend with driver=oracle)")
            else:
                _raise_jexception(e)

    def get_units(self) -> list[str]:
        return to_pylist(self.jobj.getUnitList())

    @override
    def read_file(
        self,
        path: Path,  # type: ignore[override]
        item_type: ItemType,
        **kwargs: Unpack[ReadKwargs],
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

            with _handle_jexception():
                self.jindex[ts].readSolutionFromGDX(*args)

            self.cache_invalidate(ts)
        else:
            raise NotImplementedError(path, item_type)

    @override
    def write_file(
        self,
        path: os.PathLike[str],
        item_type: ItemType,
        **kwargs: Unpack[WriteKwargs],
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
        model: Optional[str],
        scenario: Optional[str],
        version: Optional[Union[int, str]],
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
        with _handle_jexception():
            jobj = method(ts.model, ts.scenario, *args)

        self._index_and_set_attrs(jobj, ts)

    def get(self, ts: TimeSeries) -> None:
        args: list[Union[int, str]] = [ts.model, ts.scenario]
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
            _raise_jexception(e)

        self._index_and_set_attrs(jobj, ts)

    def del_ts(self, ts: TimeSeries) -> None:
        super().del_ts(ts)

        # Aggressively free memory
        self.gc()
        self.jindex.pop(ts, None)

    def check_out(self, ts: TimeSeries, timeseries_only: bool) -> None:
        with _handle_jexception():
            self.jindex[ts].checkOut(timeseries_only)

    def commit(self, ts: TimeSeries, comment: str) -> None:
        try:
            self.jindex[ts].commit(comment)
        except java.Exception as e:
            arg = e.args[0]
            if isinstance(arg, str) and "this Scenario is not checked out" in arg:
                raise RuntimeError(arg)
            else:  # pragma: no cover
                _raise_jexception(e)
        if ts.version == 0:
            ts.version = self.jindex[ts].getVersion()

    def discard_changes(self, ts: TimeSeries) -> None:
        self.jindex[ts].discardChanges()

    def set_as_default(self, ts: TimeSeries) -> None:
        self.jindex[ts].setAsDefaultVersion()

    def is_default(self, ts: TimeSeries) -> bool:
        return bool(self.jindex[ts].isDefault())

    def last_update(self, ts: TimeSeries) -> Optional[str]:
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
        year: Union[Sequence[int], Sequence[str]],
    ) -> Generator[tuple[str, str, str, int, float], Any, None]:
        # Convert the selectors to Java lists
        r = to_jlist(region)
        v = to_jlist(variable)
        u = to_jlist(unit)
        y = to_jlist(year)

        # Field types
        ftype = {
            "year": int,
            "value": float,
        }

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
        if self._properties["jdbc.driver"] == DRIVER_CLASS["oracle"] and any(
            map(np.isinf, data.values())
        ):
            raise ValueError(
                f"± infinity (at region={region}, variable={variable}) cannot be stored"
                " in an Oracle database using JDBCBackend"
            )

        # Convert *data* to a Java data structure. Explicitly cast the key (period) to
        # Integer so JPype does not produce invalid java.lang.Long.
        jdata = java.LinkedHashMap({java.Integer(k): v for k, v in data.items()})

        try:
            self.jindex[ts].addTimeseries(
                region, variable, subannual, jdata, unit, meta
            )
        except java.IxException as e:
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
            region, variable, subannual, java.Integer(year), value, unit, meta
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
        years = to_jlist(years, java.Integer)
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
        years = to_jlist(years, java.Integer)
        self.jindex[ts].removeGeoData(region, variable, subannual, years, unit)

    # Scenario methods

    def clone(
        self,
        s: Scenario,
        platform_dest: Platform,
        model: str,
        scenario: str,
        annotation: Optional[str],
        keep_solution: bool,
        first_model_year: Optional[int] = None,
    ) -> Scenario:
        # Raise exceptions for limitations of JDBCBackend
        if not isinstance(platform_dest._backend, self.__class__):
            raise NotImplementedError(  # pragma: no cover
                f"Clone between {self.__class__} and{platform_dest._backend.__class__}"
            )
        elif platform_dest._backend is not self:
            package = s.__class__.__module__.split(".")[0]
            msg = f"Cross-platform clone of {package}.Scenario with"
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

    def list_items(self, s: Scenario, type: ItemTypeNames) -> list[str]:
        return to_pylist(getattr(self.jindex[s], f"get{type.title()}List")())

    def init_item(
        self,
        s: Scenario,
        type: ItemTypeNames,
        name: str,
        idx_sets: Sequence[str],
        idx_names: Optional[Sequence[str]],
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
        except java.Exception as e:
            if "already exists" in e.args[0]:
                raise ValueError(f"{repr(name)} already exists")
            else:
                _raise_jexception(e)

    def delete_item(
        self, s: Scenario, type: Literal["set", "par", "equ"], name: str
    ) -> None:
        try:
            getattr(self.jindex[s], f"remove{type.title()}")(name)
        except jpype.JException as e:
            if "There exists no" in e.args[0]:
                raise KeyError(name)
            else:  # pragma: no cover
                _raise_jexception(e)
        self.cache_invalidate(s, type, name)

    def item_index(
        self, s: Scenario, name: str, sets_or_names: Literal["sets", "names"]
    ) -> list[str]:
        jitem = self._get_item(s, "item", name, load=False)
        return list(getattr(jitem, f"getIdx{sets_or_names.title()}")())

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["set"],
        name: str,
        filters: Optional[Mapping[str, Iterable[object]]] = None,
    ) -> Union["pd.Series[Union[float, int, str]]", pd.DataFrame]: ...

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["par"],
        name: str,
        filters: Optional[Mapping[str, Iterable[object]]] = None,
    ) -> Union[dict[str, Union[float, str]], pd.DataFrame]: ...

    @overload
    def item_get_elements(
        self,
        s: Scenario,
        ix_type: Literal["equ", "var"],
        name: str,
        filters: Optional[Mapping[str, Iterable[object]]] = None,
    ) -> Union[dict[str, float], pd.DataFrame]: ...

    # FIXME reduce complexity 18 → ≤13
    def item_get_elements(  # noqa: C901
        self,
        s: Scenario,
        ix_type: ItemTypeNames,
        name: str,
        filters: Optional[Mapping[str, Iterable[object]]] = None,
    ) -> Union[
        dict[str, Union[float, str]],
        dict[str, float],
        "pd.Series[Union[float, int, str]]",
        pd.DataFrame,
    ]:
        if filters:
            # Convert filter elements to strings
            filters = {dim: as_str_list(ele) for dim, ele in filters.items()}

        try:
            # Retrieve the cached value with this exact set of filters
            return self.cache_get(s, ix_type, name, filters)
        except KeyError:
            pass  # Cache miss

        try:
            # Retrieve a cached, unfiltered value of the same item
            unfiltered = self.cache_get(s, ix_type, name, None)
        except KeyError:
            pass  # Cache miss
        else:
            # Success; filter and return
            # We seem to rely on this
            assert isinstance(unfiltered, pd.DataFrame)
            return filtered(unfiltered, filters)

        # Failed to load item from cache

        # Retrieve the item
        item = self._get_item(s, ix_type, name, load=True)
        idx_names = list(item.getIdxNames())
        idx_sets = list(item.getIdxSets())

        # Get list of elements, using filters if provided
        if filters is not None:
            jFilter = java.HashMap()

            for idx_name, values in filters.items():
                # Retrieve the elements of the index set as a list
                idx_set_name = idx_sets[idx_names.index(idx_name)]
                idx_set = self.item_get_elements(s, "set", idx_set_name)
                assert isinstance(idx_set, pd.Series)
                elements = idx_set.tolist()

                # Filter for only included values and store
                filtered_elements = filter(lambda e: e in values, elements)
                jFilter.put(idx_name, to_jlist(filtered_elements))

            jList = item.getElements(jFilter)
        else:
            jList = item.getElements()

        result: Union[
            pd.DataFrame,
            "pd.Series[Union[float, int, str]]",
            dict[str, float],
            dict[str, Union[float, str]],
        ]

        if item.getDim() > 0:
            # Mapping set or multi-dimensional equation, parameter, or variable
            columns = copy(idx_names)

            # Prepare dtypes for index columns
            dtypes: dict[str, Union[type[float], type[int], type[str]]] = {}
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
                columns.append(
                    pd.Series(
                        # NB [:] causes JPype to use a faster code path
                        getattr(item, f"get{method}")(*args, jList)[:],
                        dtype=dtypes[name],
                        name=name,
                    )
                )

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

            result = pd.concat(columns, axis=1, copy=False)
        elif ix_type == "set":
            # Index sets
            # dtype=object is to silence a warning in pandas 1.0
            result = pd.Series(item.getCol(0, jList)[:], dtype=object)
        elif ix_type == "par":
            # Scalar parameter
            # NOTE Not sure why we have to tell mypy this cast
            result = cast(
                dict[str, Union[float, str]],
                dict(
                    value=float(item.getScalarValue().floatValue()),
                    unit=str(item.getScalarUnit()),
                ),
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
        type: Literal["par", "set"],
        name: str,
        elements: Iterable[tuple[Any, Optional[float], Optional[str], Optional[str]]],
    ) -> None:
        jobj = self._get_item(s, type, name)

        try:
            for key, value, unit, comment in elements:
                # Prepare arguments
                args = [to_jlist(key)] if key else []
                if type == "par":
                    args.extend([java.Double(value), unit])
                if comment:
                    args.append(comment)

                # Activates one of 5 signatures for addElement:
                # - set: (key)
                # - set: (key, comment)
                # - par: (key, value, unit, comment)
                # - par: (key, value, unit)
                # - par: (value, unit, comment)
                jobj.addElement(*args)
        except java.IxException as e:
            if any(s in e.args[0] for s in ("does not have an element", "The unit")):
                # Re-raise as Python ValueError
                raise ValueError(e.args[0]) from None
            elif "cannot be edited" in e.args[0]:
                raise RuntimeError(e.args[0])
            else:  # pragma: no cover
                _raise_jexception(e)

        self.cache_invalidate(s, type, name)

    def item_delete_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        keys: Iterable[Iterable[str]],
    ) -> None:
        jitem = self._get_item(s, type, name, load=False)
        for key in keys:
            jitem.removeElement(to_jlist(key))

        # Since `name` may be an index set, clear the cache entirely. This ensures that
        # e.g. parameter elements for parameters indexed by `name` are also refreshed
        # on the next call to item_get_elements.
        args = (s,) if type == "set" else (s, type, name)
        self.cache_invalidate(*args)

    def get_meta(
        self,
        model: Optional[str] = None,
        scenario: Optional[str] = None,
        version: VersionType = None,
        strict: bool = False,
    ) -> dict[str, Any]:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.Long(version)

        with _handle_jexception():
            meta = self.jobj.getMeta(model, scenario, version, strict)

        return {entry.getKey(): _unwrap(entry.getValue()) for entry in meta.entrySet()}

    def set_meta(
        self,
        meta: dict[str, Union[bool, float, int, str]],
        model: Optional[str] = None,
        scenario: Optional[str] = None,
        version: Optional[int] = None,
    ) -> None:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.Long(version)

        jmeta = java.HashMap()
        for k, v in meta.items():
            jmeta.put(str(k), _wrap(v))

        with _handle_jexception():
            self.jobj.setMeta(model, scenario, version, jmeta)

    def remove_meta(
        self,
        names: list[str],
        model: Optional[str] = None,
        scenario: Optional[str] = None,
        version: Optional[int] = None,
    ) -> None:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.Long(version)
        self.jobj.removeMeta(model, scenario, version, to_jlist(names))

    def clear_solution(self, s: Scenario, from_year: Optional[int] = None) -> None:
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
        keys: Union[str, Sequence[str]],
        is_unique: bool,
    ) -> None:
        self.jindex[ms].addCatEle(name, cat, to_jlist(keys), is_unique)

    # Helpers; not part of the Backend interface

    def _get_item(
        self,
        s: Scenario,
        ix_type: Literal["set", "par", "equ", "var", "item"],
        name: str,
        load: bool = True,
    ) -> Any:
        """Return the Java object for item `name` of `ix_type`.

        Parameters
        ----------
        load : bool, optional
            If `ix_type` is 'par', 'var', or 'equ', the elements of the item are loaded
            from the database before :meth:`_item` returns. If :const:`False`, the
            elements can be loaded later using ``item.loadItemElementsfromDB()``.
        """
        # getItem is not overloaded to accept a second bool argument
        args = [name] + ([load] if ix_type != "item" else [])
        try:
            return getattr(self.jindex[s], f"get{ix_type.title()}")(*args)
        except java.IxException as e:
            # Regex for similar but not consistent messages from Java code
            msg = f"No (item|{ix_type.title()}) '?{name}'? exists in this Scenario!"
            if re.match(msg, e.args[0]):
                # Re-raise as a Python KeyError
                raise KeyError(name) from None
            else:  # pragma: no cover
                _raise_jexception(e)

    # Functions to override type hints of CachingBackend

    def cache_get(
        self,
        ts: TimeSeries,
        ix_type: str,
        name: str,
        filters: Optional[Mapping[str, Iterable[Any]]],
    ) -> Union[
        dict[str, Union[float, str]], "pd.Series[Union[float, int, str]]", pd.DataFrame
    ]:
        # NOTE Based on how we use self.cache() above, cache will only have these values
        return cast(
            Union[
                dict[str, Union[float, str]],
                "pd.Series[Union[float, int, str]]",
                pd.DataFrame,
            ],
            super().cache_get(ts, ix_type, name, filters),
        )


def start_jvm(jvmargs: Optional[Union[str, list[str]]] = None) -> None:
    """Start the Java Virtual Machine via JPype_.

    Parameters
    ----------
    jvmargs : str or list of str, optional
        Additional arguments for launching the JVM, passed to :func:`jpype.startJVM`.

        For instance, to set the maximum heap space to 4 GiB, give
        ``jvmargs=['-Xmx4G']``. See the `JVM documentation`_ for a list of options.

        .. _`JVM documentation`: https://docs.oracle.com/javase/7/docs
           /technotes/tools/windows/java.html)
    """
    from ixmp.model.gams import gams_info

    if jvmargs is None:
        jvmargs = []
    if jpype.isJVMStarted():
        return

    # Base directory for the classpath and library path
    base = Path(__file__).with_name("jdbc")

    # Arguments
    args = jvmargs if isinstance(jvmargs, list) else [jvmargs]

    # Append path to directories containing arch-specific libraries
    uname = platform.uname()
    paths = [
        gams_info().java_api_dir,  # GAMS system directory
        base.joinpath(uname.machine),  # Subdirectory of ixmp/backend/jdbc
    ]
    sep = ";" if uname.system == "Windows" else ":"
    args.append(f"-Djava.library.path={sep.join(map(str, paths))}")

    # Keyword arguments
    kwargs = dict(
        # Use ixmp.jar and related Java JAR files
        classpath=str(base.joinpath("*")),
        # For JPype 0.7 (raises a warning) and 0.8 (default is False). 'True' causes
        # Java string objects to be converted automatically to Python str(), as expected
        # by ixmp Python code.
        convertStrings=True,
    )

    log.debug(f"JAVA_HOME: {os.environ.get('JAVA_HOME', '(not set)')}")
    log.debug(f"jpype.getDefaultJVMPath: {jpype.getDefaultJVMPath()}")
    log.debug(f"args to startJVM: {args} {kwargs}")

    try:
        jpype.startJVM(*args, **kwargs)
    except FileNotFoundError as e:  # pragma: no cover
        # Not covered by tests. jpype.getDefaultJVMPath() tries an extensive set of
        # methods to find the JVM; it would require excessive effort to defeat these.
        raise FileNotFoundError(
            "This error may occur because you have not installed or configured a Java"
            "Runtime Environment. See the install documentation."
        ) from e

    # Define auxiliary references to Java classes
    global java
    for class_name in JAVA_CLASSES:
        setattr(java, class_name.split(".")[-1], jpype.JClass(class_name))


# Conversion methods


def to_pylist(jlist: Any) -> list[Any]:
    """Convert Java list types to :class:`list`."""
    try:
        return list(jlist[:])
    except Exception:
        # java.LinkedList
        return list(jlist.toArray()[:])


def to_jlist(
    arg: Union[str, Iterable[Union[float, int, str]]],
    convert: Optional[Callable[..., Any]] = None,
) -> Any:
    """Convert :class:`list` *arg* to java.LinkedList.

    Parameters
    ----------
    arg : Collection or Iterable or str
    convert : callable, optional
        If supplied, every element of `arg` is passed through `convert` before being
        added.

    Returns
    -------
    java.LinkedList
    """
    # Previously JPype1 (prior to 1.0) could take single argument in addAll method of
    # Java collection. As string implements Sequence contract in Python we need to
    # convert it explicitly to list here.
    if isinstance(arg, str):
        arg = [arg]

    if convert is not None:
        return java.LinkedList(list(map(convert, arg)))
    elif isinstance(arg, Sequence):
        # Sized collection can be used directly
        return java.LinkedList(arg)
    elif isinstance(arg, Iterable):
        # Transfer items from an iterable, generator, etc. to the LinkedList
        return java.LinkedList(list(arg))
    else:
        raise ValueError(arg)
