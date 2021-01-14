import gc
import logging
import os
import re
from collections import ChainMap
from collections.abc import Iterable, Sequence
from copy import copy
from itertools import chain
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Generator, Mapping
from weakref import WeakKeyDictionary

import jpype
import pandas as pd

from ixmp.core import Scenario
from ixmp.utils import as_str_list, filtered

from . import FIELDS, ItemType
from .base import CachingBackend

log = logging.getLogger(__name__)


_EXCEPTION_VERBOSE = os.environ.get("IXMP_JDBC_EXCEPTION_VERBOSE", "0") == "1"

# Whether to collect garbage aggressively when instances of TimeSeries die.
# See JDBCBackend.gc().
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

# Java classes, loaded by start_jvm(). These become available as e.g.
# java.IxException or java.HashMap.
java = SimpleNamespace()

JAVA_CLASSES = [
    "at.ac.iiasa.ixmp.exceptions.IxException",
    "at.ac.iiasa.ixmp.objects.Scenario",
    "at.ac.iiasa.ixmp.dto.TimesliceDTO",
    "at.ac.iiasa.ixmp.Platform",
    "java.lang.Double",
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


def _create_properties(driver=None, path=None, url=None, user=None, password=None):
    """Create a database Properties from arguments."""
    properties = java.Properties()

    # Handle arguments
    try:
        properties.setProperty("jdbc.driver", DRIVER_CLASS[driver])
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
            # Convert Windows paths to use forward slashes per HyperSQL JDBC
            # URL spec
            url_path = str(PurePosixPath(Path(path).resolve())).replace("\\", "")
            full_url = f"jdbc:hsqldb:file:{url_path}"
        user = user or "ixmp"
        password = password or "ixmp"

    properties.setProperty("jdbc.url", full_url)
    properties.setProperty("jdbc.user", user)
    properties.setProperty("jdbc.pwd", password)

    return properties


def _read_properties(file):
    """Read database properties from *file*, returning :class:`dict`."""
    properties = dict()
    for line in file.read_text().split("\n"):
        match = re.search(r"([^\s]+)\s*=\s*(.+)\s*", line)
        if match is not None:
            properties[match.group(1)] = match.group(2)
    return properties


def _raise_jexception(exc, msg="unhandled Java exception: "):
    """Convert Java/JPype exceptions to ordinary Python RuntimeError."""
    if _EXCEPTION_VERBOSE:
        msg += "\n\n" + exc.stacktrace()
    else:
        msg += exc.message()
    raise RuntimeError(msg) from None


def _domain_enum(domain):
    domain_enum = java.DocumentationKey.DocumentationDomain
    try:
        return domain_enum.valueOf(domain.upper())
    except java.IllegalArgumentException:
        domains = ", ".join([d.name().lower() for d in domain_enum.values()])
        raise ValueError(f"No such domain: {domain}, " f"existing domains: {domains}")


def _unwrap(v):
    """Unwrap meta numeric value or list of values (BigDecimal -> Double)."""
    if isinstance(v, java.BigDecimal):
        return v.doubleValue()
    if isinstance(v, java.ArrayList):
        return [_unwrap(elt) for elt in v]
    return v


def _wrap(value):
    if isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)):
        return java.BigDecimal(value)
    elif isinstance(value, (Sequence, Iterable)):
        jlist = java.ArrayList()
        jlist.addAll([_wrap(elt) for elt in value])
        return jlist
    else:
        raise ValueError(f"Cannot use value {value} as metadata")


class JDBCBackend(CachingBackend):
    """Backend using JPype/JDBC to connect to Oracle and HyperSQL databases.

    Parameters
    ----------
    driver : 'oracle' or 'hsqldb'
        JDBC driver to use.
    path : path-like, optional
        Path to the HyperSQL database.
    url : str, optional
        Partial or complete JDBC URL for the Oracle or HyperSQL database,
        e.g. ``database-server.example.com:PORT:SCHEMA``. See
        :ref:`configuration`.
    user : str, optional
        Database user name.
    password : str, optional
        Database user password.
    cache : bool, optional
        If :obj:`True` (the default), cache Python objects after conversion
        from Java objects.
    jvmargs : str, optional
        Java Virtual Machine arguments. See :meth:`.start_jvm`.
    dbprops : path-like, optional
        With ``driver='oracle'``, the path to a database properties file
        containing `driver`, `url`, `user`, and `password` information.
    """

    # NB Much of the code of this backend is in Java, in the iiasa/ixmp_source
    #    Github repository.
    #
    #    Among other abstractions, this backend:
    #
    #    - Handles any conversion between Java and Python types that is not
    #      done automatically by JPype.
    #    - Catches Java exceptions such as ixmp.exceptions.IxException, and
    #      re-raises them as appropriate Python exceptions.
    #
    #    Limitations:
    #
    #    - s_clone() is only supported when target_backend is JDBCBackend.

    #: Reference to the at.ac.iiasa.ixmp.Platform Java object
    jobj: jpype.JObject = None

    #: Mapping from ixmp.TimeSeries object to the underlying
    #: at.ac.iiasa.ixmp.Scenario object (or subclasses of either)
    jindex: Mapping[object, jpype.JObject] = WeakKeyDictionary()

    def __init__(self, jvmargs=None, **kwargs):
        properties = None

        # Handle arguments
        if "dbprops" in kwargs:
            # Use an existing file
            dbprops = Path(kwargs.pop("dbprops"))
            if dbprops.exists() and dbprops.is_file():
                # Existing properties file
                properties = _read_properties(dbprops)
                if "jdbc.url" not in properties:
                    raise ValueError("Config file contains no database URL")
            else:
                raise FileNotFoundError(dbprops)

        start_jvm(jvmargs)

        # Invoke the parent constructor to initialize the cache
        super().__init__(cache_enabled=kwargs.pop("cache", True))

        # Extract a log_level keyword argument before _create_properties().
        # By default, use the same level as the 'ixmp' logger, whatever that
        # has been set to.
        ixmp_logger = logging.getLogger("ixmp")
        log_level = kwargs.pop("log_level", ixmp_logger.getEffectiveLevel())

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

        log.info(
            "launching ixmp.Platform connected to {}".format(
                properties.getProperty("jdbc.url")
            )
        )

        try:
            self.jobj = java.Platform("Python", properties)
        except java.NoClassDefFoundError as e:  # pragma: no cover
            raise NameError(
                f"{e}\nCheck that dependencies of ixmp.jar are "
                f"included in {Path(__file__).parents[2] / 'lib'}"
            )
        except jpype.JException as e:  # pragma: no cover
            # Handle Java exceptions
            jclass = e.__class__.__name__
            if jclass.endswith("HikariPool.PoolInitializationException"):
                redacted = copy(kwargs)
                redacted.update({"user": "(HIDDEN)", "password": "(HIDDEN)"})
                msg = f"unable to connect to database:\n{repr(redacted)}"
            elif jclass.endswith("FlywayException"):
                msg = "when initializing database:"
                if "applied migration" in e.args[0]:
                    msg += (
                        "\n\nThe schema of the database does not match the "
                        "schema of this version of ixmp. To resolve, either "
                        "install the version of ixmp used to create the "
                        "database, or delete it and retry."
                    )
            else:
                _raise_jexception(e)
            raise RuntimeError(f"{msg}\n(Java: {jclass})")

        # Set the log level
        self.set_log_level(log_level)

    def __del__(self):
        self.close_db()

    @classmethod
    def gc(cls):
        if _GC_AGGRESSIVE:
            # log.debug('Collect garbage')
            java.System.gc()
            gc.collect()
        # else:
        #     log.debug('Skip garbage collection')

    # Platform methods

    def set_log_level(self, level):
        # Set the level of the 'ixmp.backend.jdbc' logger. Messages are handled
        # by the 'ixmp' logger; see ixmp/__init__.py.
        log.setLevel(level)

        # Translate to Java log level and set
        if isinstance(level, int):
            level = logging.getLevelName(level)
        self.jobj.setLogLevel(LOG_LEVELS[level])

    def get_log_level(self):
        levels = {v: k for k, v in LOG_LEVELS.items()}
        return levels.get(self.jobj.getLogLevel(), "UNKNOWN")

    def set_doc(self, domain, docs):
        dd = _domain_enum(domain)
        jdata = java.LinkedHashMap()
        if type(docs) == dict:
            docs = list(docs.items())
        for k, v in docs:
            jdata.put(str(k), str(v))
        self.jobj.setDoc(dd, jdata)

    def get_doc(self, domain, name=None):
        dd = _domain_enum(domain)
        if name is None:
            doc = self.jobj.getDoc(dd)
            return {entry.getKey(): entry.getValue() for entry in doc.entrySet()}
        else:
            return self.jobj.getDoc(dd, str(name))

    def open_db(self):
        """(Re-)open the database connection."""
        self.jobj.openDB()

    def close_db(self):
        """Close the database connection.

        A HSQL database can only be used by one :class:`Backend` instance at a
        time. Any existing connection must be closed before a new one can be
        opened.
        """
        try:
            self.jobj.closeDB()
        except java.IxException as e:  # pragma: no cover
            log.warning(str(e))
        except (AttributeError, jpype.JVMNotRunning):
            # - self.jobj is None, e.g. cleanup after __init__ fails
            # - JVM has already shut down, e.g. on program exit
            pass

    def get_auth(self, user, models, kind):
        return self.jobj.checkModelAccess(user, kind, to_jlist(models))

    def set_node(self, name, parent=None, hierarchy=None, synonym=None):
        if parent and hierarchy and not synonym:
            self.jobj.addNode(name, parent, hierarchy)
        elif synonym and not (parent or hierarchy):
            self.jobj.addNodeSynonym(synonym, name)

    def get_nodes(self):
        for r in self.jobj.listNodes("%"):
            n, p, h = r.getName(), r.getParent(), r.getHierarchy()
            yield (n, None, p, h)
            yield from [(s, n, p, h) for s in (r.getSynonyms() or [])]

    def get_timeslices(self):
        for r in self.jobj.getTimeslices():
            name, category, duration = (r.getName(), r.getCategory(), r.getDuration())
            yield name, category, duration

    def set_timeslice(self, name, category, duration):
        self.jobj.addTimeslice(name, category, java.Double(duration))

    def add_model_name(self, name):
        self.jobj.addModel(str(name))

    def add_scenario_name(self, name):
        self.jobj.addScenario(str(name))

    def get_model_names(self) -> Generator[str, None, None]:
        for model in self.jobj.listModels():
            yield str(model)

    def get_scenario_names(self) -> Generator[str, None, None]:
        for scenario in self.jobj.listScenarios():
            yield str(scenario)

    def get_scenarios(self, default, model, scenario):
        # List<Map<String, Object>>
        scenarios = self.jobj.getScenarioList(default, model, scenario)

        for s in scenarios:
            data = []
            for field in FIELDS["get_scenarios"]:
                data.append(int(s[field]) if field == "version" else s[field])
            yield data

    def set_unit(self, name, comment):
        self.jobj.addUnitToDB(name, comment)

    def get_units(self):
        return to_pylist(self.jobj.getUnitList())

    def read_file(self, path, item_type: ItemType, **kwargs):
        """Read Platform, TimeSeries, or Scenario data from file.

        JDBCBackend supports reading from:

        - ``path='*.gdx', item_type=ItemType.MODEL``. The keyword arguments
          `check_solution`, `comment`, `equ_list`, and `var_list` are
          **required**.

        Other parameters
        ----------------
        check_solution : bool
            If True, raise an exception if the GAMS solver did not reach
            optimality. (Only for MESSAGE-scheme Scenarios.)
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

            if not isinstance(ts, Scenario):
                raise ValueError("read from GDX requires a Scenario object")
            elif set(kwargs.keys()) != kw:
                raise ValueError(
                    f"keyword arguments {kwargs.keys()} do not " f"match required {kw}"
                )

            args = (
                str(path.parent),
                path.name,
                kwargs.pop("comment"),
                to_jlist(kwargs.pop("var_list")),
                to_jlist(kwargs.pop("equ_list")),
                kwargs.pop("check_solution"),
            )

            if len(kwargs):
                raise ValueError(f"extra keyword arguments {kwargs}")

            self.jindex[ts].readSolutionFromGDX(*args)

            self.cache_invalidate(ts)
        else:
            raise NotImplementedError(path, item_type)

    def write_file(self, path, item_type: ItemType, **kwargs):
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
              versions with :meth:`set_default` are written.

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

        ts, filters = self._handle_rw_filters(kwargs.pop("filters", {}))
        if path.suffix == ".gdx" and item_type is ItemType.SET | ItemType.PAR:
            if len(filters):
                raise NotImplementedError("write to GDX with filters")
            elif not isinstance(ts, Scenario):
                raise ValueError("write to GDX requires a Scenario object")

            # include_var_equ=False -> do not include variables/equations in
            # GDX
            self.jindex[ts].toGDX(str(path.parent), path.name, False)
        elif path.suffix == ".csv" and item_type is ItemType.TS:
            models = set(filters.pop("model"))
            scenarios = set(filters.pop("scenario"))
            variables = filters.pop("variable")
            units = filters.pop("unit")
            regions = filters.pop("region")
            default = filters.pop("default")
            export_all_runs = filters.pop("export_all_runs")

            scen_list = self.jobj.getScenarioList(default, None, None)
            # TODO: replace with passing list of models/scenarios
            #       to the method above
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
                str(path),
                export_all_runs,
            )
        else:
            raise NotImplementedError

    # Timeseries methods

    def _index_and_set_attrs(self, jobj, ts):
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

    def _validate_meta_args(self, model, scenario, version):
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
            msg = (
                "Invalid arguments. Valid combinations are: (model), "
                "(scenario), (model, scenario), (model, scenario, version)"
            )
            raise ValueError(msg)

    def init(self, ts, annotation):
        klass = ts.__class__.__name__

        # Final arguments: scheme only for Scenarios
        args = [ts.scheme, annotation] if klass == "Scenario" else [annotation]

        # Call either newTimeSeries or newScenario
        method = getattr(self.jobj, "new" + klass)
        jobj = method(ts.model, ts.scenario, *args)

        self._index_and_set_attrs(jobj, ts)

    def get(self, ts):
        args = [ts.model, ts.scenario]
        if ts.version is not None:
            # Load a TimeSeries of specific version
            args.append(ts.version)

        # either getTimeSeries or getScenario
        method = getattr(self.jobj, "get" + ts.__class__.__name__)
        try:
            # Either the 2- or 3- argument form, depending on args
            jobj = method(*args)
        except java.IxException as e:
            # Try to re-raise as a ValueError for bad model or scenario name
            match = re.search(r"table '([^']*)' from the database", e.args[0])
            if match:
                param = match.group(1).lower()
                if param in ("model", "scenario"):
                    raise ValueError(f"{param}={repr(getattr(ts, param))}")

            # Failed
            _raise_jexception(e)

        self._index_and_set_attrs(jobj, ts)

    def del_ts(self, ts):
        super().del_ts(ts)

        # Aggressively free memory
        self.gc()

    def check_out(self, ts, timeseries_only):
        try:
            self.jindex[ts].checkOut(timeseries_only)
        except java.IxException as e:
            _raise_jexception(e)

    def commit(self, ts, comment):
        try:
            self.jindex[ts].commit(comment)
        except java.IxException as e:
            if "this Scenario is not checked out" in e.args[0]:
                raise RuntimeError(e.args[0])
            else:  # pragma: no cover
                _raise_jexception(e)
        if ts.version == 0:
            ts.version = self.jindex[ts].getVersion()

    def discard_changes(self, ts):
        self.jindex[ts].discardChanges()

    def set_as_default(self, ts):
        self.jindex[ts].setAsDefaultVersion()

    def is_default(self, ts):
        return bool(self.jindex[ts].isDefault())

    def last_update(self, ts):
        timestamp = self.jindex[ts].getLastUpdateTimestamp()
        if timestamp is not None:
            return timestamp.toString()
        else:
            return timestamp  # None

    def run_id(self, ts):
        return self.jindex[ts].getRunId()

    def preload(self, ts):
        self.jindex[ts].preloadAllTimeseries()

    def get_data(self, ts, region, variable, unit, year):
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

    def get_geo(self, ts):
        # NB the return type of getGeoData() requires more processing than
        #    getTimeseries. It also accepts no selectors.

        # Field types
        ftype = {
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

            # At this point, the 'year' key is a not a single value, but a
            # year -> value mapping with multiple entries
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

    def set_data(self, ts, region, variable, data, unit, subannual, meta):
        # Convert *data* to a Java data structure
        jdata = java.LinkedHashMap()
        for k, v in data.items():
            # Explicit cast is necessary; otherwise java.lang.Long
            jdata.put(java.Integer(k), v)

        self.jindex[ts].addTimeseries(region, variable, subannual, jdata, unit, meta)

    def set_geo(self, ts, region, variable, subannual, year, value, unit, meta):
        self.jindex[ts].addGeoData(
            region, variable, subannual, java.Integer(year), value, unit, meta
        )

    def delete(self, ts, region, variable, subannual, years, unit):
        years = to_jlist(years, java.Integer)
        self.jindex[ts].removeTimeseries(region, variable, subannual, years, unit)

    def delete_geo(self, ts, region, variable, subannual, years, unit):
        years = to_jlist(years, java.Integer)
        self.jindex[ts].removeGeoData(region, variable, subannual, years, unit)

    # Scenario methods

    def clone(
        self,
        s,
        platform_dest,
        model,
        scenario,
        annotation,
        keep_solution,
        first_model_year=None,
    ):
        # Raise exceptions for limitations of JDBCBackend
        if not isinstance(platform_dest._backend, self.__class__):
            raise NotImplementedError(  # pragma: no cover
                f"Clone between {self.__class__} and"
                f"{platform_dest._backend.__class__}"
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

    def has_solution(self, s):
        return self.jindex[s].hasSolution()

    def list_items(self, s, type):
        return to_pylist(getattr(self.jindex[s], f"get{type.title()}List")())

    def init_item(self, s, type, name, idx_sets, idx_names):
        # generate index-set and index-name lists
        if isinstance(idx_sets, set) or isinstance(idx_names, set):
            raise TypeError("index dimension must be string or ordered lists")

        idx_sets = to_jlist(idx_sets) if len(idx_sets) else None

        if idx_names:
            if len(idx_names) != len(idx_sets):
                raise ValueError(
                    f"index names {repr(idx_names)} must have same length as "
                    f"index sets {repr(idx_sets)}"
                )
            idx_names = to_jlist(idx_names)
        else:
            idx_names = idx_sets

        # Initialize the Item
        func = getattr(self.jindex[s], f"initialize{type.title()}")

        # The constructor returns a reference to the Java Item, but these
        # aren't exposed by Backend, so don't return here
        try:
            func(name, idx_sets, idx_names)
        except jpype.JException as e:
            if "already exists" in e.args[0]:
                raise ValueError(f"{repr(name)} already exists")
            else:
                _raise_jexception(e)

    def delete_item(self, s, type, name):
        getattr(self.jindex[s], f"remove{type.title()}")(name)
        self.cache_invalidate(s, type, name)

    def item_index(self, s, name, sets_or_names):
        jitem = self._get_item(s, "item", name, load=False)
        return list(getattr(jitem, f"getIdx{sets_or_names.title()}")())

    def item_get_elements(self, s, type, name, filters=None):
        if filters:
            # Convert filter elements to strings
            filters = {dim: as_str_list(ele) for dim, ele in filters.items()}

        try:
            # Retrieve the cached value with this exact set of filters
            return self.cache_get(s, type, name, filters)
        except KeyError:
            pass  # Cache miss

        try:
            # Retrieve a cached, unfiltered value of the same item
            unfiltered = self.cache_get(s, type, name, None)
        except KeyError:
            pass  # Cache miss
        else:
            # Success; filter and return
            return filtered(unfiltered, filters)

        # Failed to load item from cache

        # Retrieve the item
        item = self._get_item(s, type, name, load=True)
        idx_names = list(item.getIdxNames())
        idx_sets = list(item.getIdxSets())

        # Get list of elements, using filters if provided
        if filters is not None:
            jFilter = java.HashMap()

            for idx_name, values in filters.items():
                # Retrieve the elements of the index set as a list
                idx_set = idx_sets[idx_names.index(idx_name)]
                elements = self.item_get_elements(s, "set", idx_set).tolist()

                # Filter for only included values and store
                filtered_elements = filter(lambda e: e in values, elements)
                jFilter.put(idx_name, to_jlist(filtered_elements))

            jList = item.getElements(jFilter)
        else:
            jList = item.getElements()

        if item.getDim() > 0:
            # Mapping set or multi-dimensional equation, parameter, or variable
            columns = copy(idx_names)

            # Prepare dtypes for index columns
            dtypes = {}
            for idx_name, idx_set in zip(columns, idx_sets):
                # NB using categoricals could be more memory-efficient, but
                #    requires adjustment of tests/documentation. See
                #    https://github.com/iiasa/ixmp/issues/228
                # dtypes[idx_name] = CategoricalDtype(
                #     self.item_get_elements(s, 'set', idx_set))
                dtypes[idx_name] = str

            # Prepare dtypes for additional columns
            if type == "par":
                columns.extend(["value", "unit"])
                dtypes["value"] = float
                # Same as above
                # dtypes['unit'] = CategoricalDtype(self.jobj.getUnitList())
                dtypes["unit"] = str
            elif type in ("equ", "var"):
                columns.extend(["lvl", "mrg"])
                dtypes.update({"lvl": float, "mrg": float})
            # Prepare empty DataFrame
            result = pd.DataFrame(index=pd.RangeIndex(len(jList)), columns=columns)

            # Copy vectors from Java into DataFrame columns
            # NB [:] causes JPype to use a faster code path
            for i in range(len(idx_sets)):
                result.iloc[:, i] = item.getCol(i, jList)[:]
            if type == "par":
                result.loc[:, "value"] = item.getValues(jList)[:]
                result.loc[:, "unit"] = item.getUnits(jList)[:]
            elif type in ("equ", "var"):
                result.loc[:, "lvl"] = item.getLevels(jList)[:]
                result.loc[:, "mrg"] = item.getMarginals(jList)[:]

            # .loc assignment above modifies dtypes; set afterwards
            result = result.astype(dtypes)
        elif type == "set":
            # Index sets
            # dtype=object is to silence a warning in pandas 1.0
            result = pd.Series(item.getCol(0, jList), dtype=object)
        elif type == "par":
            # Scalar parameters
            result = dict(
                value=float(item.getScalarValue().floatValue()),
                unit=str(item.getScalarUnit()),
            )
        elif type in ("equ", "var"):
            # Scalar equations and variables
            result = dict(
                lvl=float(item.getScalarLevel().floatValue()),
                mrg=float(item.getScalarMarginal().floatValue()),
            )

        # Store cache
        self.cache(s, type, name, filters, result)

        return result

    def item_set_elements(self, s, type, name, elements):
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

    def item_delete_elements(self, s, type, name, keys):
        jitem = self._get_item(s, type, name, load=False)
        for key in keys:
            jitem.removeElement(to_jlist(key))

        # Since `name` may be an index set, clear the cache entirely. This
        # ensures that e.g. parameter elements for parameters indexed by `name`
        # are also refreshed on the next call to item_get_elements.
        args = (s,) if type == "set" else (s, type, name)
        self.cache_invalidate(*args)

    def get_meta(
        self,
        model: str = None,
        scenario: str = None,
        version: int = None,
        strict: bool = False,
    ) -> dict:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.Long(version)
        meta = self.jobj.getMeta(model, scenario, version, strict)
        return {entry.getKey(): _unwrap(entry.getValue()) for entry in meta.entrySet()}

    def set_meta(
        self, meta: dict, model: str = None, scenario: str = None, version: int = None
    ) -> None:
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.Long(version)

        jmeta = java.HashMap()
        for k, v in meta.items():
            jmeta.put(str(k), _wrap(v))
        self.jobj.setMeta(model, scenario, version, jmeta)

    def remove_meta(
        self, categories, model: str = None, scenario: str = None, version: int = None
    ):
        self._validate_meta_args(model, scenario, version)
        if version is not None:
            version = java.Long(version)
        return self.jobj.removeMeta(model, scenario, version, to_jlist(categories))

    def clear_solution(self, s, from_year=None):
        from ixmp.core import Scenario

        if from_year:
            if type(s) is not Scenario:
                raise TypeError(
                    "s_clear_solution(from_year=...) only valid "
                    "for ixmp.Scenario; not subclasses"
                )
            self.jindex[s].removeSolution(from_year)
        else:
            self.jindex[s].removeSolution()

        self.cache_invalidate(s)

    # MsgScenario methods

    def cat_list(self, ms, name):
        return to_pylist(self.jindex[ms].getTypeList(name))

    def cat_get_elements(self, ms, name, cat):
        return to_pylist(self.jindex[ms].getCatEle(name, cat))

    def cat_set_elements(self, ms, name, cat, keys, is_unique):
        self.jindex[ms].addCatEle(name, cat, to_jlist(keys), is_unique)

    # Helpers; not part of the Backend interface

    def _get_item(self, s, ix_type, name, load=True):
        """Return the Java object for item *name* of *ix_type*.

        Parameters
        ----------
        load : bool, optional
            If *ix_type* is 'par', 'var', or 'equ', the elements of the item
            are loaded from the database before :meth:`_item` returns. If
            :const:`False`, the elements can be loaded later using
            ``item.loadItemElementsfromDB()``.
        """
        # getItem is not overloaded to accept a second bool argument
        args = [name] + ([load] if ix_type != "item" else [])
        try:
            return getattr(self.jindex[s], f"get{ix_type.title()}")(*args)
        except java.IxException as e:
            # Regex for similar but not consistent messages from Java code
            msg = f"No (item|{ix_type.title()}) '?{name}'? exists in this " "Scenario!"
            if re.match(msg, e.args[0]):
                # Re-raise as a Python KeyError
                raise KeyError(name) from None
            else:  # pragma: no cover
                _raise_jexception(e)


def start_jvm(jvmargs=None):
    """Start the Java Virtual Machine via :mod:`JPype`.

    Parameters
    ----------
    jvmargs : str or list of str, optional
        Additional arguments for launching the JVM, passed to
        :func:`jpype.startJVM`.

        For instance, to set the maximum heap space to 4 GiB, give
        ``jvmargs=['-Xmx4G']``. See the `JVM documentation`_ for a list of
        options.

        .. _`JVM documentation`: https://docs.oracle.com/javase/7/docs
           /technotes/tools/windows/java.html)
    """
    if jvmargs is None:
        jvmargs = []
    if jpype.isJVMStarted():
        return

    # Arguments
    args = jvmargs if isinstance(jvmargs, list) else [jvmargs]

    # Base for Java classpath entries
    cp = Path(__file__).parents[1]

    # Keyword arguments
    kwargs = dict(
        # Given 'lib/*' JPype will only glob '*.jar', so glob here explicitly
        classpath=map(str, chain([cp / "ixmp.jar"], cp.glob("lib/*"))),
        # For JPype 0.7 (raises a warning) and 0.8 (default is False).
        # 'True' causes Java string objects to be converted automatically to
        # Python str(), as expected by ixmp Python code.
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

    # define auxiliary references to Java classes
    global java
    for class_name in JAVA_CLASSES:
        setattr(java, class_name.split(".")[-1], jpype.JClass(class_name))


# Conversion methods


def to_pylist(jlist):
    """Convert Java list types to :class:`list`."""
    try:
        return list(jlist[:])
    except Exception:
        # java.LinkedList
        return list(jlist.toArray()[:])


def to_jlist(arg, convert=None):
    """Convert :class:`list` *arg* to java.LinkedList.

    Parameters
    ----------
    arg : Collection or Iterable or str
    convert : callable, optional
        If supplied, every element of *arg* is passed through `convert` before
        being added.

    Returns
    -------
    java.LinkedList
    """
    jlist = java.LinkedList()

    # Previously JPype1 (prior to 1.0) could take single argument
    # in addAll method of Java collection. As string implements Sequence
    # contract in Python we need to convert it explicitly to list here.
    if isinstance(arg, str):
        arg = [arg]

    if convert:
        arg = map(convert, arg)

    if isinstance(arg, Sequence):
        # Sized collection can be used directly
        jlist.addAll(arg)

    elif isinstance(arg, Iterable):
        # Transfer items from an iterable, generator, etc. to the LinkedList
        [jlist.add(value) for value in arg]
    else:
        raise ValueError(arg)

    return jlist
