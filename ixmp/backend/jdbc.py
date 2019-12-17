from copy import copy
from collections import ChainMap
from collections.abc import Collection, Iterable
from functools import lru_cache
from itertools import chain
import logging
import os
from pathlib import Path, PurePosixPath
import re
from types import SimpleNamespace

import jpype
from jpype import JClass
import numpy as np
import pandas as pd

from ixmp.core import Scenario
from ixmp.utils import as_str_list, filtered, islistable
from . import FIELDS, ItemType
from .base import CachingBackend


log = logging.getLogger(__name__)


# Map of Python to Java log levels
LOG_LEVELS = {
    'CRITICAL': 'ALL',
    'ERROR': 'ERROR',
    'WARNING': 'WARN',
    'INFO': 'INFO',
    'DEBUG': 'DEBUG',
    'NOTSET': 'OFF',
}

# Java classes, loaded by start_jvm(). These become available as e.g.
# java.IxException or java.HashMap.
java = SimpleNamespace()

JAVA_CLASSES = [
    'at.ac.iiasa.ixmp.exceptions.IxException',
    'at.ac.iiasa.ixmp.objects.Scenario',
    'at.ac.iiasa.ixmp.objects.TimeSeries.TimeSpan',
    'at.ac.iiasa.ixmp.Platform',
    'java.lang.Double',
    'java.lang.Integer',
    'java.lang.NoClassDefFoundError',
    'java.math.BigDecimal',
    'java.util.HashMap',
    'java.util.LinkedHashMap',
    'java.util.LinkedList',
    'java.util.Properties',
]


DRIVER_CLASS = {
    'oracle': 'oracle.jdbc.driver.OracleDriver',
    'hsqldb': 'org.hsqldb.jdbcDriver',
}


def _create_properties(driver=None, path=None, url=None, user=None,
                       password=None):
    """Create a database Properties from arguments."""
    properties = java.Properties()

    # Handle arguments
    try:
        properties.setProperty('jdbc.driver', DRIVER_CLASS[driver])
    except KeyError:
        raise ValueError(f'unrecognized/unsupported JDBC driver {driver!r}')

    if driver == 'oracle':
        if url is None or path is not None:
            raise ValueError("use JDBCBackend(driver='oracle', url=…)")

        full_url = 'jdbc:oracle:thin:@{}'.format(url)
    elif driver == 'hsqldb':
        if path is None and url is None:
            raise ValueError("use JDBCBackend(driver='hsqldb', path=…)")

        if url is not None:
            if url.startswith('jdbc:hsqldb:'):
                full_url = url
            else:
                raise ValueError(url)
        else:
            # Convert Windows paths to use forward slashes per HyperSQL JDBC
            # URL spec
            url_path = (str(PurePosixPath(Path(path).resolve()))
                        .replace('\\', ''))
            full_url = 'jdbc:hsqldb:file:{}'.format(url_path)
        user = user or 'ixmp'
        password = password or 'ixmp'

    properties.setProperty('jdbc.url', full_url)
    properties.setProperty('jdbc.user', user)
    properties.setProperty('jdbc.pwd', password)

    return properties


def _read_properties(file):
    """Read database properties from *file*, returning :class:`dict`."""
    properties = dict()
    for line in file.read_text().split('\n'):
        match = re.search(r'([^\s]+)\s*=\s*(.+)\s*', line)
        if match is not None:
            properties[match.group(1)] = match.group(2)
    return properties


class JDBCBackend(CachingBackend):
    """Backend using JPype/JDBC to connect to Oracle and HyperSQLDB instances.

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
    jobj = None

    #: Mapping from ixmp.TimeSeries object to the underlying
    #: at.ac.iiasa.ixmp.Scenario object (or subclasses of either)
    jindex = {}

    def __init__(self, jvmargs=None, **kwargs):
        properties = None

        # Handle arguments
        if 'dbprops' in kwargs:
            # Use an existing file
            dbprops = Path(kwargs.pop('dbprops'))
            if dbprops.exists() and dbprops.is_file():
                # Existing properties file
                properties = _read_properties(dbprops)
                if 'jdbc.url' not in properties:
                    raise ValueError('Config file contains no database URL')
            else:
                raise FileNotFoundError(dbprops)

        start_jvm(jvmargs)

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
                msg = e.args[0].replace('_create_properties', 'JDBCBackend')
                raise TypeError(msg)

        log.info('launching ixmp.Platform connected to {}'
                 .format(properties.getProperty('jdbc.url')))

        try:
            self.jobj = java.Platform('Python', properties)
        except java.NoClassDefFoundError as e:  # pragma: no cover
            raise NameError(
                '{}\nCheck that dependencies of ixmp.jar are included in {}'
                .format(e, Path(__file__).parents[2] / 'lib'))
        except jpype.JException as e:  # pragma: no cover
            # Handle Java exceptions
            jclass = e.__class__.__name__
            info = '\n{}\n(Java: {})'.format(e, jclass)
            if jclass.endswith('HikariPool.PoolInitializationException'):
                redacted = copy(kwargs)
                redacted.update({'user': '(HIDDEN)', 'password': '(HIDDEN)'})
                raise RuntimeError('unable to connect to database:\n{!r}{}'
                                   .format(redacted, info)) from None
            elif jclass.endswith('FlywayException'):
                raise RuntimeError('when initializing database:' + info)
            else:
                raise RuntimeError('unhandled Java exception:' + info) from e

        # Invoke the parent constructor to initialize the cache
        super().__init__()

    # Platform methods

    def set_log_level(self, level):
        self.jobj.setLogLevel(LOG_LEVELS[level])

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
        except java.IxException as e:
            if 'Error closing the database connection!' in str(e):
                log.warning('Database connection could not be closed or was '
                            'already closed')
            else:
                log.warning(str(e))

    def get_auth(self, user, models, kind):
        return self.jobj.checkModelAccess(user, kind, to_jlist2(models))

    def set_node(self, name, parent=None, hierarchy=None, synonym=None):
        if parent and hierarchy and not synonym:
            self.jobj.addNode(name, parent, hierarchy)
        elif synonym and not (parent or hierarchy):
            self.jobj.addNodeSynonym(synonym, name)

    def get_nodes(self):
        for r in self.jobj.listNodes('%'):
            n, p, h = r.getName(), r.getParent(), r.getHierarchy()
            yield (n, None, p, h)
            yield from [(s, n, p, h) for s in (r.getSynonyms() or [])]

    def get_scenarios(self, default, model, scenario):
        # List<Map<String, Object>>
        scenarios = self.jobj.getScenarioList(default, model, scenario)

        for s in scenarios:
            data = []
            for field in FIELDS['get_scenarios']:
                data.append(int(s[field]) if field == 'version' else s[field])
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
        ts, filters = self._handle_rw_filters(kwargs.pop('filters', {}))
        if path.suffix == '.gdx' and item_type is ItemType.MODEL:
            kw = {'check_solution', 'comment', 'equ_list', 'var_list'}

            if not isinstance(ts, Scenario):
                raise ValueError('read from GDX requires a Scenario object')
            elif set(kwargs.keys()) != kw:
                raise ValueError(('keyword arguments {} do not match required '
                                  '{}').format(kwargs.keys(), kw))

            args = (
                str(path.parent),
                path.name,
                kwargs.pop('comment'),
                to_jlist2(kwargs.pop('var_list')),
                to_jlist2(kwargs.pop('equ_list')),
                kwargs.pop('check_solution'),
            )

            if len(kwargs):
                raise ValueError('extra keyword arguments {}'.format(kwargs))

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
        ts, filters = self._handle_rw_filters(kwargs.pop('filters', {}))
        if path.suffix == '.gdx' and item_type is ItemType.SET | ItemType.PAR:
            if len(filters):
                raise NotImplementedError('write to GDX with filters')
            elif not isinstance(ts, Scenario):
                raise ValueError('write to GDX requires a Scenario object')

            # include_var_equ=False -> do not include variables/equations in
            # GDX
            self.jindex[ts].toGDX(str(path.parent), path.name, False)
        elif path.suffix == '.csv' and item_type is ItemType.TS:
            model = filters.pop('model')
            scenario = filters.pop('scenario')
            variables = filters.pop('variable')
            default = filters.pop('default')

            scen_list = self.jobj.getScenarioList(default, model, scenario)
            run_ids = [s['run_id'] for s in scen_list]
            self.jobj.exportTimeseriesData(to_jlist2(run_ids),
                                           to_jlist2(variables),
                                           str(path))
        else:
            raise NotImplementedError

    # Timeseries methods

    def _common_init(self, ts, klass, *args):
        """Common code for ts_init and s_init."""
        method = getattr(self.jobj, 'new' + klass)
        # Create a new TimeSeries
        jobj = method(ts.model, ts.scenario, *args)

        # Add to index
        self.jindex[ts] = jobj

        # Retrieve initial version
        ts.version = jobj.getVersion()

    def init_ts(self, ts, annotation=None):
        self._common_init(ts, 'TimeSeries', annotation)

    def get(self, ts, version):
        args = [ts.model, ts.scenario]
        if version is not None:
            # Load a TimeSeries of specific version
            args.append(version)

        # either getTimeSeries or getScenario
        method = getattr(self.jobj, 'get' + ts.__class__.__name__)
        try:
            jobj = method(*args)
        except java.IxException as e:
            raise RuntimeError(*e.args)

        # Add to index
        self.jindex[ts] = jobj

        # Retrieve or check the version
        if version is None:
            version = jobj.getVersion()
        else:
            assert version == jobj.getVersion()

        # Update the version attribute
        ts.version = version

        if isinstance(ts, Scenario):
            # Also retrieve the scheme
            ts.scheme = jobj.getScheme()

    def check_out(self, ts, timeseries_only):
        try:
            self.jindex[ts].checkOut(timeseries_only)
        except java.IxException as e:
            raise RuntimeError(e)

    def commit(self, ts, comment):
        self.jindex[ts].commit(comment)
        if ts.version == 0:
            ts.version = self.jindex[ts].getVersion()

    def discard_changes(self, ts):
        self.jindex[ts].discardChanges()

    def set_as_default(self, ts):
        self.jindex[ts].setAsDefaultVersion()

    def is_default(self, ts):
        return bool(self.jindex[ts].isDefault())

    def last_update(self, ts):
        return self.jindex[ts].getLastUpdateTimestamp().toString()

    def run_id(self, ts):
        return self.jindex[ts].getRunId()

    def preload(self, ts):
        self.jindex[ts].preloadAllTimeseries()

    def get_data(self, ts, region, variable, unit, year):
        # Convert the selectors to Java lists
        r = to_jlist2(region)
        v = to_jlist2(variable)
        u = to_jlist2(unit)
        y = to_jlist2(year)

        # Field types
        ftype = {
            'year': int,
            'value': float,
        }

        # Iterate over returned rows
        for row in self.jindex[ts].getTimeseries(r, v, u, None, y):
            # Get the value of each field and maybe convert its type
            yield tuple(ftype.get(f, str)
                        (getattr(row, 'get' + f.capitalize())())
                        for f in FIELDS['ts_get'])

    def get_geo(self, ts):
        # NB the return type of getGeoData() requires more processing than
        #    getTimeseries. It also accepts no selectors.

        # Field types
        ftype = {
            'meta': int,
            'time': lambda ord: timespans()[int(ord)],  # Look up the name
            'year': lambda obj: obj,  # Pass through; handled later
        }

        # Returned names in Java data structure do not match API column names
        jname = {
            'meta': 'meta',
            'region': 'nodeName',
            'time': 'time',
            'unit': 'unitName',
            'variable': 'keyString',
            'year': 'yearlyData'
        }

        # Iterate over rows from the Java backend
        for row in self.jindex[ts].getGeoData():
            data1 = {f: ftype.get(f, str)(row.get(jname.get(f, f)))
                     for f in FIELDS['ts_get_geo'] if f != 'value'}

            # At this point, the 'year' key is a not a single value, but a
            # year -> value mapping with multiple entries
            yv_entries = data1.pop('year').entrySet()

            # Construct a chain map: look up in data1, then data2
            data2 = {'year': None, 'value': None}
            cm = ChainMap(data1, data2)

            for yv in yv_entries:
                # Update data2
                data2['year'] = yv.getKey()
                data2['value'] = yv.getValue()

                # Construct a row with a single value
                yield tuple(cm[f] for f in FIELDS['ts_get_geo'])

    def set_data(self, ts, region, variable, data, unit, meta):
        # Convert *data* to a Java data structure
        jdata = java.LinkedHashMap()
        for k, v in data.items():
            # Explicit cast is necessary; otherwise java.lang.Long
            jdata.put(java.Integer(k), v)

        self.jindex[ts].addTimeseries(region, variable, None, jdata, unit,
                                      meta)

    def set_geo(self, ts, region, variable, time, year, value, unit, meta):
        self.jindex[ts].addGeoData(region, variable, time, java.Integer(year),
                                   value, unit, meta)

    def delete(self, ts, region, variable, years, unit):
        years = to_jlist2(years, java.Integer)
        self.jindex[ts].removeTimeseries(region, variable, None, years, unit)

    def delete_geo(self, ts, region, variable, time, years, unit):
        years = to_jlist2(years, java.Integer)
        self.jindex[ts].removeGeoData(region, variable, time, years, unit)

    # Scenario methods

    def init_s(self, s, scheme, annotation):
        self._common_init(s, 'Scenario', scheme, annotation)

    def clone(self, s, platform_dest, model, scenario, annotation,
              keep_solution, first_model_year=None):
        # Raise exceptions for limitations of JDBCBackend
        if not isinstance(platform_dest._backend, self.__class__):
            raise NotImplementedError(  # pragma: no cover
                f'Clone between {self.__class__} and'
                f'{platform_dest._backend.__class__}')
        elif platform_dest._backend is not self:
            msg = 'Cross-platform clone of {}.Scenario with'.format(
                s.__class__.__module__.split('.')[0])
            if keep_solution is False:
                raise NotImplementedError(msg + ' `keep_solution=False`')
            elif 'message_ix' in msg and first_model_year is not None:
                raise NotImplementedError(msg + ' first_model_year != None')

        # Prepare arguments
        args = [platform_dest._backend.jobj, model, scenario, annotation,
                keep_solution]
        if first_model_year:
            args.append(first_model_year)

        # Reference to the cloned Java object
        jclone = self.jindex[s].clone(*args)

        # Instantiate same class as the original object
        return s.__class__(platform_dest, model, scenario,
                           version=jclone.getVersion())

    def has_solution(self, s):
        return self.jindex[s].hasSolution()

    def list_items(self, s, type):
        return to_pylist(getattr(self.jindex[s], f'get{type.title()}List')())

    def init_item(self, s, type, name, idx_sets, idx_names):
        # generate index-set and index-name lists
        if isinstance(idx_sets, set) or isinstance(idx_names, set):
            raise ValueError('index dimension must be string or ordered lists')
        idx_sets = to_jlist(idx_sets)
        idx_names = to_jlist(idx_names if idx_names is not None else idx_sets)

        # Initialize the Item
        func = getattr(self.jindex[s], f'initialize{type.title()}')

        # The constructor returns a reference to the Java Item, but these
        # aren't exposed by Backend, so don't return here
        try:
            func(name, idx_sets, idx_names)
        except jpype.JException as e:
            e = str(e)
            if 'This Scenario cannot be edited' in e:
                raise RuntimeError(e)
            elif 'already exists' in e:
                raise ValueError('{!r} already exists'.format(name))
            else:
                raise

    def delete_item(self, s, type, name):
        getattr(self.jindex[s], f'remove{type.title()}')(name)
        self.cache_invalidate(s, type, name)

    def item_index(self, s, name, sets_or_names):
        jitem = self._get_item(s, 'item', name, load=False)
        return list(getattr(jitem, f'getIdx{sets_or_names.title()}')())

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
                elements = self.item_get_elements(s, 'set', idx_set).tolist()

                # Filter for only included values and store
                filtered_elements = filter(lambda e: e in values, elements)
                jFilter.put(idx_name, to_jlist2(filtered_elements))

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
            if type == 'par':
                columns.extend(['value', 'unit'])
                dtypes['value'] = float
                # Same as above
                # dtypes['unit'] = CategoricalDtype(self.jobj.getUnitList())
                dtypes['unit'] = str
            elif type in ('equ', 'var'):
                columns.extend(['lvl', 'mrg'])
                dtypes.update({'lvl': float, 'mrg': float})
            # Prepare empty DataFrame
            result = pd.DataFrame(index=pd.RangeIndex(len(jList)),
                                  columns=columns)

            # Copy vectors from Java into DataFrame columns
            # NB [:] causes JPype to use a faster code path
            for i in range(len(idx_sets)):
                result.iloc[:, i] = item.getCol(i, jList)[:]
            if type == 'par':
                result.loc[:, 'value'] = item.getValues(jList)[:]
                result.loc[:, 'unit'] = item.getUnits(jList)[:]
            elif type in ('equ', 'var'):
                result.loc[:, 'lvl'] = item.getLevels(jList)[:]
                result.loc[:, 'mrg'] = item.getMarginals(jList)[:]

            # .loc assignment above modifies dtypes; set afterwards
            result = result.astype(dtypes)
        elif type == 'set':
            # Index sets
            # dtype=object is to silence a warning in pandas 1.0
            result = pd.Series(item.getCol(0, jList), dtype=object)
        elif type == 'par':
            # Scalar parameters
            result = dict(value=item.getScalarValue().floatValue(),
                          unit=str(item.getScalarUnit()))
        elif type in ('equ', 'var'):
            # Scalar equations and variables
            result = dict(lvl=item.getScalarLevel().floatValue(),
                          mrg=item.getScalarMarginal().floatValue())

        # Store cache
        self.cache(s, type, name, filters, result)

        return result

    def item_set_elements(self, s, type, name, elements):
        jobj = self._get_item(s, type, name)

        try:
            for key, value, unit, comment in elements:
                # Prepare arguments
                args = [to_jlist2(key)] if key else []
                if type == 'par':
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
            msg = e.message()
            if ('does not have an element' in msg) or ('The unit' in msg):
                # Re-raise as Python ValueError
                raise ValueError(msg) from e
            else:  # pragma: no cover
                raise RuntimeError('unhandled Java exception') from e

        self.cache_invalidate(s, type, name)

    def item_delete_elements(self, s, type, name, keys):
        jitem = self._get_item(s, type, name, load=False)
        for key in keys:
            jitem.removeElement(to_jlist2(key))

        self.cache_invalidate(s, type, name)

    def get_meta(self, s):
        def unwrap(v):
            """Unwrap metadata numeric value (BigDecimal -> Double)"""
            return v.doubleValue() if isinstance(v, java.BigDecimal) else v

        return {entry.getKey(): unwrap(entry.getValue())
                for entry in self.jindex[s].getMeta().entrySet()}

    def set_meta(self, s, name, value):
        _type = type(value)
        try:
            _type = {int: 'Num', float: 'Num', str: 'Str', bool: 'Bool'}[_type]
            method_name = 'setMeta' + _type
        except KeyError:
            raise TypeError('Cannot store metadata of type {}'.format(_type))

        getattr(self.jindex[s], method_name)(name, value)

    def clear_solution(self, s, from_year=None):
        from ixmp.core import Scenario

        if from_year:
            if type(s) is not Scenario:
                raise TypeError('s_clear_solution(from_year=...) only valid '
                                'for ixmp.Scenario; not subclasses')
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
        self.jindex[ms].addCatEle(name, cat, to_jlist2(keys), is_unique)

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
        args = [name] + ([load] if ix_type != 'item' else [])
        try:
            return getattr(self.jindex[s], f'get{ix_type.title()}')(*args)
        except java.IxException as e:
            if re.match('No item [^ ]* exists in this Scenario', e.args[0]):
                # Re-raise as a Python KeyError
                raise KeyError(f'No {ix_type.title()} {name!r} exists in this '
                               'Scenario!') from None
            else:  # pragma: no cover
                raise RuntimeError('unhandled Java exception') from e


def start_jvm(jvmargs=[]):
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
    if jpype.isJVMStarted():
        return

    # Arguments
    args = jvmargs if isinstance(jvmargs, list) else [jvmargs]

    # Base for Java classpath entries
    cp = Path(__file__).parents[1]

    # Keyword arguments
    kwargs = dict(
        # Given 'lib/*' JPype will only glob '*.jar', so glob here explicitly
        classpath=map(str, chain([cp / 'ixmp.jar'], cp.glob('lib/*'))),

        # For JPype 0.7 (raises a warning) and 0.8 (default is False).
        # 'True' causes Java string objects to be converted automatically to
        # Python str(), as expected by ixmp Python code.
        convertStrings=True,
    )

    log.debug('JAVA_HOME: {}'.format(os.environ.get('JAVA_HOME', '(not set)')))
    log.debug('jpype.getDefaultJVMPath: {}'.format(jpype.getDefaultJVMPath()))
    log.debug('args to startJVM: {} {}'.format(args, kwargs))

    jpype.startJVM(*args, **kwargs)

    # define auxiliary references to Java classes
    global java
    for class_name in JAVA_CLASSES:
        setattr(java, class_name.split('.')[-1], JClass(class_name))


# Conversion methods

def to_pylist(jlist):
    """Transforms a Java.Array or Java.List to a :class:`numpy.array`."""
    # handling string array
    try:
        return np.array(jlist[:])
    # handling Java LinkedLists
    except Exception:
        return np.array(jlist.toArray()[:])


def to_jlist(pylist, idx_names=None):
    """Convert *pylist* to a jLinkedList."""
    if pylist is None:
        return None

    jList = java.LinkedList()
    if idx_names is None:
        if islistable(pylist):
            for key in pylist:
                jList.add(str(key))
        else:
            jList.add(str(pylist))
    else:
        # pylist must be a dict
        for idx in idx_names:
            jList.add(str(pylist[idx]))
    return jList


def to_jlist2(arg, convert=None):
    """Simple conversion of :class:`list` *arg* to java.LinkedList."""
    jlist = java.LinkedList()

    if convert:
        arg = map(convert, arg)

    if isinstance(arg, Collection):
        # Sized collection can be used directly
        jlist.addAll(arg)
    elif isinstance(arg, Iterable):
        # Transfer items from an iterable, generator, etc. to the LinkedList
        [jlist.add(value) for value in arg]
    else:
        raise ValueError(arg)

    return jlist


@lru_cache(1)
def timespans():
    # Mapping for the enums of at.ac.iiasa.ixmp.objects.TimeSeries.TimeSpan
    return {t.getDbValue(): t.name() for t in java.TimeSpan.values()}
