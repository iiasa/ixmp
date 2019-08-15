import os
from pathlib import Path
import re
from types import SimpleNamespace

import jpype
from jpype import JClass
import numpy as np
import pandas as pd

from ixmp.config import _config
from ixmp.utils import islistable, logger
from ixmp.backend.base import Backend, FIELDS


# Map of Python to Java log levels
LOG_LEVELS = {
    'CRITICAL': 'ALL',
    'ERROR': 'ERROR',
    'WARNING': 'WARN',
    'INFO': 'INFO',
    'DEBUG': 'DEBUG',
    'NOTSET': 'OFF',
}

# Java classes, loaded by start_jvm()
java = SimpleNamespace()

JAVA_CLASSES = [
    'at.ac.iiasa.ixmp.exceptions.IxException',
    'at.ac.iiasa.ixmp.objects.Scenario',
    'at.ac.iiasa.ixmp.Platform',
    'java.lang.Double',
    'java.lang.Integer',
    'java.util.HashMap',
    'java.util.LinkedHashMap',
    'java.util.LinkedList',
    ]


class JDBCBackend(Backend):
    """Backend using JPype and JDBC to connect to Oracle and HSQLDB instances.

    Much of the code of this backend is implemented in Java code in the
    iiasa/ixmp_source Github repository.

    Among other things, this backend:

    - Catches Java exceptions such as ixmp.exceptions.IxException, and
      re-raises them as appropriate Python exceptions.

    """
    #: Reference to the at.ac.iiasa.ixmp.Platform Java object
    jobj = None

    #: Mapping from ixmp.TimeSeries object to the underlying
    #: at.ac.iiasa.ixmp.Scenario object (or subclasses of either)
    jindex = {}

    def __init__(self, dbprops=None, dbtype=None, jvmargs=None):
        start_jvm(jvmargs)
        self.dbtype = dbtype

        try:
            # if no dbtype is specified, launch Platform with properties file
            if dbtype is None:
                dbprops = _config.find_dbprops(dbprops)
                if dbprops is None:
                    raise ValueError("Not found database properties file "
                                     "to launch platform")
                logger().info("launching ixmp.Platform using config file at "
                              "'{}'".format(dbprops))
                self.jobj = java.Platform('Python', str(dbprops))
            # if dbtype is specified, launch Platform with local database
            elif dbtype == 'HSQLDB':
                dbprops = dbprops or _config.get('DEFAULT_LOCAL_DB_PATH')
                logger().info("launching ixmp.Platform with local {} database "
                              "at '{}'".format(dbtype, dbprops))
                self.jobj = java.Platform('Python', str(dbprops), dbtype)
            else:
                raise ValueError('Unknown dbtype: {}'.format(dbtype))
        except TypeError:
            msg = ("Could not launch the JVM for the ixmp.Platform."
                   "Make sure that all dependencies of ixmp.jar"
                   "are included in the 'ixmp/lib' folder.")
            logger().info(msg)
            raise

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
        self.jobj.closeDB()

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

    def set_unit(self, name, comment):
        self.jobj.addUnitToDB(name, comment)

    def get_units(self):
        """Return all units described in the database."""
        return to_pylist(self.jobj.getUnitList())

    def get_scenarios(self, default, model, scenario):
        # List<Map<String, Object>>
        scenarios = self.jobj.getScenarioList(default, model, scenario)

        for s in scenarios:
            data = []
            for field in FIELDS['get_scenarios']:
                data.append(int(s[field]) if field == 'version' else s[field])
            yield data

    # Timeseries methods

    def ts_init(self, ts, annotation=None):
        """Initialize the ixmp.TimeSeries *ts*."""
        if ts.version == 'new':
            # Create a new TimeSeries
            jobj = self.jobj.newTimeSeries(ts.model, ts.scenario, annotation)
        elif isinstance(ts.version, int):
            # Load a TimeSeries of specific version
            jobj = self.jobj.getTimeSeries(ts.model, ts.scenario, ts.version)
        else:
            # Load the latest version of a TimeSeries
            jobj = self.jobj.getTimeSeries(ts.model, ts.scenario)

            # Update the version attribute
            ts.version = jobj.getVersion()

        # Add to index
        self.jindex[ts] = jobj

    def ts_check_out(self, ts, timeseries_only):
        self.jindex[ts].checkOut(timeseries_only)

    def ts_commit(self, ts, comment):
        self.jindex[ts].commit(comment)
        if ts.version == 0:
            ts.version = self.jindex[ts].getVersion()

    def ts_discard_changes(self, ts):
        """Discard all changes and reload from the database."""
        self.jindex[ts].discardChanges()

    def ts_set_as_default(self, ts):
        """Set the current :attr:`version` as the default."""
        self.jindex[ts].setAsDefaultVersion()

    def ts_is_default(self, ts):
        """Return :obj:`True` if the :attr:`version` is the default version."""
        return bool(self.jindex[ts].isDefault())

    def ts_last_update(self, ts):
        """get the timestamp of the last update/edit of this TimeSeries"""
        return self.jindex[ts].getLastUpdateTimestamp().toString()

    def ts_run_id(self, ts):
        """get the run id of this TimeSeries"""
        return self.jindex[ts].getRunId()

    def ts_preload(self, ts):
        """Preload timeseries data to in-memory cache. Useful for bulk updates.
        """
        self.jindex[ts].preloadAllTimeseries()

    # Scenario methods

    def s_init(self, s, scheme=None, annotation=None):
        """Initialize the ixmp.Scenario *s*."""
        if s.version == 'new':
            jobj = self.jobj.newScenario(s.model, s.scenario, scheme,
                                         annotation)
        elif isinstance(s.version, int):
            jobj = self.jobj.getScenario(s.model, s.scenario, s.version)
        # constructor for `message_ix.Scenario.__init__` or `clone()` function
        elif isinstance(s.version, java.Scenario):
            jobj = s.version
        elif s.version is None:
            jobj = self.jobj.getScenario(s.model, s.scenario)
        else:
            raise ValueError('Invalid `version` arg: `{}`'.format(s.version))

        s.version = jobj.getVersion()
        s.scheme = jobj.getScheme()

        # Add to index
        self.jindex[s] = jobj

    def s_clone(self, s, target_backend, model, scenario, annotation,
                keep_solution, first_model_year=None):
        if not isinstance(target_backend, self.__class__):
            raise RuntimeError('Clone only possible between two instances of'
                               f'{self.__class__.__name__}')

        args = [model, scenario, annotation, keep_solution]
        if first_model_year:
            args.append(first_model_year)
        # Reference to the cloned Java object
        return self.jindex[s].clone(target_backend.jobj, *args)

    def s_has_solution(self, s):
        return self.jindex[s].hasSolution()

    def s_list_items(self, s, type):
        return to_pylist(getattr(self.jindex[s], f'get{type.title()}List')())

    def s_init_item(self, s, type, name, idx_sets, idx_names):
        # generate index-set and index-name lists
        if isinstance(idx_sets, set) or isinstance(idx_names, set):
            raise ValueError('index dimension must be string or ordered lists')
        idx_sets = to_jlist(idx_sets)
        idx_names = to_jlist(idx_names if idx_names is not None else idx_sets)

        # Initialize the Item
        func = getattr(self.jindex[s], f'initialize{type.title()}')

        # The constructor returns a reference to the Java Item, but these
        # aren't exposed by Backend, so don't return here
        func(name, idx_sets, idx_names)

    def s_item_index(self, s, name, sets_or_names):
        jitem = self._get_item(s, 'item', name, load=False)
        return list(getattr(jitem, f'getIdx{sets_or_names.title()}')())

    def s_item_elements(self, s, type, name, filters=None, has_value=False,
                        has_level=False):
        # Retrieve the item
        item = self._get_item(s, type, name, load=True)

        # get list of elements, with filter HashMap if provided
        if filters is not None:
            jFilter = java.HashMap()
            for idx_name in filters.keys():
                jFilter.put(idx_name, to_jlist(filters[idx_name]))
            jList = item.getElements(jFilter)
        else:
            jList = item.getElements()

        # return a dataframe if this is a mapping or multi-dimensional
        # parameter
        dim = item.getDim()
        if dim > 0:
            idx_names = np.array(item.getIdxNames().toArray()[:])
            idx_sets = np.array(item.getIdxSets().toArray()[:])

            data = {}
            for d in range(dim):
                ary = np.array(item.getCol(d, jList)[:])
                if idx_sets[d] == "year":
                    # numpy tricks to avoid extra copy
                    # _ary = ary.view('int')
                    # _ary[:] = ary
                    ary = ary.astype('int')
                data[idx_names[d]] = ary

            if has_value:
                data['value'] = np.array(item.getValues(jList)[:])
                data['unit'] = np.array(item.getUnits(jList)[:])

            if has_level:
                data['lvl'] = np.array(item.getLevels(jList)[:])
                data['mrg'] = np.array(item.getMarginals(jList)[:])

            df = pd.DataFrame.from_dict(data, orient='columns', dtype=None)
            return df

        else:
            #  for index sets
            if not (has_value or has_level):
                return pd.Series(item.getCol(0, jList)[:])

            data = {}

            # for parameters as scalars
            if has_value:
                data['value'] = item.getScalarValue().floatValue()
                data['unit'] = str(item.getScalarUnit())

            # for variables as scalars
            elif has_level:
                data['lvl'] = item.getScalarLevel().floatValue()
                data['mrg'] = item.getScalarMarginal().floatValue()

            return data

    def s_add_set_elements(self, s, name, elements):
        """Add elements to set *name* in Scenario *s*."""
        # Retrieve the Java Set and its number of dimensions
        jSet = self._get_item(s, 'set', name)
        dim = jSet.getDim()

        try:
            for e, comment in elements:
                if dim:
                    # Convert e to a JLinkedList
                    e = to_jlist2(e)

                # Call with 1 or 2 args
                jSet.addElement(e, comment) if comment else jSet.addElement(e)
        except java.IxException as e:
            msg = e.message()
            if 'does not have an element' in msg:
                # Re-raise as Python ValueError
                raise ValueError(msg) from e
            else:
                raise RuntimeError('Unhandled Java exception') from e

    def s_add_par_values(self, s, name, elements):
        """Add values to parameter *name* in Scenario *s*."""
        jPar = self._get_item(s, 'par', name)

        for key, value, unit, comment in elements:
            args = []
            if key:
                args.append(to_jlist2(key))
            args.extend([java.Double(value), unit])
            if comment:
                args.append(comment)

            # Activates one of 3 signatures for addElement:
            # - (key, value, unit, comment)
            # - (key, value, unit)
            # - (value, unit, comment)
            jPar.addElement(*args)

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
            else:
                raise RuntimeError('Unhandled Java exception') from e


def start_jvm(jvmargs=None):
    """Start the Java Virtual Machine via JPype.

    Parameters
    ----------
    jvmargs : str or list of str, optional
        Additional arguments to pass to :meth:`jpype.startJVM`.
    """
    # TODO change the jvmargs default to [] instead of None
    if jpype.isJVMStarted():
        return

    jvmargs = jvmargs or []

    # Arguments
    args = [jpype.getDefaultJVMPath()]

    # Add the ixmp root directory, ixmp.jar and bundled .jar and .dll files to
    # the classpath
    module_root = Path(__file__).parents[1]
    jarfile = module_root / 'ixmp.jar'
    module_jars = list(module_root.glob('lib/*'))
    classpath = map(str, [module_root, jarfile] + list(module_jars))

    sep = ';' if os.name == 'nt' else ':'
    args.append('-Djava.class.path={}'.format(sep.join(classpath)))

    # Add user args
    args.extend(jvmargs if isinstance(jvmargs, list) else [jvmargs])

    # For JPype 0.7 (raises a warning) and 0.8 (default is False).
    # 'True' causes Java string objects to be converted automatically to Python
    # str(), as expected by ixmp Python code.
    kwargs = dict(convertStrings=True)

    jpype.startJVM(*args, **kwargs)

    # define auxiliary references to Java classes
    global java
    for class_name in JAVA_CLASSES:
        setattr(java, class_name.split('.')[-1], JClass(class_name))


# Conversion methods

def to_pylist(jlist):
    """Transforms a Java.Array or Java.List to a python list"""
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


def to_jlist2(arg):
    """Simple conversion of :class:`list` *arg* to JLinkedList."""
    jlist = java.LinkedList()
    jlist.addAll(arg)
    return jlist


# Helper methods


def filtered(df, filters):
    """Returns a filtered dataframe based on a filters dictionary"""
    if filters is None:
        return df

    mask = pd.Series(True, index=df.index)
    for k, v in filters.items():
        isin = df[k].isin(v)
        mask = mask & isin
    return df[mask]
