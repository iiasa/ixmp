import os
import sys
import warnings
import jpype

import numpy as np
import pandas as pd

from jpype import JPackage as java
from jpype import JClass
from subprocess import check_call

import ixmp as ix
from ixmp import model_settings
from ixmp.default_path_constants import DEFAULT_LOCAL_DB_PATH
from ixmp.default_paths import default_dbprops_file, find_dbprops
from ixmp.utils import logger, islistable

# %% default settings for column headers

IAMC_IDX = ['model', 'scenario', 'region', 'variable', 'unit']


# %% Java Virtual Machine start-up

def start_jvm(jvmargs=None):
    if jpype.isJVMStarted():
        return

    module_root = os.path.dirname(__file__)
    jarfile = os.path.join(module_root, 'ixmp.jar')
    module_lib = os.path.join(module_root, 'lib')
    module_jars = [os.path.join(module_lib, f) for f in os.listdir(module_lib)]
    sep = ';' if os.name == 'nt' else ':'
    classpath = sep.join([module_root, jarfile] + module_jars)
    args = ["-Djava.class.path={}".format(classpath)]
    if jvmargs is not None:
        args += jvmargs if isinstance(jvmargs, list) else [jvmargs]
    jpype.startJVM(jpype.getDefaultJVMPath(), *args)

    # define auxiliary references to Java classes
    java.ixmp = java("at.ac.iiasa.ixmp")
    java.Integer = java("java.lang").Integer
    java.Double = java("java.lang").Double
    java.LinkedList = java("java.util").LinkedList
    java.HashMap = java("java.util").HashMap
    java.LinkedHashMap = java("java.util").LinkedHashMap


class Platform(object):
    """Database-backed instance of the ixmp.

    Each Platform connects three components:

    1. A **database** for storing model inputs and outputs. This may either be
       a local file (``dbtype='HSQLDB'``) or a database server accessed via a
       network connection. In the latter case, connection information is read
       from a `properties file`.
    2. A Java Virtual Machine (**JVM**) to run core ixmp logic and access the
       database.
    3. One or more **model(s)**, implemented in GAMS or another language or
       framework.

    The constructor parameters control these components. :class:`TimeSeries`
    and :class:`Scenario` objects are specific to one Platform; to move data
    between platforms, see :meth:`Scenario.clone`.

    Parameters
    ----------
    dbprops : path-like, optional
        If `dbtype` is :obj:`None`, the name of a database properties file
        (default: 'default.properties') in the properties file directory
        (default: ???) or the path of a properties file.

        If `dbtype == 'HSQLDB'`, the path of a local database,
        (default: "$HOME/.local/ixmp/localdb/default") or name of a
        database file in the local database directory (default:
        "$HOME/.local/ixmp/localdb/").

    dbtype : 'HSQLDB', optional
        Database type to use. If `None`, a remote database is accessed. If
        'HSQLDB', a local database is created and used at the path given by
        `dbprops`.

    jvmargs : str, optional
        Options for launching the Java Virtual Machine, e.g., the maximum heap
        space: "-Xmx4G". See the `JVM documentation`_ for a list of options.

        .. _`JVM documentation`: https://docs.oracle.com/javase/7/docs
           /technotes/tools/windows/java.html)
    """

    def __init__(self, dbprops=None, dbtype=None, jvmargs=None):
        start_jvm(jvmargs)
        self.dbtype = dbtype

        try:
            # if no dbtype is specified, launch Platform with properties file
            if dbtype is None:
                dbprops = default_dbprops_file() if dbprops is None \
                    else find_dbprops(dbprops)
                logger().info("launching ixmp.Platform using config file at"
                              "'{}'".format(dbprops))
                self._jobj = java.ixmp.Platform("Python", dbprops)
            # if dbtype is specified, launch Platform with local database
            elif dbtype == 'HSQLDB':
                dbprops = dbprops or DEFAULT_LOCAL_DB_PATH
                logger().info("launching ixmp.Platform with local {} database "
                              "at '{}'".format(dbtype, dbprops))
                self._jobj = java.ixmp.Platform("Python", dbprops, dbtype)
            else:
                raise ValueError('Unknown dbtype: {}'.format(dbtype))
        except TypeError:
            msg = ("Could not launch the JVM for the ixmp.Platform."
                   "Make sure that all dependencies of ixmp.jar"
                   "are included in the 'ixmp/lib' folder.")
            logger().info(msg)
            raise

    def set_log_level(self, level):
        """Set global logger level (for both Python and Java)

        Parameters
        ----------
        level : str
            set the logger level if specified, see
            https://docs.python.org/3/library/logging.html#logging-levels
        """
        py_to_java = {
            'CRITICAL': 'ALL',
            'ERROR': 'ERROR',
            'WARNING': 'WARN',
            'INFO': 'INFO',
            'DEBUG': 'DEBUG',
            'NOTSET': 'OFF',
        }
        if level not in py_to_java.keys():
            msg = '{} not a valid Python logger level, see ' + \
                'https://docs.python.org/3/library/logging.html#logging-level'
            raise ValueError(msg.format(level))
        logger().setLevel(level)
        self._jobj.setLogLevel(py_to_java[level])

    def open_db(self):
        """(Re-)open the database connection.

        The database connection is opened automatically for many operations.
        After calling :meth:`close_db`, it must be re-opened.

        """
        self._jobj.openDB()

    def close_db(self):
        """Close the database connection.

        A HSQL database can only be used by one :class:`Platform` instance at a
        time. Any existing connection must be closed before a new one can be
        opened.
        """
        self._jobj.closeDB()

    def scenario_list(self, default=True, model=None, scen=None):
        """Return information on all TimeSeries and Scenarios in the database.

        Parameters
        ----------
        default : boolean, optional
            Return only the default version of each TimeSeries/Scenario. If
            :obj:`False`, return all versions.
        model : str, optional
            A model name. If given, only return information for *model*.
        scen : str, optional
            A Scenario name. If given, only return information for *scen*.

        Returns
        -------
        pandas.DataFrame
            Scenario information, with the columns:

            - ``model``, ``scenario``, ``version``, and ``scheme``—Scenario
              identifiers; see :class:`Scenario`.
            - ``is_default``—:obj:`True` if the ``version`` is the default
              version for the (``model``, ``scenario``).
            - ``is_locked``—:obj:`True` if the Scenario has been locked for
              use.
            - ``cre_user`` and ``cre_date``—database user that created the
              Scenario, and creation time.
            - ``upd_user`` and ``upd_date``—user and time for last modification
              of the Scenario.
            - ``lock_user`` and ``lock_date``—user that locked the Scenario and
              lock time.
            - ``annotation``: description of the Scenario or changelog.
        """
        mod_scen_list = self._jobj.getScenarioList(default, model, scen)

        mod_range = range(mod_scen_list.size())
        cols = ['model', 'scenario', 'scheme', 'is_default', 'is_locked',
                'cre_user', 'cre_date', 'upd_user', 'upd_date',
                'lock_user', 'lock_date', 'annotation']

        data = {}
        for i in cols:
            data[i] = [str(mod_scen_list.get(j).get(i)) for j in mod_range]

        data['version'] = [int(str(mod_scen_list.get(j).get('version')))
                           for j in mod_range]
        cols.append("version")

        df = pd.DataFrame.from_dict(data, orient='columns', dtype=None)
        df = df[cols]
        return df

    def Scenario(self, model, scen, version=None,
                 scheme=None, annotation=None, cache=False):
        """Initialize a new :class:`Scenario`.

        .. deprecated:: 1.1.0

           Instead, use:

           >>> mp = ixmp.Platform(…)
           >>> ixmp.Scenario(mp, …)
        """

        warnings.warn('The constructor `mp.Scenario()` is deprecated, '
                      'please use `ixmp.Scenario(mp, ...)`')

        return Scenario(self, model, scen, version, scheme, annotation, cache)

    def units(self):
        """Return all units described in the database.

        Returns
        -------
        list
        """
        return to_pylist(self._jobj.getUnitList())

    def add_unit(self, unit, comment='None'):
        """Define a unit.

        Parameters
        ----------
        unit : str
            Name of the unit.
        comment : str, optional
            Annotation describing the unit or why it was added. The current
            database user and timestamp are appended automatically.
        """
        self._jobj.addUnitToDB(unit, comment)

# %% class TimeSeries


class TimeSeries(object):
    """Generic collection of data in time series format.

    TimeSeries is the parent/super-class of :class:`Scenario`.

    A TimeSeries is uniquely identified by three values:

    1. `model`: the name of a model used to perform calculations between input
       and output data.

       - In TimeSeries storing non-model data, arbitrary strings can be used.
       - In a :class:`Scenario`, the `model` is a reference to a GAMS program
         registered to the :class:`Platform` that can be solved with
         :meth:`Scenario.solve`. See
         :meth:`ixmp.model_settings.register_model`.

    2. `scenario`: the name of a specific, coherent description of the real-
       world system being modeled. Any `model` may be used to represent mutiple
       alternate, or 'counter-factual', `scenarios`.
    3. `version`: an integer identifying a specific iteration of a
       (`model`, `scenario`). A new `version` is created by:

       - Instantiating a new TimeSeries with the same `model` and `scenario` as
         an existing TimeSeries.
       - Calling :meth:`Scenario.clone`.

    Parameters
    ----------
    mp : :class:`Platform`
        ixmp instance in which to store data.
    model : str
        Model name.
    scenario : str
        Scenario name.
    version : int or str, optional
        If omitted, load the default version of the (`model`, `scenario`).
        If :class:`int`, load a specific version.
        If ``'new'``, create a new TimeSeries.
    annotation : str, optional
        A short annotation/comment used when ``version='new'``.
    """

    # Version of the TimeSeries
    version = None

    def __init__(self, mp, model, scenario, version=None, annotation=None):
        if not isinstance(mp, Platform):
            raise ValueError('mp is not a valid `ixmp.Platform` instance')

        if version == 'new':
            self._jobj = mp._jobj.newTimeSeries(model, scenario, annotation)
        elif isinstance(version, int):
            self._jobj = mp._jobj.getTimeSeries(model, scenario, version)
        else:
            self._jobj = mp._jobj.getTimeSeries(model, scenario)

        self.platform = mp
        self.model = model
        self.scenario = scenario
        self.version = self._jobj.getVersion()

    # functions for platform management

    def check_out(self, timeseries_only=False):
        """check out from the ixmp database instance for making changes"""
        if not timeseries_only and self.has_solution():
            raise ValueError('This Scenario has a solution, '
                             'use `Scenario.remove_solution()` or '
                             '`Scenario.clone(..., keep_solution=False)`'
                             )
        self._jobj.checkOut(timeseries_only)

    def commit(self, comment):
        """Commit all changed data to the database.

        :attr:`version` is not incremented.
        """
        self._jobj.commit(comment)
        # if version == 0, this is a new instance
        # and a new version number was assigned after the initial commit
        if self.version == 0:
            self.version = self._jobj.getVersion()

    def discard_changes(self):
        """Discard all changes and reload from the database."""
        self._jobj.discardChanges()

    def set_as_default(self):
        """Set the current :attr:`version` as the default."""
        self._jobj.setAsDefaultVersion()

    def is_default(self):
        """Return :obj:`True` if the :attr:`version` is the default."""
        return bool(self._jobj.isDefault())

    def last_update(self):
        """get the timestamp of the last update/edit of this TimeSeries"""
        return self._jobj.getLastUpdateTimestamp().toString()

    def run_id(self):
        """get the run id of this TimeSeries"""
        return self._jobj.getRunId()

    def version(self):
        """get the version number of this TimeSeries"""
        return self._jobj.getVersion()

    # functions for importing and retrieving timeseries data

    def add_timeseries(self, df, meta=False):
        """Add data to the TimeSeries.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to add. `df` must have the following columns:

            - `region` or `node`
            - `variable`
            - `unit`

            Additional column names may be either of:

            - `year` and `value`—long, or 'tabular', format.
            - one or more specific years—wide, or 'IAMC' format.

        meta : bool, optional
            If :obj:`True`, store `df` as metadata. Metadata is treated
            specially when :meth:`Scenario.clone` is called for Scenarios
            created with ``scheme='MESSAGE'``.
        """
        meta = 1 if meta else 0

        if "time" in df.columns:
            raise("sub-annual time slices not supported by Python interface!")

        # rename columns to standard notation
        cols = {c: str(c).lower() for c in df.columns}
        cols.update(node='region')
        df = df.rename(columns=cols)
        required_cols = ['region', 'variable', 'unit']
        if not set(required_cols).issubset(set(df.columns)):
            missing = list(set(required_cols) - set(df.columns))
            raise ValueError("missing required columns {}!".format(missing))

        # if in tabular format
        if ("value" in df.columns):
            df = df.sort_values(by=['region', 'variable', 'unit', 'year'])\
                .reset_index(drop=True)

            region = df.region[0]
            variable = df.variable[0]
            unit = df.unit[0]
            time = None
            jData = java.LinkedHashMap()

            for i in df.index:
                if not (region == df.region[i] and variable == df.variable[i]
                        and unit == df.unit[i]):
                    # if new 'line', pass to Java interface, start a new
                    # LinkedHashMap
                    self._jobj.addTimeseries(region, variable, time, jData,
                                             unit, meta)

                    region = df.region[i]
                    variable = df.variable[i]
                    unit = df.unit[i]
                    jData = java.LinkedHashMap()

                jData.put(java.Integer(int(df.year[i])),
                          java.Double(float(df.value[i])))
            # add the final iteration of the loop
            self._jobj.addTimeseries(region, variable, time, jData, unit, meta)

        # if in 'IAMC-style' format
        else:
            for i in df.index:
                jData = java.LinkedHashMap()

                for j in ix.utils.numcols(df):
                    jData.put(java.Integer(int(j)),
                              java.Double(float(df[j][i])))

                time = None
                self._jobj.addTimeseries(df.region[i], df.variable[i], time,
                                         jData, df.unit[i], meta)

    def timeseries(self, iamc=False, regions=None, variables=None, units=None,
                   years=None):
        """Retrieve TimeSeries data.

        Parameters
        ----------
        iamc : bool
            Return data in wide/'IAMC' format. If :obj:`False`, return data in
            long/'tabular' format; see :meth:`add_timeseries`.
        regions : list of str, optional
            Regions to include in returned data.
        variables : list of str, optional
            Variables to include in returned data.
        units : list of str, optional
            Units to include in returned data.
        years : list of int, optional
            Years to include in returned data.

        Returns
        -------
        :class:`pandas.DataFrame`
            Specified data.
        """

        # convert filter lists to Java objects
        regions = ix.to_jlist(regions)
        variables = ix.to_jlist(variables)
        units = ix.to_jlist(units)
        years = ix.to_jlist(years)

        # retrieve data, convert to pandas.DataFrame
        data = self._jobj.getTimeseries(regions, variables, units, None, years)
        dictionary = {}

        # if in tabular format
        ts_range = range(data.size())

        cols = ['region', 'variable', 'unit']
        for i in cols:
            dictionary[i] = [str(data.get(j).get(i)) for j in ts_range]

        dictionary['year'] = [data.get(j).get('year').intValue()
                              for j in ts_range]
        cols.append("year")

        dictionary['value'] = [data.get(j).get('value').floatValue()
                               for j in ts_range]
        cols.append("value")

        df = pd.DataFrame
        df = df.from_dict(dictionary, orient='columns', dtype=None)

        df['model'] = self.model
        df['scenario'] = self.scenario

        df = df[['model', 'scenario'] + cols]

        if iamc:
            df = df.pivot_table(index=IAMC_IDX, columns='year')['value']
            df.reset_index(inplace=True)

        return df


# %% class Scenario

class Scenario(TimeSeries):
    """Collection of model-related input and output data.

    A Scenario is a :class:`TimeSeries` associated with a particular model that
    can be run on the current :class:`Platform` by calling :meth:`solve`. The
    Scenario also stores the output, or 'solution' of a model run; this
    includes the 'level' and 'marginal' values of GAMS equations and variables.

    Data in a Scenario are closely related to different types in the GAMS data
    model:

    - A **set** is a named collection of labels. See :meth:`init_set`,
      :meth:`add_set`, and :meth:`set`. There are two types of sets:

      1. Sets that are lists of labels.
      2. Sets that are 'indexed' by one or more other set(s). For this type of
         set, each member is an ordered tuple of the labels in the index sets.

    - A **scalar** is a named, single, numerical value. See
      :meth:`init_scalar`, :meth:`change_scalar`, and :meth:`scalar`.

    - **Parameters**, **variables**, and **equations** are multi-dimensional
      arrays of values that are indexed by one or more sets (i.e. with
      dimension 1 or greater). The Scenario methods for handling these types
      are very similar; they mainly differ in how they are used within GAMS
      models registered with ixmp:

      - **Parameters** are generic data that can be defined before a model run.
        They may be altered by the model solution. See :meth:`init_par`,
        :meth:`remove_par`, :meth:`par_list`, :meth:`add_par`, and :meth:`par`.
      - **Variables** are calculated during or after a model run by GAMS code,
        so they cannot be modified by a Scenario. See :meth:`init_var`,
        :meth:`var_list`, and :meth:`var`.
      - **Equations** describe fundamental relationships between other types
        (parameters, variables, and scalars) in a model. They are defined in
        GAMS code, so cannot be modified by a Scenario. See :meth:`init_equ`,
        :meth:`equ_list`, and :meth:`equ`.

    Parameters
    ----------
    mp : :class:`Platform`
        ixmp instance in which to store data.
    model : str
        Model name; must be a registered model.
    scenario : str
        Scenario name.
    version : str or int or at.ac.iiasa.ixmp.objects.Scenario, optional
        If omitted, load the default version of the (`model`, `scenario`).
        If :class:`int`, load the specified version.
        If ``'new'``, initialize a new TimeSeries.
    scheme : str, optional
        Use an explicit scheme for initializing a new scenario.
    annotation : str, optional
        A short annotation/comment used when ``version='new'``.
    cache : bool, optional
        Store data in memory and return cached values instead of repeatedly
        querying the database.

    """
    # Name of the model associated with the Scenario
    model = None

    # Name of the Scenario
    scenario = None

    _java_kwargs = {
        'set': {},
        'par': {'has_value': True},
        'var': {'has_level': True},
        'equ': {'has_level': True},
    }

    def __init__(self, mp, model, scenario, version=None, scheme=None,
                 annotation=None, cache=False):
        if not isinstance(mp, Platform):
            raise ValueError('mp is not a valid `ixmp.Platform` instance')

        if version == 'new':
            self._jobj = mp._jobj.newScenario(model, scenario, scheme,
                                              annotation)
        elif isinstance(version, int):
            self._jobj = mp._jobj.getScenario(model, scenario, version)
        # constructor for `message_ix.Scenario.__init__` or `clone()` function
        elif isinstance(version, JClass('at.ac.iiasa.ixmp.objects.Scenario')):
            self._jobj = version
        else:
            self._jobj = mp._jobj.getScenario(model, scenario)

        self.platform = mp
        self.model = model
        self.scenario = scenario
        self.version = self._jobj.getVersion()
        self.scheme = scheme or self._jobj.getScheme()
        if self.scheme == 'MESSAGE' and not hasattr(self, 'is_message_scheme'):
            warnings.warn('Using `ixmp.Scenario` for MESSAGE-scheme scenarios '
                          'is deprecated, please use `message_ix.Scenario`')

        self._cache = cache
        self._pycache = {}

    def item(self, ix_type, name):
        """internal function to retrieve the Java instance of an item"""
        funcs = {
            'item': self._jobj.getItem,
            'set': self._jobj.getSet,
            'par': self._jobj.getPar,
            'var': self._jobj.getVar,
            'equ': self._jobj.getEqu,
        }
        return funcs[ix_type](name)

    def load_scenario_data(self):
        """Load all Scenario data into memory.

        Raises
        ------
        ValueError
            If the Scenario was instantiated with ``cache=False``.
        """
        if not self._cache:
            raise ValueError('Cache must be enabled to load scenario data')

        funcs = {
            'set': (self.set_list, self.set),
            'par': (self.par_list, self.par),
            'var': (self.var_list, self.var),
            'equ': (self.equ_list, self.equ),
        }
        for ix_type, (list_func, get_func) in funcs.items():
            logger().info('Caching {} data'.format(ix_type))
            for item in list_func():
                get_func(item)

    def element(self, ix_type, name, filters=None, cache=None):
        """internal function to retrieve a dataframe of item elements"""
        item = self.item(ix_type, name)
        cache_key = (ix_type, name)

        # if dataframe in python cache, retrieve from there
        if cache_key in self._pycache:
            return filtered(self._pycache[cache_key], filters)

        # if no cache, retrieve from Java with filters
        if filters is not None and not self._cache:
            return _get_ele_list(item, filters, **self._java_kwargs[ix_type])

        # otherwise, retrieve from Java and keep in python cache
        df = _get_ele_list(item, None, **self._java_kwargs[ix_type])

        # save if using memcache
        if self._cache:
            self._pycache[cache_key] = df

        return filtered(df, filters)

    def idx_sets(self, name):
        """Return the list of index sets for an item (set, par, var, equ)

        Parameters
        ----------
        name : str
            name of the item
        """
        return to_pylist(self.item('item', name).getIdxSets())

    def idx_names(self, name):
        """return the list of index names for an item (set, par, var, equ)

        Parameters
        ----------
        name : str
            name of the item
        """
        return to_pylist(self.item('item', name).getIdxNames())

    def cat_list(self, name):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def add_cat(self, name, cat, keys, is_unique=False):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def cat(self, name, cat):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def set_list(self):
        """List all defined sets."""
        return to_pylist(self._jobj.getSetList())

    def init_set(self, name, idx_sets=None, idx_names=None):
        """Initialize a new set.

        Parameters
        ----------
        name : str
            Name of the set.
        idx_sets : list of str, optional
            Names of other sets that index this set.
        idx_names : list of str, optional
            Names of the dimensions indexed by `idx_sets`.

        Raises
        ------
        :class:`jpype.JavaException`
            If the set (or another object with the same *name*) already exists.
        """
        self._jobj.initializeSet(name, *make_dims(idx_sets, idx_names))

    def set(self, name, filters=None, **kwargs):
        """Return the (filtered) elements of a set.

        Parameters
        ----------
        name : str
            Name of the set.
        filters : dict
            Mapping of `dimension_name` → `elements`, where `dimension_name`
            is one of the `idx_names` given when the set was initialized (see
            :meth:`init_set`), and `elements` is an iterable of labels to
            include in the return value.

        Returns
        -------
        pandas.DataFrame
        """
        return self.element('set', name, filters, **kwargs)

    def add_set(self, name, key, comment=None):
        """Add elements to an existing set.

        Parameters
        ----------
        name : str
            Name of the set.
        key : str or iterable of str or dict or :class:`pandas.DataFrame`
            Element(s) to be added. If *name* exists, the elements are
            appended to existing elements.
        comment : str or iterable of str, optional
            Comment describing the element(s). Only used if *key* is a string
            or list/range.

        Raises
        ------
        :class:`jpype.JavaException`
            If the set *name* does not exist. :meth:`init_set` must be called
            before :meth:`add_set`.
        """
        self.clear_cache(name=name, ix_type='set')

        jSet = self.item('set', name)

        if sys.version_info[0] > 2 and isinstance(key, range):
            key = list(key)

        if (jSet.getDim() == 0) and isinstance(key, list):
            for i in range(len(key)):
                if comment and i < len(comment):
                    jSet.addElement(str(key[i]), str(comment[i]))
                else:
                    jSet.addElement(str(key[i]))
        elif isinstance(key, pd.DataFrame) or isinstance(key, dict):
            if isinstance(key, dict):
                key = pd.DataFrame.from_dict(key, orient='columns', dtype=None)
            idx_names = self.idx_names(name)
            if "comment" in list(key):
                for i in key.index:
                    jSet.addElement(to_jlist(key.loc[i], idx_names),
                                    str(key['comment'][i]))
            else:
                for i in key.index:
                    jSet.addElement(to_jlist(key.loc[i], idx_names))
        elif isinstance(key, list):
            if isinstance(key[0], list):
                for i in range(len(key)):
                    if comment and i < len(comment):
                        jSet.addElement(to_jlist(
                            key[i]), str(comment[i]))
                    else:
                        jSet.addElement(to_jlist(key[i]))
            else:
                if comment:
                    jSet.addElement(to_jlist(key), str(comment[i]))
                else:
                    jSet.addElement(to_jlist(key))
        else:
            jSet.addElement(str(key), str(comment))

    def remove_set(self, name, key=None):
        """delete a set from the scenario
        or remove an element from a set (if key is specified)

        Parameters
        ----------
        name : str
            name of the set
        key : dataframe or key list or concatenated string
            elements to be removed
        """
        self.clear_cache(name=name, ix_type='set')

        if key is None:
            self._jobj.removeSet(name)
        else:
            _remove_ele(self._jobj.getSet(name), key)

    def par_list(self):
        """List all defined parameters."""
        return to_pylist(self._jobj.getParList())

    def init_par(self, name, idx_sets, idx_names=None):
        """Initialize a new parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        idx_sets : list of str
            Names of sets that index this parameter.
        idx_names : list of str, optional
            Names of the dimensions indexed by `idx_sets`.
        """
        self._jobj.initializePar(name, *make_dims(idx_sets, idx_names))

    def par(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific parameter

        Parameters
        ----------
        name : str
            name of the parameter
        filters : dict
            index names mapped list of index set elements
        """
        return self.element('par', name, filters, **kwargs)

    def add_par(self, name, key, val=None, unit=None, comment=None):
        """Set the values of a parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key : str, list/range of strings/values, dictionary, dataframe
            element(s) to be added
        val : values, list/range of values, optional
            element values (only used if 'key' is a string or list/range)
        unit : str, list/range of strings, optional
            element units (only used if 'key' is a string or list/range)
        comment : str or list/range of strings, optional
            comment (optional, only used if 'key' is a string or list/range)
        """
        self.clear_cache(name=name, ix_type='par')

        jPar = self.item('par', name)

        if sys.version_info[0] > 2 and isinstance(key, range):
            key = list(key)

        if isinstance(key, pd.DataFrame) and "key" in list(key):
            if "comment" in list(key):
                for i in key.index:
                    jPar.addElement(str(key['key'][i]),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]),
                                    str(key['comment'][i]))
            else:
                for i in key.index:
                    jPar.addElement(str(key['key'][i]),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]))

        elif isinstance(key, pd.DataFrame) or isinstance(key, dict):
            if isinstance(key, dict):
                key = pd.DataFrame.from_dict(key, orient='columns', dtype=None)
            idx_names = self.idx_names(name)
            if "comment" in list(key):
                for i in key.index:
                    jPar.addElement(to_jlist(key.loc[i], idx_names),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]),
                                    str(key['comment'][i]))
            else:
                for i in key.index:
                    jPar.addElement(to_jlist(key.loc[i], idx_names),
                                    _jdouble(key['value'][i]),
                                    str(key['unit'][i]))
        elif isinstance(key, list) and isinstance(key[0], list):
            unit = unit or ["???"] * len(key)
            for i in range(len(key)):
                if comment and i < len(comment):
                    jPar.addElement(to_jlist(key[i]), _jdouble(val[i]),
                                    str(unit[i]), str(comment[i]))
                else:
                    jPar.addElement(to_jlist(key[i]), _jdouble(val[i]),
                                    str(unit[i]))
        elif isinstance(key, list) and isinstance(val, list):
            unit = unit or ["???"] * len(key)
            for i in range(len(key)):
                if comment and i < len(comment):
                    jPar.addElement(str(key[i]), _jdouble(val[i]),
                                    str(unit[i]), str(comment[i]))
                else:
                    jPar.addElement(str(key[i]), _jdouble(val[i]),
                                    str(unit[i]))
        elif isinstance(key, list) and not isinstance(val, list):
            jPar.addElement(to_jlist(
                key), _jdouble(val), unit, comment)
        else:
            jPar.addElement(str(key), _jdouble(val), unit, comment)

    def init_scalar(self, name, val, unit, comment=None):
        """Initialize a new scalar.

        Parameters
        ----------
        name : str
            Name of the scalar
        val : number
            Initial value of the scalar.
        unit : str
            Unit of the scalar.
        comment : str, optional
            Description of the scalar.
        """
        jPar = self._jobj.initializePar(name, None, None)
        jPar.addElement(_jdouble(val), unit, comment)

    def scalar(self, name):
        """Return the value and unit of a scalar.

        Parameters
        ----------
        name : str
            Name of the scalar.

        Returns
        -------
        {'value': value, 'unit': unit}
        """
        return _get_ele_list(self._jobj.getPar(name), None, has_value=True)

    def change_scalar(self, name, val, unit, comment=None):
        """Set the value and unit of a scalar.

        Parameters
        ----------
        name : str
            Name of the scalar.
        val : number
            New value of the scalar.
        unit : str
            New unit of the scalar.
        comment : str, optional
            Description of the change.
        """
        self.clear_cache(name=name, ix_type='par')
        self.item('par', name).addElement(_jdouble(val), unit, comment)

    def remove_par(self, name, key=None):
        """Remove parameter values or an entire parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key : dataframe or key list or concatenated string, optional
            elements to be removed
        """
        self.clear_cache(name=name, ix_type='par')

        if key is None:
            self._jobj.removePar(name)
        else:
            _remove_ele(self._jobj.getPar(name), key)

    def var_list(self):
        """List all defined variables."""
        return to_pylist(self._jobj.getVarList())

    def init_var(self, name, idx_sets=None, idx_names=None):
        """initialize a new variable in the scenario

        Parameters
        ----------
        name : str
            name of the item
        idx_sets : list of str
            index set list
        idx_names : list of str, optional
            index name list
        """
        self._jobj.initializeVar(name, *make_dims(idx_sets, idx_names))

    def var(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific variable

        Parameters
        ----------
        name : str
            name of the variable
        filters : dict
            index names mapped list of index set elements
        """
        return self.element('var', name, filters, **kwargs)

    def equ_list(self):
        """List all defined equations."""
        return to_pylist(self._jobj.getEquList())

    def init_equ(self, name, idx_sets=None, idx_names=None):
        """Initialize a new equation.

        Parameters
        ----------
        name : str
            name of the item
        idx_sets : list of str
            index set list
        idx_names : list of str, optional
            index name list
        """
        self._jobj.initializeEqu(name, *make_dims(idx_sets, idx_names))

    def equ(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific equation

        Parameters
        ----------
        name : str
            name of the equation
        filters : dict
            index names mapped list of index set elements
        """
        return self.element('equ', name, filters, **kwargs)

    def clone(self, model=None, scenario=None, annotation=None,
              keep_solution=True, first_model_year=None, platform=None,
              **kwargs):
        """Clone the current scenario and return the clone.

        Parameters
        ----------
        model : str
            new model name
        scenario : str
            new scenario name
        annotation : str, optional
            explanatory comment
        keep_solution : boolean, optional
            indicator whether to include an existing solution
            in the cloned scenario
        first_model_year: int, optional
            new first model year in cloned scenario
            ('slicing', only available for MESSAGE-scheme scenarios)
        platform : :class:`Platform`, optional
            Platform to clone to (default: current platform)
        """
        if 'keep_sol' in kwargs:
            warnings.warn(
                '`keep_sol` is deprecated and will be removed in the next' +
                ' release, please use `keep_solution`')
            keep_solution = kwargs.pop('keep_sol')

        if 'scen' in kwargs:
            warnings.warn(
                '`scen` is deprecated and will be removed in the next' +
                ' release, please use `scenario`')
            scenario = kwargs.pop('scen')

        first_model_year = first_model_year or 0

        platform = self.platform if not platform else platform
        model = self.model if not model else model
        scenario = self.scenario if not scenario else scenario

        return Scenario(platform, model, scenario,
                        version=self._jobj.clone(model, scenario, annotation,
                                                 keep_solution,
                                                 first_model_year),
                        cache=self._cache)

    def to_gdx(self, path, filename, include_var_equ=False):
        """export the scenario data to GAMS gdx

        Parameters
        ----------
        path : str
            path to the folder
        filename : str
            name of the gdx file
        include_var_equ : boolean, optional
            indicator whether to include variables/equations in gdx
        """
        self._jobj.toGDX(path, filename, include_var_equ)

    def read_sol_from_gdx(self, path, filename, comment=None,
                          var_list=None, equ_list=None, check_solution=True):
        """read solution from GAMS gdx and import it to the scenario

        Parameters
        ----------
        path : str
            path to the folder
        filename : str
            name of the gdx file
        comment : str
            comment to be added to the changelog
        var_list : list of str
            variables (levels and marginals) to be imported from gdx
        equ_list : list of str
            equations (levels and marginals) to be imported from gdx
        check_solution : boolean, optional
            raise an error if GAMS did not solve to optimality
            (only applicable for a MESSAGE-scheme scenario)
        """
        self.clear_cache()  # reset Python data cache
        self._jobj.readSolutionFromGDX(path, filename, comment,
                                       to_jlist(var_list), to_jlist(equ_list),
                                       check_solution)

    def has_solution(self):
        """Return :obj:`True` if the Scenario has been solved.

        If ``has_solution() == True``, model solution data is exists in the
        database.
        """
        return self._jobj.hasSolution()

    def remove_solution(self):
        """Delete the model solution.

        Raises
        ------
        ValueError
            If Scenario has no solution.
        """
        if self.has_solution():
            self.clear_cache()  # reset Python data cache
            self._jobj.removeSolution()
        else:
            raise ValueError('this Scenario does not have a solution')

    def solve(self, model, case=None, model_file=None,
              in_file=None, out_file=None, solve_args=None, comment=None,
              var_list=None, equ_list=None, check_solution=True):
        """Solve the model and store output.

        ixmp 'solves' a model using the following steps:

        1. Write all Scenario data to a GDX model input file.
        2. Run GAMS for the specified `model` to perform calculations.
        3. Read the model output, or 'solution', into the database.

        Parameters
        ----------
        model : str
            model (e.g., MESSAGE) or GAMS file name (excluding '.gms')
        case : str
            identifier of gdx file names, defaults to 'model_name_scen_name'
        model_file : str, optional
            path to GAMS file (including '.gms' extension)
        in_file : str, optional
            path to GAMS gdx input file (including '.gdx' extension)
        out_file : str, optional
            path to GAMS gdx output file (including '.gdx' extension)
        solve_args : str, optional
            arguments to be passed to GAMS (input/output file names, etc.)
        comment : str, optional
            additional comment added to changelog when importing the solution
        var_list : list of str, optional
            variables to be imported from the solution
        equ_list : list of str, optional
            equations to be imported from the solution
        check_solution : boolean, optional
            flag whether a non-optimal solution raises an exception
            (only applies to MESSAGE runs)
        """
        config = model_settings.model_config(model) \
            if model_settings.model_registered(model) \
            else model_settings.model_config('default')

        # define case name for gdx export/import, replace spaces by '_'
        case = case or '{}_{}'.format(self.model, self.scenario)
        case = case.replace(" ", "_")

        model_file = model_file or config.model_file.format(model=model)

        # define paths for writing to gdx, running GAMS, and reading a solution
        inp = in_file or config.inp.format(model=model, case=case)
        outp = out_file or config.outp.format(model=model, case=case)
        args = solve_args or [arg.format(model=model, case=case, inp=inp,
                                         outp=outp) for arg in config.args]

        ipth = os.path.dirname(inp)
        ingdx = os.path.basename(inp)
        opth = os.path.dirname(outp)
        outgdx = os.path.basename(outp)

        # write to gdx, execture GAMS, read solution from gdx
        self.to_gdx(ipth, ingdx)
        run_gams(model_file, args)
        self.read_sol_from_gdx(opth, outgdx, comment,
                               var_list, equ_list, check_solution)

    def clear_cache(self, name=None, ix_type=None):
        """clear the Python cache of item elements

        Parameters
        ----------
        name : str, optional
            item name (`None` clears entire Python cache)
        ix_type : str, optional
            type of item (if provided, cache clearing is faster)
        """
        # if no name is given, clean the entire cache
        if name is None:
            self._pycache = {}
            return  # exit early

        # remove this element from the cache if it exists
        key = None
        keys = self._pycache.keys()
        if ix_type is not None:
            key = (ix_type, name) if (ix_type, name) in keys else None
        else:  # look for it
            hits = [k for k in keys if k[1] == name]  # 0 is ix_type, 1 is name
            if len(hits) > 1:
                raise ValueError('Multiple values named {}'.format(name))
            if len(hits) == 1:
                key = hits[0]
        if key is not None:
            self._pycache.pop(key)

    def years_active(self, node, tec, yr_vtg):
        """return a list of years in which a technology of certain vintage
        at a specific node can be active

        Parameters
        ----------
        node : str
            node name
        tec : str
            name of the technology
        yr_vtg : str
            vintage year
        """
        return to_pylist(self._jobj.getTecActYrs(node, tec, str(yr_vtg)))

    def get_meta(self, name=None):
        """get scenario metadata

        Parameters
        ----------
        name : str, optional
            metadata attribute name
        """
        def unwrap(value):
            """Unwrap metadata numeric value (BigDecimal -> Double)"""
            if type(value).__name__ == 'java.math.BigDecimal':
                return value.doubleValue()
            return value
        meta = np.array(self._jobj.getMeta().entrySet().toArray()[:])
        meta = {x.getKey(): unwrap(x.getValue()) for x in meta}
        return meta if name is None else meta[name]

    def set_meta(self, name, value):
        """set scenario metadata

        Parameters
        ----------
        name : str
            metadata attribute name
        value : str or number or bool
            metadata attribute value
        """
        self._jobj.setMeta(name, value)


# %% auxiliary functions for class Scenario


def filtered(df, filters):
    """Returns a filtered dataframe based on a filters dictionary"""
    if filters is None:
        return df

    mask = pd.Series(True, index=df.index)
    for k, v in filters.items():
        isin = df[k].isin(v)
        mask = mask & isin
    return df[mask]


def _jdouble(val):
    """Returns a Java.Double"""
    return java.Double(float(val))


def to_pylist(jlist):
    """Transforms a Java.Array or Java.List to a python list"""
    # handling string array
    try:
        return np.array(jlist[:])
    # handling Java LinkedLists
    except Exception:
        return np.array(jlist.toArray()[:])


def to_jlist(pylist, idx_names=None):
    """Transforms a python list to a Java.LinkedList"""
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
        for idx in idx_names:
            jList.add(str(pylist[idx]))
    return jList


def make_dims(sets, names):
    """Wrapper of `to_jlist()` to generate an index-name and index-set list"""
    if isinstance(sets, set) or isinstance(names, set):
        raise ValueError('index dimension must be string or ordered lists!')
    return to_jlist(sets), to_jlist(names if names is not None else sets)


def _get_ele_list(item, filters=None, has_value=False, has_level=False):

    # get list of elements, with filter HashMap if provided
    if filters is not None:
        jFilter = java.HashMap()
        for idx_name in filters.keys():
            jFilter.put(idx_name, to_jlist(filters[idx_name]))
        jList = item.getElements(jFilter)
    else:
        jList = item.getElements()

    # return a dataframe if this is a mapping or multi-dimensional parameter
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


def _remove_ele(item, key):
    """auxiliary """
    if item.getDim() > 0:
        if isinstance(key, list) or isinstance(key, pd.Series):
            item.removeElement(to_jlist(key))
        elif isinstance(key, pd.DataFrame) or isinstance(key, dict):
            if isinstance(key, dict):
                key = pd.DataFrame.from_dict(key, orient='columns', dtype=None)
            idx_names = to_pylist(item.getIdxNames())
            for i in key.index:
                item.removeElement(to_jlist(key.loc[i], idx_names))
        else:
            item.removeElement(str(key))

    else:
        if isinstance(key, list) or isinstance(key, pd.Series):
            item.removeElement(to_jlist(key))
        else:
            item.removeElement(str(key))


def run_gams(model_file, args, gams_args=['LogOption=4']):
    """Parameters
    ----------
    model : str
        the path to the gams file
    args : list
        arguments related to the GAMS code (input/output gdx paths, etc.)
    gams_args : list of str
        additional arguments for the CLI call to gams
        - `LogOption=4` prints output to stdout (not console) and the log file
    """
    cmd = ['gams', model_file] + args + gams_args
    cmd = cmd if os.name != 'nt' else ' '.join(cmd)
    file_path = os.path.dirname(model_file).strip('"')
    file_path = None if file_path == '' else file_path
    check_call(cmd, shell=os.name == 'nt', cwd=file_path)
