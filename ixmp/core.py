# coding=utf-8
import os
import sys
import warnings
from subprocess import check_call

import jpype
import numpy as np
import pandas as pd
from jpype import JClass
from jpype import JPackage as java

import ixmp as ix
from ixmp import model_settings
from ixmp.config import _config
from ixmp.utils import logger, islistable, check_year, harmonize_path

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


class Platform:
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
                dbprops = _config.find_dbprops(dbprops)
                if dbprops is None:
                    raise ValueError("Not found database properties file "
                                     "to launch platform")
                logger().info("launching ixmp.Platform using config file at "
                              "'{}'".format(dbprops))
                self._jobj = java.ixmp.Platform("Python", str(dbprops))
            # if dbtype is specified, launch Platform with local database
            elif dbtype == 'HSQLDB':
                dbprops = dbprops or _config.get('DEFAULT_LOCAL_DB_PATH')
                logger().info("launching ixmp.Platform with local {} database "
                              "at '{}'".format(dbtype, dbprops))
                self._jobj = java.ixmp.Platform("Python", str(dbprops), dbtype)
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
        """Return information on TimeSeries and Scenarios in the database.

        Parameters
        ----------
        default : bool, optional
            Return *only* the default version of each TimeSeries/Scenario (see
            :meth:`TimeSeries.set_as_default`). Any (`model`, `scenario`)
            without a default version is omitted. If :obj:`False`, return all
            versions.
        model : str, optional
            A model name. If given, only return information for *model*.
        scen : str, optional
            A scenario name. If given, only return information for *scen*.

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
        if unit not in self.units():
            self._jobj.addUnitToDB(unit, comment)
        else:
            msg = 'unit `{}` is already defined in the platform instance'
            logger().info(msg.format(unit))

    def regions(self):
        """Return all regions defined for the IAMC-style timeseries format
        including known synonyms.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        lst = []
        for r in self._jobj.listNodes('%'):
            n, p, h = (r.getName(), r.getParent(), r.getHierarchy())
            lst.extend([(n, None, p, h)])
            lst.extend([(s, n, p, h) for s in (r.getSynonyms() or [])])
        region = pd.DataFrame(lst)
        region.columns = ['region', 'mapped_to', 'parent', 'hierarchy']
        return region

    def add_region(self, region, hierarchy, parent='World'):
        """Define a region including a hierarchy level and a 'parent' region.

        .. tip::
           On a :class:`Platform` backed by a shared database, a region may
           already exist with a different spelling. Use :meth:`regions` first
           to check, and consider calling :meth:`add_region_synonym` instead.

        Parameters
        ----------
        region : str
            Name of the region.
        hierarchy : str
            Hierarchy level of the region (e.g., country, R11, basin)
        parent : str, default 'World'
            Assign a 'parent' region.
        """
        _regions = self.regions()
        if region not in list(_regions['region']):
            self._jobj.addNode(region, parent, hierarchy)
        else:
            _logger_region_exists(_regions, region)

    def add_region_synomym(self, region, mapped_to):
        """Define a synomym for a `region`.

        When adding timeseries data using the synonym in the region column, it
        will be converted to `mapped_to`.

        Parameters
        ----------
        region : str
            Name of the region synonym.
        mapped_to : str
            Name of the region to which the synonym should be mapped.
        """
        _regions = self.regions()
        if region not in list(_regions['region']):
            self._jobj.addNodeSynonym(mapped_to, region)
        else:
            _logger_region_exists(_regions, region)

    def check_access(self, user, models, access='view'):
        """Check access to specific model

        Parameters
        ----------
        user: str
            Registered user name
        models : str or list of str
            Model(s) name
        access : str, optional
            Access type - view or edit
        """

        if isinstance(models, str):
            return self._jobj.checkModelAccess(user, access, models)
        else:
            models_list = java.LinkedList()
            for model in models:
                models_list.add(model)
            access_map = self._jobj.checkModelAccess(user, access, models_list)
            result = {}
            for model in models:
                result[model] = access_map.get(model) == 1
            return result


def _logger_region_exists(_regions, r):
    region = _regions.set_index('region').loc[r]
    msg = 'region `{}` is already defined in the platform instance'
    if region['mapped_to'] is not None:
        msg += ' as synomym for region `{}`'.format(region.mapped_to)
    if region['parent'] is not None:
        msg += ', as subregion of `{}`'.format(region.parent)
    logger().info(msg.format(r))

# %% class TimeSeries


class TimeSeries:
    """Generic collection of data in time series format.

    TimeSeries is the parent/super-class of :class:`Scenario`.

    A TimeSeries is uniquely identified on its :class:`Platform` by three
    values:

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

       Optionally, one `version` may be set as a **default version**. See
       :meth:`set_as_default`.

    Parameters
    ----------
    mp : :class:`Platform`
        ixmp instance in which to store data.
    model : str
        Model name.
    scenario : str
        Scenario name.
    version : int or str, optional
        If omitted and a default version of the (`model`, `scenario`) has been
        designated (see :meth:`set_as_default`), load that version.
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
        """Return :obj:`True` if the :attr:`version` is the default version."""
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

    def preload_timeseries(self):
        """Preload timeseries data to in-memory cache. Useful for bulk updates.
        """
        self._jobj.preloadAllTimeseries()

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

        df = to_iamc_template(df)

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

    def timeseries(self, iamc=False, region=None, variable=None, level=None,
                   unit=None, year=None, **kwargs):
        """Retrieve TimeSeries data.

        Parameters
        ----------
        iamc : bool, default: False
            Return data in wide/'IAMC' format. If :obj:`False`, return data in
            long/'tabular' format; see :meth:`add_timeseries`.
        region : str or list of strings
            Regions to include in returned data.
        variable : str or list of strings
            Variables to include in returned data.
        unit : str or list of strings
            Units to include in returned data.
        year : str, int or list of strings or integers
            Years to include in returned data.

        Returns
        -------
        :class:`pandas.DataFrame`
            Specified data.
        """
        # convert filter lists to Java objects
        region = ix.to_jlist(region)
        variable = ix.to_jlist(variable)
        unit = ix.to_jlist(unit)
        year = ix.to_jlist(year)

        # retrieve data, convert to pandas.DataFrame
        data = self._jobj.getTimeseries(region, variable, unit, None, year)
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
            df.columns = [c if isinstance(c, str) else int(c)
                          for c in df.columns]
            df.columns.names = [None]

        return df

    def remove_timeseries(self, df):
        """Remove timeseries data from the TimeSeries instance.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to remove. `df` must have the following columns:

            - `region` or `node`
            - `variable`
            - `unit`
            - `year`
        """
        df = to_iamc_template(df)
        if 'year' not in df.columns:
            df = pd.melt(df, id_vars=['region', 'variable', 'unit'],
                         var_name='year', value_name='value')
        for name, data in df.groupby(['region', 'variable', 'unit']):
            years = java.LinkedList()
            for y in data['year']:
                years.add(java.Integer(y))
            self._jobj.removeTimeseries(name[0], name[1], None, years, name[2])


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
        If ``'new'``, initialize a new Scenario.
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
        elif version is None:
            self._jobj = mp._jobj.getScenario(model, scenario)
        else:
            raise ValueError('Invalid `version` arg: `{}`'.format(version))

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

    def _item(self, ix_type, name, load=True):
        """Return the Java object for item *name* of *ix_type*.

        Parameters
        ----------
        load : bool, optional
            If *ix_type* is 'par', 'var', or 'equ', the elements of the item
            are loaded from the database before :meth:`_item` returns. If
            :const:`False`, the elements can be loaded later using
            ``item.loadItemElementsfromDB()``.
        """
        funcs = {
            'item': self._jobj.getItem,
            'set': self._jobj.getSet,
            'par': self._jobj.getPar,
            'var': self._jobj.getVar,
            'equ': self._jobj.getEqu,
        }
        # getItem is not overloaded to accept a second bool argument
        args = [name] + ([load] if ix_type != 'item' else [])
        return funcs[ix_type](*args)

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

    def _element(self, ix_type, name, filters=None, cache=None):
        """Return a pd.DataFrame of item elements."""
        item = self._item(ix_type, name)
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
        return to_pylist(self._item('item', name).getIdxSets())

    def idx_names(self, name):
        """return the list of index names for an item (set, par, var, equ)

        Parameters
        ----------
        name : str
            name of the item
        """
        return to_pylist(self._item('item', name).getIdxNames())

    def cat_list(self, name):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def add_cat(self, name, cat, keys, is_unique=False):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def cat(self, name, cat):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def set_list(self):
        """List all defined sets."""
        return to_pylist(self._jobj.getSetList())

    def has_set(self, name):
        """check whether the scenario has a set with that name"""
        return self._jobj.hasSet(name)

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
        return self._element('set', name, filters, **kwargs)

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

        jSet = self._item('set', name)

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

    def has_par(self, name):
        """check whether the scenario has a parameter with that name"""
        return self._jobj.hasPar(name)

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
        return self._element('par', name, filters, **kwargs)

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

        jPar = self._item('par', name)

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
        self._item('par', name).addElement(_jdouble(val), unit, comment)

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

    def has_var(self, name):
        """check whether the scenario has a variable with that name"""
        return self._jobj.hasVar(name)

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
        return self._element('var', name, filters, **kwargs)

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

    def has_equ(self, name):
        """check whether the scenario has an equation with that name"""
        return self._jobj.hasEqu(name)

    def equ(self, name, filters=None, **kwargs):
        """return a dataframe of (filtered) elements for a specific equation

        Parameters
        ----------
        name : str
            name of the equation
        filters : dict
            index names mapped list of index set elements
        """
        return self._element('equ', name, filters, **kwargs)

    def clone(self, model=None, scenario=None, annotation=None,
              keep_solution=True, first_model_year=None, platform=None,
              **kwargs):
        """Clone the current scenario and return the clone.

        If the (`model`, `scenario`) given already exist on the
        :class:`Platform`, the `version` for the cloned Scenario follows the
        last existing version. Otherwise, the `version` for the cloned Scenario
        is 1.

        .. note::
            :meth:`clone` does not set or alter default versions. This means
            that a clone to new (`model`, `scenario`) names has no default
            version, and will not be returned by
            :meth:`Platform.scenario_list` unless `default=False` is given.

        Parameters
        ----------
        model : str, optional
            New model name. If not given, use the existing model name.
        scenario : str, optional
            New scenario name. If not given, use the existing scenario name.
        annotation : str, optional
            Explanatory comment for the clone commit message to the database.
        keep_solution : bool, optional
            If :py:const:`True`, include all timeseries data and the solution
            (vars and equs) from the source scenario in the clone.
            If :py:const:`False`, only include timeseries data marked
            `meta=True` (see :meth:`TimeSeries.add_timeseries`).
        first_model_year: int, optional
            If given, all timeseries data in the Scenario is omitted from the
            clone for years from `first_model_year` onwards. Timeseries data
            with the `meta` flag (see :meth:`TimeSeries.add_timeseries`) are
            cloned for all years.
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

        if keep_solution and first_model_year is not None:
            raise ValueError('Use `keep_solution=False` when cloning with '
                             '`first_model_year`!')

        if platform is not None and not keep_solution:
            raise ValueError('Cloning across platforms is only possible '
                             'with `keep_solution=True`!')

        platform = platform or self.platform
        model = model or self.model
        scenario = scenario or self.scenario
        args = [platform._jobj, model, scenario, annotation, keep_solution]
        if check_year(first_model_year, 'first_model_year'):
            args.append(first_model_year)

        scenario_class = self.__class__
        return scenario_class(platform, model, scenario, cache=self._cache,
                              version=self._jobj.clone(*args))

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

        If ``has_solution() == True``, model solution data exists in the db.
        """
        return self._jobj.hasSolution()

    def remove_solution(self, first_model_year=None):
        """Remove the solution from the scenario

        This function removes the solution (variables and equations) and
        timeseries data marked as `meta=False` from the scenario
        (see :meth:`TimeSeries.add_timeseries`).

        Parameters
        ----------
        first_model_year: int, optional
            If given, timeseries data marked as `meta=False` is removed
            only for years from `first_model_year` onwards.

        Raises
        ------
        ValueError
            If Scenario has no solution or if `first_model_year` is not `int`.
        """
        if self.has_solution():
            self.clear_cache()  # reset Python data cache
            if check_year(first_model_year, 'first_model_year'):
                self._jobj.removeSolution(first_model_year)
            else:
                self._jobj.removeSolution()
        else:
            raise ValueError('This Scenario does not have a solution!')

    def solve(self, model, case=None, model_file=None, in_file=None,
              out_file=None, solve_args=None, comment=None, var_list=None,
              equ_list=None, check_solution=True, callback=None,
              gams_args=['LogOption=4'], cb_kwargs={}):
        """Solve the model and store output.

        ixmp 'solves' a model using the following steps:

        1. Write all Scenario data to a GDX model input file.
        2. Run GAMS for the specified `model` to perform calculations.
        3. Read the model output, or 'solution', into the database.

        If the optional argument `callback` is given, then additional steps are
        performed:

        4. Execute the `callback` with the Scenario as an argument. The
           Scenario has an `iteration` attribute that stores the number of
           times the underlying model has been solved (#2).
        5. If the `callback` returns :obj:`False` or similar, go to #1;
           otherwise exit.

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
        callback : callable, optional
            Method to execute arbitrary non-model code. Must accept a single
            argument, the Scenario. Must return a non-:obj:`False` value to
            indicate convergence.
        gams_args : list of str, optional
            additional arguments for the CLI call to gams
            - `LogOption=4` prints output to stdout (not console) and the log file
        cb_kwargs : dict, optional
            Keyword arguments to pass to `callback`.

        Warns
        -----
        UserWarning
            If `callback` is given and returns :obj:`None`. This may indicate
            that the user has forgotten a ``return`` statement, in which case
            the iteration will continue indefinitely.

        Raises
        ------
        ValueError
            If the Scenario has already been solved.
        """
        if self.has_solution():
            raise ValueError('This Scenario has already been solved, ',
                             'use `remove_solution()` first!')

        model = str(harmonize_path(model))
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

        # Validate *callback* argument
        if callback is not None and not callable(callback):
            raise ValueError('callback={!r} is not callable'.format(callback))
        elif callback is None:
            # Make the callback a no-op
            def callback(scenario, **kwargs):
                return True

        warn_none = True

        # Iterate until convergence
        while True:
            # Write model data to file
            self.to_gdx(ipth, ingdx)

            # Invoke GAMS
            run_gams(model_file, args, gams_args=gams_args)

            # Read model solution
            self.read_sol_from_gdx(opth, outgdx, comment,
                                   var_list, equ_list, check_solution)

            # Store an iteration number to help the callback
            if not hasattr(self, 'iteration'):
                self.iteration = 0

            self.iteration += 1

            # Invoke the callback
            cb_result = callback(self, **cb_kwargs)

            if cb_result is None and warn_none:
                warnings.warn('solve(callback=...) argument returned None;'
                              ' will loop indefinitely unless True is'
                              ' returned.')
                # Don't repeat the warning
                warn_none = False

            if cb_result:
                # Callback indicates convergence is reached
                break

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


def to_iamc_template(df):
    """Formats a pd.DataFrame to an IAMC-compatible table"""
    if "time" in df.columns:
        raise("sub-annual time slices not supported by the Python interface!")

    # reset the index if meaningful entries are included there
    if not list(df.index.names) == [None]:
        df.reset_index(inplace=True)

    # rename columns to standard notation
    cols = {c: str(c).lower() for c in df.columns}
    cols.update(node='region')
    df = df.rename(columns=cols)
    required_cols = ['region', 'variable', 'unit']
    if not set(required_cols).issubset(set(df.columns)):
        missing = list(set(required_cols) - set(df.columns))
        raise ValueError("missing required columns `{}`!".format(missing))

    return df


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
