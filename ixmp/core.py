# coding=utf-8
import inspect
from itertools import repeat, zip_longest
import logging
import os
import sys
from subprocess import check_call
import warnings
from warnings import warn

import numpy as np
import pandas as pd

from ixmp import model_settings
from .backend import BACKENDS

# TODO remove these direct imports of Java-related methods
from .backend.jdbc import (
    JLinkedList,
    JLinkedHashMap,
    JInt,
    JDouble,
    to_jdouble as _jdouble,
    to_jlist,
    to_pylist,
    filtered,
)
from ixmp.utils import (
    as_str_list,
    check_year,
    harmonize_path,
    logger,
    numcols,
)

# %% default settings for column headers

IAMC_IDX = ['model', 'scenario', 'region', 'variable', 'unit']


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
    backend : 'jdbc'
        Storage backend type. Currently 'jdbc' is the only available backend.
    backend_kwargs
        Keyword arguments to configure the backend; see below.

    Other parameters
    ----------------
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

    # List of method names which are handled directly by the backend
    _backend_direct = [
        'open_db',
        'close_db',
        'units',
        ]

    def __init__(self, *args, backend='jdbc', **backend_args):
        if backend != 'jdbc':
            raise ValueError(f'unknown ixmp backend {backend!r}')
        else:
            # Copy positional args for the default JDBC backend
            print(args, backend_args)
            for i, arg in enumerate(['dbprops', 'dbtype', 'jvmargs']):
                if len(args) > i:
                    backend_args[arg] = args[i]

        backend_cls = BACKENDS[backend]
        self._backend = backend_cls(**backend_args)

    @property
    def _jobj(self):
        """Shim to allow existing code that references ._jobj to work."""
        # TODO address all such warnings, then remove
        loc = inspect.stack()[1].function
        warn(f'Accessing Platform._jobj in {loc}')
        return self._backend.jobj

    def __getattr__(self, name):
        """Convenience for methods of Backend."""
        return getattr(self._backend, name)

    def set_log_level(self, level):
        """Set global logger level.

        Parameters
        ----------
        level : str
            set the logger level if specified, see
            https://docs.python.org/3/library/logging.html#logging-levels
        """
        if level not in dir(logging):
            msg = '{} not a valid Python logger level, see ' + \
                'https://docs.python.org/3/library/logging.html#logging-level'
            raise ValueError(msg.format(level))
        logger().setLevel(level)
        self._backend.set_log_level(level)

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
            models_list = JLinkedList()
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
    #: Name of the model associated with the TimeSeries
    model = None

    #: Name of the scenario associated with the TimeSeries
    scenario = None

    #: Version of the TimeSeries. Immutable for a specific instance.
    version = None

    def __init__(self, mp, model, scenario, version=None, annotation=None):
        if not isinstance(mp, Platform):
            raise ValueError('mp is not a valid `ixmp.Platform` instance')

        # Set attributes
        self.model = model
        self.scenario = scenario
        self.version = version

        # All the backend to complete initialization
        self.platform = mp
        self._backend('init', annotation)

    @property
    def _jobj(self):
        """Shim to allow existing code that references ._jobj to work."""
        # TODO address all such warnings, then remove
        loc = inspect.stack()[1].function
        warn(f'Accessing {self.__class__.__name__}._jobj in {loc}')
        return self.platform._backend.jindex[self]

    def _backend(self, method, *args, **kwargs):
        """Convenience for calling *method* on the backend."""
        func = getattr(self.platform._backend, f'ts_{method}')
        return func(self, *args, **kwargs)

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
        self._backend('discard_changes')

    def set_as_default(self):
        """Set the current :attr:`version` as the default."""
        self._backend('set_as_default')

    def is_default(self):
        """Return :obj:`True` if the :attr:`version` is the default version."""
        return self._backend('is_default')

    def last_update(self):
        """get the timestamp of the last update/edit of this TimeSeries"""
        return self._backend('last_update')

    def run_id(self):
        """get the run id of this TimeSeries"""
        return self._backend('run_id')

    # functions for importing and retrieving timeseries data

    def preload_timeseries(self):
        """Preload timeseries data to in-memory cache. Useful for bulk updates.
        """
        self._backend('preload')

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
            jData = JLinkedHashMap()

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
                    jData = JLinkedHashMap()

                jData.put(JInt(int(df.year[i])),
                          JDouble(float(df.value[i])))
            # add the final iteration of the loop
            self._jobj.addTimeseries(region, variable, time, jData, unit, meta)

        # if in 'IAMC-style' format
        else:
            for i in df.index:
                jData = JLinkedHashMap()

                for j in numcols(df):
                    jData.put(JInt(int(j)),
                              JDouble(float(df[j][i])))

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
        region = to_jlist(region)
        variable = to_jlist(variable)
        unit = to_jlist(unit)
        year = to_jlist(year)

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
            years = JLinkedList()
            for y in data['year']:
                years.add(JInt(y))
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

        # Set attributes
        self.model = model
        self.scenario = scenario
        self.version = version

        # All the backend to complete initialization
        self.platform = mp
        self._backend('init', scheme, annotation)

        if self.scheme == 'MESSAGE' and not hasattr(self, 'is_message_scheme'):
            warnings.warn('Using `ixmp.Scenario` for MESSAGE-scheme scenarios '
                          'is deprecated, please use `message_ix.Scenario`')

        # Initialize cache
        self._cache = cache
        self._pycache = {}

    def _backend(self, method, *args, **kwargs):
        """Convenience for calling *method* on the backend."""
        try:
            func = getattr(self.platform._backend, f's_{method}')
        except AttributeError:
            func = getattr(self.platform._backend, f'ts_{method}')
        return func(self, *args, **kwargs)

    def _item(self, ix_type, name, load=True):
        """Shim to allow existing code that references ._item to work."""
        # TODO address all such warnings, then remove
        loc = inspect.stack()[1].function
        warn(f'Calling {self.__class__.__name__}._item() in {loc}')
        return self.platform._backend._get_item(self, ix_type, name)

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
        cache_key = (ix_type, name)

        # if dataframe in python cache, retrieve from there
        if cache_key in self._pycache:
            return filtered(self._pycache[cache_key], filters)

        # if no cache, retrieve from Java with filters
        if filters is not None and not self._cache:
            return self._backend('item_elements', ix_type, name, filters,
                                 **self._java_kwargs[ix_type])

        # otherwise, retrieve from Java and keep in python cache
        df = self._backend('item_elements', ix_type, name, None,
                           **self._java_kwargs[ix_type])

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
        return self._backend('item_index', name, 'sets')

    def idx_names(self, name):
        """return the list of index names for an item (set, par, var, equ)

        Parameters
        ----------
        name : str
            name of the item
        """
        return self._backend('item_index', name, 'names')

    def cat_list(self, name):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def add_cat(self, name, cat, keys, is_unique=False):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def cat(self, name, cat):
        raise DeprecationWarning('function was migrated to `message_ix` class')

    def set_list(self):
        """List all defined sets."""
        return self._backend('list_items', 'set')

    def has_set(self, name):
        """Check whether the scenario has a set *name*."""
        return name in self.set_list()

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
        return self._backend('init_item', 'set', name, idx_sets, idx_names)

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
            Comment describing the element(s). If given, there must be the
            same number of comments as elements.

        Raises
        ------
        KeyError
            If the set *name* does not exist. :meth:`init_set` must be called
            before :meth:`add_set`.
        ValueError
            For invalid forms or combinations of *key* and *comment*.
        """
        # TODO expand docstring (here or in doc/source/api.rst) with examples,
        #      per test_core.test_add_set.
        self.clear_cache(name=name, ix_type='set')

        # Get index names for set *name*, may raise KeyError
        idx_names = self.idx_names(name)

        # Check arguments and convert to two lists: keys and comments
        if len(idx_names) == 0:
            # Basic set. Keys must be strings.
            if isinstance(key, (dict, pd.DataFrame)):
                raise ValueError('dict, DataFrame keys invalid for '
                                 f'basic set {name!r}')

            # Ensure keys is a list of str
            keys = as_str_list(key)
        else:
            # Set defined over 1+ other sets

            # Check for ambiguous arguments
            if comment and isinstance(key, (dict, pd.DataFrame)) and \
                    'comment' in key:
                raise ValueError("ambiguous; both key['comment'] and comment "
                                 "given")

            if isinstance(key, pd.DataFrame):
                # DataFrame of key values and perhaps comments
                try:
                    # Pop a 'comment' column off the DataFrame, convert to list
                    comment = key.pop('comment').to_list()
                except KeyError:
                    pass

                # Convert key to list of list of key values
                keys = []
                for row in key.to_dict(orient='records'):
                    keys.append(as_str_list(row, idx_names=idx_names))
            elif isinstance(key, dict):
                # Dict of lists of key values

                # Pop a 'comment' list from the dict
                comment = key.pop('comment', None)

                # Convert to list of list of key values
                keys = list(map(as_str_list,
                                zip(*[key[i] for i in idx_names])))
            elif isinstance(key[0], str):
                # List of key values; wrap
                keys = [as_str_list(key)]
            elif isinstance(key[0], list):
                # List of lists of key values; convert to list of list of str
                keys = map(as_str_list, key)
            elif isinstance(key, str) and len(idx_names) == 1:
                # Bare key given for a 1D set; wrap for convenience
                keys = [[key]]
            else:
                # Other, invalid value
                raise ValueError(key)

        # Process comments to a list of str, or let them all be None
        comments = as_str_list(comment) if comment else repeat(None, len(keys))

        # Combine iterators to tuples. If the lengths are mismatched, the
        # sentinel value 'False' is filled in
        to_add = list(zip_longest(keys, comments, fillvalue=False))

        # Check processed arguments
        for e, c in to_add:
            # Check for sentinel values
            if e is False:
                raise ValueError(f'Comment {c!r} without matching key')
            elif c is False:
                raise ValueError(f'Key {e!r} without matching comment')
            elif len(idx_names) and len(idx_names) != len(e):
                raise ValueError(f'{len(e)}-D key {e!r} invalid for '
                                 f'{len(idx_names)}-D set {name}{idx_names!r}')

        # Send to backend
        self._backend('add_set_elements', name, to_add)

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
        return self._backend('list_items', 'par')

    def has_par(self, name):
        """check whether the scenario has a parameter with that name"""
        return name in self.par_list()

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
        return self._backend('init_item', 'par', name, idx_sets, idx_names)

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
        self.init_par(name, None, None)
        jPar = self._item('par', name)
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
        return self._backend('item_elements', 'par', name, None,
                             has_value=True)

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
        return self._backend('list_items', 'var')

    def has_var(self, name):
        """check whether the scenario has a variable with that name"""
        return name in self.var_list()

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
        return self._backend('init_item', 'var', name, idx_sets, idx_names)

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
        return self._backend('list_items', 'equ')

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
        return self._backend('init_item', 'equ', name, idx_sets, idx_names)

    def has_equ(self, name):
        """check whether the scenario has an equation with that name"""
        return name in self.equ_list()

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
        return self._backend('has_solution')

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
            Additional arguments for the CLI call to GAMS. See, e.g.,
            https://www.gams.com/latest/docs/UG_GamsCall.html#UG_GamsCall_ListOfCommandLineParameters

            - `LogOption=4` prints output to stdout (not console) and the log
              file.
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
