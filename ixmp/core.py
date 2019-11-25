from functools import partial
from itertools import repeat, zip_longest
import logging
from warnings import warn

import pandas as pd

from ._config import config
from .backend import BACKENDS, FIELDS
from .model import get_model
from .utils import (
    as_str_list,
    check_year,
    logger,
    parse_url
)

log = logging.getLogger(__name__)

# %% default settings for column headers

IAMC_IDX = ['model', 'scenario', 'region', 'variable', 'unit']


class Platform:
    """Instance of the modeling platform.

    A Platform connects two key components:

    1. A **back end** for storing data such as model inputs and outputs.
    2. One or more **model(s)**; codes in Python or other languages or
       frameworks that run, via :meth:`Scenario.solve`, on the data stored in
       the Platform.

    The Platform parameters control these components. :class:`TimeSeries` and
    :class:`Scenario` objects tied to a single Platform; to move data between
    platforms, see :meth:`Scenario.clone`.

    Parameters
    ----------
    backend : 'jdbc'
        Storage backend type. 'jdbc' corresponds to the built-in
        :class:`.JDBCBackend`; see :obj:`.BACKENDS`.
    backend_args
        Keyword arguments to specific to the `backend`. The “Other Parameters”
        shown below are specific to :class:`.JDBCBackend`.

    Other parameters
    ----------------
    dbtype : 'HSQLDB', optional
        Database type to use. If :obj:`None`, a remote database is accessed. If
        'HSQLDB', a local database is created and used at the path given by
        `dbprops`.

    dbprops : path-like, optional
        If `dbtype` is :obj:`None`, the name of a *database properties file*
        (default: ``default.properties``) in the properties file directory
        (see :class:`.Config`) or a path to a properties file.

        If `dbtype` is 'HSQLDB'`, the path of a local database,
        (default: ``$HOME/.local/ixmp/localdb/default``) or name of a
        database file in the local database directory (default:
        ``$HOME/.local/ixmp/localdb/``).

    jvmargs : str, optional
        Java Virtual Machine arguments. See :func:`.start_jvm`.
    """

    # List of method names which are handled directly by the backend
    _backend_direct = [
        'open_db',
        'close_db',
    ]

    def __init__(self, *args, name=None, backend=None, **backend_args):
        if name is None:
            if backend is None and not len(backend_args):
                # No arguments given: use the default platform config
                name = 'default'
            elif backend is None:
                # Only backend_args given
                log.info('Using default JDBC backend')
                kwargs = {'class': 'jdbc'}
            else:
                # Backend and maybe backend_args were given
                kwargs = {'class': backend}

        if name:
            # Using a named platform config; retrieve it
            self.name, kwargs = config.get_platform_info(name)

        if len(args):
            # Handle deprecated positional arguments
            if backend and backend != 'jdbc':
                message = ('backend={!r} conflicts with deprecated positional '
                           'arguments for JDBCBackend (dbprops, dbtype, '
                           'jvmargs)').format(backend)
                raise ValueError(message)
            elif backend is None:
                # Providing positional args implies JDBCBackend
                kwargs['class'] = 'jdbc'

            warn('positional arguments to Platform(…) for JDBCBackend. '
                 'Use keyword arguments driver=, dbprops=, and/or jvmargs=',
                 DeprecationWarning)

            # Copy positional args to keyword args
            backend_args.update(zip(['dbprops', 'dbtype', 'jvmargs'], args))

        # Overwrite any platform config with explicit keyword arguments
        kwargs.update(backend_args)

        # Retrieve the Backend class
        try:
            backend_class = kwargs.pop('class')
            backend_class = BACKENDS[backend_class]
        except KeyError:
            raise ValueError('backend class {!r} not among {}'
                             .format(backend_class, sorted(BACKENDS.keys())))

        # Instantiate the backend
        self._backend = backend_class(**kwargs)

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
        """Return information about TimeSeries and Scenarios on the Platform.

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
        return pd.DataFrame(self._backend.get_scenarios(default, model, scen),
                            columns=FIELDS['get_scenarios'])

    def Scenario(self, model, scen, version=None,
                 scheme=None, annotation=None, cache=False):
        """Initialize a new :class:`Scenario`.

        .. deprecated:: 1.1.0

           Instead, use:

           >>> mp = ixmp.Platform(…)
           >>> ixmp.Scenario(mp, …)
        """

        warn('The constructor `mp.Scenario()` is deprecated, please use '
             '`ixmp.Scenario(mp, ...)`')

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
        if unit in self.units():
            msg = 'unit `{}` is already defined in the platform instance'
            logger().info(msg.format(unit))
            return

        self._backend.set_unit(unit, comment)

    def units(self):
        return self._backend.get_units()

    def regions(self):
        """Return all regions defined for the IAMC-style timeseries format
        including known synonyms.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        return pd.DataFrame(self._backend.get_nodes(),
                            columns=FIELDS['get_nodes'])

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
        parent : str, default 'World'
            Assign a 'parent' region.
        hierarchy : str
            Hierarchy level of the region (e.g., country, R11, basin)
        """
        for r in self._backend.get_nodes():
            if r[1] == region:
                _logger_region_exists(self.regions(), region)
                return

        self._backend.set_node(region, parent, hierarchy)

    def add_region_synonym(self, region, mapped_to):
        """Define a synonym for a `region`.

        When adding timeseries data using the synonym in the region column, it
        will be converted to `mapped_to`.

        Parameters
        ----------
        region : str
            Name of the region synonym.
        mapped_to : str
            Name of the region to which the synonym should be mapped.
        """
        for r in self._backend.get_nodes():
            if r[1] == region:
                _logger_region_exists(self.regions(), region)
                return

        self._backend.set_node(region, synonym=mapped_to)

    def check_access(self, user, models, access='view'):
        """Check access to specific models.

        Parameters
        ----------
        user: str
            Registered user name
        models : str or list of str
            Model(s) name
        access : str, optional
            Access type - view or edit

        Returns
        -------
        bool or dict of bool
        """
        models_list = as_str_list(models)
        result = self._backend.get_auth(user, models_list, access)
        if isinstance(models, str):
            return result[models]
        else:
            return {model: result.get(model) == 1 for model in models_list}


def _logger_region_exists(_regions, r):
    region = _regions.set_index('region').loc[r]
    msg = 'region `{}` is already defined in the platform instance'
    if region['mapped_to'] is not None:
        msg += ' as synonym for region `{}`'.format(region.mapped_to)
    if region['parent'] is not None:
        msg += ', as subregion of `{}`'.format(region.parent)
    logger().info(msg.format(r))

# %% class TimeSeries


class TimeSeries:
    """Collection of data in time series format.

    TimeSeries is the parent/super-class of :class:`Scenario`.

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
        self.platform = mp
        self.model = model
        self.scenario = scenario

        if version == 'new':
            self._backend('init_ts', annotation)
        elif isinstance(version, int) or version is None:
            self._backend('get', version)
        else:
            raise ValueError(f'version={version!r}')

    def _backend(self, method, *args, **kwargs):
        """Convenience for calling *method* on the backend."""
        return self.platform._backend(self, method, *args, **kwargs)

    # functions for platform management

    def check_out(self, timeseries_only=False):
        """Check out the TimeSeries for modification."""
        if not timeseries_only and self.has_solution():
            raise ValueError('This Scenario has a solution, '
                             'use `Scenario.remove_solution()` or '
                             '`Scenario.clone(..., keep_solution=False)`'
                             )
        self._backend('check_out', timeseries_only)

    def commit(self, comment):
        """Commit all changed data to the database.

        If the TimeSeries was newly created (with ``version='new'``),
        :attr:`version` is updated with a new version number assigned by the
        backend. Otherwise, :meth:`commit` does not change the :attr:`version`.

        Parameters
        ----------
        comment : str
            Description of the changes being committed.
        """
        self._backend('commit', comment)

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
        meta = bool(meta)

        # Ensure consistent column names
        df = to_iamc_template(df)

        if 'value' in df.columns:
            # Long format; pivot to wide
            df = pd.pivot_table(df,
                                values='value',
                                index=['region', 'variable', 'unit'],
                                columns=['year'])
        else:
            # Wide format: set index columns
            df.set_index(['region', 'variable', 'unit'], inplace=True)

        # Discard non-numeric columns, e.g. 'model', 'scenario'
        num_cols = [pd.api.types.is_numeric_dtype(dt) for dt in df.dtypes]
        df = df.iloc[:, num_cols]

        # Columns (year) as integer
        df.columns = df.columns.astype(int)

        # Add one time series per row
        for (r, v, u), data in df.iterrows():
            # Values as float; exclude NA
            self._backend('set_data', r, v, data.astype(float).dropna(), u,
                          meta)

    def timeseries(self, region=None, variable=None, unit=None, year=None,
                   iamc=False):
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
        # Retrieve data, convert to pandas.DataFrame
        df = pd.DataFrame(self._backend('get_data',
                                        as_str_list(region) or [],
                                        as_str_list(variable) or [],
                                        as_str_list(unit) or [],
                                        as_str_list(year) or []),
                          columns=FIELDS['ts_get'])
        df['model'] = self.model
        df['scenario'] = self.scenario

        if iamc:
            # Convert to wide format
            df = df.pivot_table(index=IAMC_IDX, columns='year')['value'] \
                   .reset_index()
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
        # Ensure consistent column names
        df = to_iamc_template(df)

        if 'year' not in df.columns:
            # Reshape from wide to long format
            df = pd.melt(df, id_vars=['region', 'variable', 'unit'],
                         var_name='year', value_name='value')

        # Remove all years for a given (r, v, u) combination at once
        for (r, v, u), data in df.groupby(['region', 'variable', 'unit']):
            self._backend('delete', r, v, data['year'].tolist(), u)

    def add_geodata(self, df):
        """Add geodata (layers) to the TimeSeries.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to add. `df` must have the following columns:

            - `region`
            - `variable`
            - `time`
            - `unit`
            - `year`
            - `value`
            - `meta`
        """
        for _, row in df.astype({'year': int, 'meta': int}).iterrows():
            self._backend('set_geo', row.region, row.variable, row.time,
                          row.year, row.value, row.unit, row.meta)

    def remove_geodata(self, df):
        """Remove geodata from the TimeSeries instance.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to remove. `df` must have the following columns:

            - `region`
            - `variable`
            - `unit`
            - `time`
            - `year`
        """
        # Remove all years for a given (r, v, t, u) combination at once
        for (r, v, t, u), data in df.groupby(['region', 'variable', 'time',
                                              'unit']):
            self._backend('delete_geo', r, v, t, data['year'].tolist(), u)

    def get_geodata(self):
        """Fetch geodata and return it as dataframe.

        Returns
        -------
        :class:`pandas.DataFrame`
            Specified data.
        """
        return pd.DataFrame(self._backend('get_geo'),
                            columns=FIELDS['ts_get_geo'])


# %% class Scenario

class Scenario(TimeSeries):
    """Collection of model-related data.

    See :class:`TimeSeries` for the meaning of parameters `mp`, `model`,
    `scenario`, `version`, and `annotation`.

    Parameters
    ----------
    scheme : str, optional
        Use an explicit scheme for initializing a new scenario.
    cache : bool, optional
        Store data in memory and return cached values instead of repeatedly
        querying the backend.
    """
    #: Scheme of the Scenario.
    scheme = None

    def __init__(self, mp, model, scenario, version=None, scheme=None,
                 annotation=None, cache=False):
        if not isinstance(mp, Platform):
            raise ValueError('mp is not a valid `ixmp.Platform` instance')

        # Set attributes
        self.platform = mp
        self.model = model
        self.scenario = scenario

        if version == 'new':
            self._backend('init_s', scheme, annotation)
        elif isinstance(version, int) or version is None:
            self._backend('get', version)
        else:
            raise ValueError(f'version={version!r}')

        if self.scheme == 'MESSAGE' and not hasattr(self, 'is_message_scheme'):
            warn('Using `ixmp.Scenario` for MESSAGE-scheme scenarios is '
                 'deprecated, please use `message_ix.Scenario`')

    @property
    def _cache(self):
        return hasattr(self.platform._backend, '_cache')

    @classmethod
    def from_url(cls, url, errors='warn'):
        """Instantiate a Scenario given an ixmp-scheme URL.

        The following are equivalent::

            from ixmp import Platform, Scenario
            mp = Platform(name='example')
            scen = Scenario(mp 'model', 'scenario', version=42)

        and::

            from ixmp import Scenario
            scen, mp = Scenario.from_url('ixmp://example/model/scenario#42')

        Parameters
        ----------
        url : str
            See :meth:`parse_url <ixmp.utils.parse_url>`.
        errors : 'warn' or 'raise'
            If 'warn', a failure to load the Scenario is logged as a warning,
            and the platform is still returned. If 'raise', the exception
            is raised.

        Returns
        -------
        scenario, platform : 2-tuple of (Scenario, :class:`Platform`)
            The Scenario and Platform referred to by the URL.
        """
        assert errors in ('warn', 'raise'), "errors= must be 'warn' or 'raise'"

        platform_info, scenario_info = parse_url(url)
        platform = Platform(**platform_info)

        try:
            scenario = cls(platform, **scenario_info)
        except Exception as e:
            if errors == 'warn':
                log.warning('{}: {}\nwhen loading Scenario from url {}'
                            .format(e.__class__.__name__, e.args[0], url))
                return None, platform
            else:
                raise
        else:
            return scenario, platform

    def load_scenario_data(self):
        """Load all Scenario data into memory.

        Raises
        ------
        ValueError
            If the Scenario was instantiated with ``cache=False``.
        """
        if not self._cache:
            raise ValueError('Cache must be enabled to load scenario data')

        for ix_type in 'equ', 'par', 'set', 'var':
            logger().info('Caching {} data'.format(ix_type))
            get_func = getattr(self, ix_type)
            for name in getattr(self, '{}_list'.format(ix_type))():
                get_func(name)

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

    def _keys(self, name, key_or_keys):
        if isinstance(key_or_keys, (list, pd.Series)):
            return as_str_list(key_or_keys)
        elif isinstance(key_or_keys, (pd.DataFrame, dict)):
            if isinstance(key_or_keys, dict):
                key_or_keys = pd.DataFrame.from_dict(key_or_keys,
                                                     orient='columns')
            idx_names = self.idx_names(name)
            return [as_str_list(row, idx_names)
                    for _, row in key_or_keys.iterrows()]
        else:
            return [str(key_or_keys)]

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
        ValueError
            If the set (or another object with the same *name*) already exists.
        RuntimeError
            If the Scenario is not checked out (see
            :meth:`~TimeSeries.check_out`).
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
        return self._backend('item_get_elements', 'set', name, filters)

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
                keys = list(map(as_str_list, key))
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
        elements = ((kc[0], None, None, kc[1]) for kc in to_add)
        self._backend('item_set_elements', 'set', name, elements)

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
        if key is None:
            self._backend('delete_item', 'set', name)
        else:
            self._backend('item_delete_elements', 'set', name,
                          self._keys(name, key))

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
        """Return parameter data.

        If *filters* is provided, only a subset of data, matching the filters,
        is returned.

        Parameters
        ----------
        name : str
            Name of the parameter
        filters : dict (str -> list of str), optional
            Index names mapped to lists of index set elements. Elements not
            appearing in the respective index set(s) are silently ignored.
        """
        return self._backend('item_get_elements', 'par', name, filters)

    def add_par(self, name, key_or_data=None, value=None, unit=None,
                comment=None, key=None, val=None):
        """Set the values of a parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key_or_data : str or iterable of str or range or dict or \
                      pandas.DataFrame
            Element(s) to be added.
        value : numeric or iterable of numeric, optional
            Values.
        unit : str or iterable of str, optional
            Unit symbols.
        comment : str or iterable of str, optional
            Comment(s) for the added values.
        """
        # Number of dimensions in the index of *name*
        idx_names = self.idx_names(name)
        N_dim = len(idx_names)

        if key:
            warn("Scenario.add_par(key=...) deprecated and will be removed in "
                 "ixmp 2.0; use key_or_data", DeprecationWarning)
            key_or_data = key
        if val:
            warn("Scenario.add_par(val=...) deprecated and will be removed in "
                 "ixmp 2.0; use value", DeprecationWarning)
            value = val

        # Convert valid forms of arguments to pd.DataFrame
        if isinstance(key_or_data, dict):
            # dict containing data
            data = pd.DataFrame.from_dict(key_or_data, orient='columns')
        elif isinstance(key_or_data, pd.DataFrame):
            data = key_or_data
            if 'value' in data.columns and value is not None:
                raise ValueError('both key_or_data.value and value supplied')
        else:
            # One or more keys; convert to a list of strings
            if isinstance(key_or_data, range):
                key_or_data = list(key_or_data)
            keys = self._keys(name, key_or_data)

            # Check the type of value
            if isinstance(value, (float, int)):
                # Single value
                values = [float(value)]

                if N_dim > 1 and len(keys) == N_dim:
                    # Ambiguous case: ._key() above returns ['dim_0', 'dim_1'],
                    # when we really want [['dim_0', 'dim_1']]
                    keys = [keys]
            else:
                # Multiple values
                values = value

            data = pd.DataFrame(zip_longest(keys, values),
                                columns=['key', 'value'])
            if data.isna().any(axis=None):
                raise ValueError('Length mismatch between keys and values')

        # Column types
        types = {
            'key': str if N_dim == 1 else object,
            'value': float,
            'unit': str,
            'comment': str,
        }

        # Further handle each column
        if 'key' not in data.columns:
            # Form the 'key' column from other columns
            if N_dim > 1:
                data['key'] = data.apply(partial(as_str_list,
                                                 idx_names=idx_names),
                                         axis=1)
            else:
                data['key'] = data[idx_names[0]]

        if 'unit' not in data.columns:
            # Broadcast single unit across all values. pandas raises ValueError
            # if *unit* is iterable but the wrong length
            data['unit'] = unit or '???'

        if 'comment' not in data.columns:
            if comment:
                # Broadcast single comment across all values. pandas raises
                # ValueError if *comment* is iterable but the wrong length
                data['comment'] = comment
            else:
                # Store a 'None' comment
                data['comment'] = None
                types.pop('comment')

        # Convert types, generate tuples
        elements = ((e.key, e.value, e.unit, e.comment)
                    for e in data.astype(types).itertuples())

        # Store
        self._backend('item_set_elements', 'par', name, elements)

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
        self.change_scalar(name, val, unit, comment)

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
        return self._backend('item_get_elements', 'par', name, None)

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
        self._backend('item_set_elements', 'par', name,
                      [(None, float(val), unit, comment)])

    def remove_par(self, name, key=None):
        """Remove parameter values or an entire parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key : dataframe or key list or concatenated string, optional
            elements to be removed
        """
        if key is None:
            self._backend('delete_item', 'par', name)
        else:
            self._backend('item_delete_elements', 'par', name,
                          self._keys(name, key))

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
        return self._backend('item_get_elements', 'var', name, filters)

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
        return self._backend('item_get_elements', 'equ', name, filters)

    def clone(self, model=None, scenario=None, annotation=None,
              keep_solution=True, shift_first_model_year=None, platform=None,
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
            warn('`keep_sol` is deprecated and will be removed in the next'
                 ' release, please use `keep_solution`')
            keep_solution = kwargs.pop('keep_sol')

        if 'scen' in kwargs:
            warn('`scen` is deprecated and will be removed in the next'
                 ' release, please use `scenario`')
            scenario = kwargs.pop('scen')

        if 'first_model_year' in kwargs:
            warn('`first_model_year` is deprecated and will be removed in the'
                 ' next release. Use `shift_first_model_year`.')
            shift_first_model_year = kwargs.pop('first_model_year')

        if len(kwargs):
            raise ValueError('Invalid arguments {!r}'.format(kwargs))

        if shift_first_model_year is not None:
            if keep_solution:
                logger().warning('Overriding keep_solution=True for '
                                 'shift_first_model_year')
                keep_solution = False

        platform = platform or self.platform
        model = model or self.model
        scenario = scenario or self.scenario

        args = [platform, model, scenario, annotation, keep_solution]
        if check_year(shift_first_model_year, 'first_model_year'):
            args.append(shift_first_model_year)

        return self._backend('clone', *args)

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
            check_year(first_model_year, 'first_model_year')
            self._backend('clear_solution', first_model_year)
        else:
            raise ValueError('This Scenario does not have a solution!')

    def solve(self, model=None, callback=None, cb_kwargs={}, **model_options):
        """Solve the model and store output.

        ixmp 'solves' a model by invoking the run() method of a :class:`.Model`
        subclass—for instance, :meth:`.GAMSModel.run`. Depending on the
        underlying model code, different steps are taken; see each model class
        for details. In general:

        1. Data from the Scenario are written to a **model input file**.
        2. Code or an external program is invoked to perform calculations or
           optimizations, **solving the model**.
        3. Data representing the model outputs or solution are read from a
           **model output file** and stored in the Scenario.

        If the optional argument `callback` is given, then additional steps are
        performed:

        4. Execute the `callback` with the Scenario as an argument. The
           Scenario has an `iteration` attribute that stores the number of
           times the underlying model has been solved (#2).
        5. If the `callback` returns :obj:`False` or similar, iterate by
           repeating from step #1. Otherwise, exit.

        Parameters
        ----------
        model : str
            model (e.g., MESSAGE) or GAMS file name (excluding '.gms')
        callback : callable, optional
            Method to execute arbitrary non-model code. Must accept a single
            argument: the Scenario. Must return a non-:obj:`False` value to
            indicate convergence.
        cb_kwargs : dict, optional
            Keyword arguments to pass to `callback`.
        model_options :
            Keyword arguments specific to the `model`. See :class:`.GAMSModel`.

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

        # Instantiate a model
        model = get_model(model, **model_options)

        # Validate *callback* argument
        if callback is not None and not callable(callback):
            raise ValueError('callback={!r} is not callable'.format(callback))
        elif callback is None:
            # Make the callback a no-op
            def callback(scenario, **kwargs):
                return True

        # Flag to warn if the *callback* appears not to return anything
        warn_none = True

        # Iterate until convergence
        while True:
            model.run(self)

            # Store an iteration number to help the callback
            if not hasattr(self, 'iteration'):
                self.iteration = 0

            self.iteration += 1

            # Invoke the callback
            cb_result = callback(self, **cb_kwargs)

            if cb_result is None and warn_none:
                warn('solve(callback=...) argument returned None; will loop '
                     'indefinitely unless True is returned.')
                # Don't repeat the warning
                warn_none = False

            if cb_result:
                # Callback indicates convergence is reached
                break

    def get_meta(self, name=None):
        """get scenario metadata

        Parameters
        ----------
        name : str, optional
            metadata attribute name
        """
        all_meta = self._backend('get_meta')
        return all_meta[name] if name else all_meta

    def set_meta(self, name, value):
        """set scenario metadata

        Parameters
        ----------
        name : str
            metadata attribute name
        value : str or number or bool
            metadata attribute value
        """
        self._backend('set_meta', name, value)


# %% auxiliary functions for class Scenario

def to_iamc_template(df):
    """Format pd.DataFrame *df* in IAMC style.

    Parameters
    ----------
    df : pandas.DataFrame
        May have a 'node' column, which will be renamed to 'region'.

    Returns
    -------
    pandas.DataFrame
        The returned object has:

        - Any (Multi)Index levels reset as columns.
        - Lower-case column names 'region', 'variable', and 'unit'.

    Raises
    ------
    ValueError
        If 'time' is among the column names; or 'region', 'variable', or 'unit'
        is not.
    """
    if 'time' in df.columns:
        raise ValueError('sub-annual time slices not supported by '
                         'ixmp.TimeSeries')

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
