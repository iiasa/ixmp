from abc import ABC, abstractmethod
from copy import copy
import json

from ixmp.core import TimeSeries, Scenario


class Backend(ABC):
    """Abstract base class for backends."""
    # NB non-abstract methods like close_db() are marked '# pragma: no cover'.
    #    In order to cover these with tests, define a MemoryBackend or similar
    #    that provides implementations of all the abstract methods but does
    #    NOT override the non-abstract methods; then call those.

    def __init__(self):  # pragma: no cover
        """OPTIONAL: Initialize the backend."""
        pass

    def __call__(self, obj, method, *args, **kwargs):
        """Call the backend method *method* for *obj*.

        The class attribute obj._backend_prefix is used to determine a prefix
        for the method name, e.g. 'ts_{method}'.
        """
        return getattr(self, method)(obj, *args, **kwargs)

    def close_db(self):  # pragma: no cover
        """OPTIONAL: Close database connection(s).

        Close any database connection(s), if open.
        """
        pass

    def get_auth(self, user, models, kind):  # pragma: no cover
        """OPTIONAL: Return user authorization for *models*.

        If the Backend implements access control, this method **must** indicate
        whether *user* has permission *kind* for each of *models*.

        *kind* **may** be 'read'/'view', 'write'/'modify', or other values;
        :meth:`get_auth` **should** raise exceptions on invalid values.

        Parameters
        ----------
        user : str
            User name or identifier.
        models : list of str
            Model names.
        kind : str
            Type of permission being requested

        Returns
        -------
        dict
            Mapping of *model name (str)* → :class:`bool`; :obj:`True` if the
            user is authorized for the model.
        """
        return {model: True for model in models}

    @abstractmethod
    def get_nodes(self):
        """Iterate over all nodes stored on the Platform.

        Yields
        -------
        tuple
            The members of each tuple are:

            ========= =========== ===
            ID        Type        Description
            ========= =========== ===
            region    str         Node name or synonym for node
            mapped_to str or None Node name
            parent    str         Parent node name
            hierarchy str         Node hierarchy ID
            ========= =========== ===

        See also
        --------
        set_node
        """

    @abstractmethod
    def get_scenarios(self, default, model, scenario):
        """Iterate over TimeSeries stored on the Platform.

        Scenarios, as subclasses of TimeSeries, are also included.

        Parameters
        ----------
        default : bool
           :obj:`True` to include only TimeSeries versions marked as default.
        model : str or None
           Model name to filter results.
        scenario : str or None
           Scenario name to filter results.

        Yields
        ------
        tuple
            The members of each tuple are:

            ========== ==== ===
            ID         Type Description
            ========== ==== ===
            model      str  Model name
            scenario   str  Scenario name
            scheme     str  Scheme name
            is_default bool :obj:`True` if `version` is the default
            is_locked  bool :obj:`True` if read-only
            cre_user   str  Name of user who created the TimeSeries
            cre_date   str  Creation datetime
            upd_user   str  Name of user who last modified the TimeSeries
            upd_date   str  Modification datetime
            lock_user  str  Name of user who locked the TimeSeries
            lock_date  str  Lock datetime
            annotation str  Description of the TimeSeries
            version    int  Version
            ========== ==== ===
        """

    @abstractmethod
    def get_units(self):
        """Return all registered symbols for units of measurement.

        Returns
        -------
        list of str

        See also
        --------
        set_unit
        """

    def open_db(self):  # pragma: no cover
        """OPTIONAL: (Re-)open database connection(s).

        A backend **may** connect to a database server. This method opens the
        database connection if it is closed.
        """
        pass

    def set_log_level(self, level):  # pragma: no cover
        """OPTIONAL: Set logging level for the backend and other code.

        Parameters
        ----------
        level : int or Python logging level
        """
        pass

    @abstractmethod
    def set_node(self, name, parent=None, hierarchy=None, synonym=None):
        """Add a node name to the Platform.

        This method **must** have one of two effects, depending on the
        arguments:

        - With `parent` and `hierarchy`: `name` is added as a child of `parent`
          in the named `hierarchy`.
        - With `synonym`: `synonym` is added as an alias for `name`.

        Parameters
        ----------
        name : str
           Node name.
        parent : str, optional
           Parent node name.
        hierarchy : str, optional
           Node hierarchy ID.
        synonym : str, optional
           Synonym for node.

        See also
        --------
        get_nodes
        """

    @abstractmethod
    def set_unit(self, name, comment):
        """Add a unit of measurement to the Platform.

        Parameters
        ----------
        name : str
            Symbol of the unit.
        comment : str
            Description of the change or of the unit.

        See also
        --------
        get_units
        """

    # Methods for ixmp.TimeSeries

    @abstractmethod
    def init_ts(self, ts: TimeSeries, annotation=None):
        """Initialize the TimeSeries *ts*.

        ts_init **may** modify the :attr:`~TimeSeries.version` attribute of
        *ts*.

        Parameters
        ----------
        annotation : str
            If *ts* is newly-created, the Backend **must** store this
            annotation with the TimeSeries.

        Returns
        -------
        None
        """

    @abstractmethod
    def get(self, ts: TimeSeries, version):
        """Retrieve the existing TimeSeries or Scenario *ts*.

        The TimeSeries is identified based on its (:attr:`~.TimeSeries.model`,
        :attr:`~.TimeSeries.scenario`) and *version*.

        If *ts* is a Scenario, :meth:`get` **must** set the
        :attr:`~.Scenario.scheme` attribute on it.

        Parameters
        ----------
        version : int or None
            If :obj:`None`, the version marked as the default is returned, and
            ts_get **must** set :attr:`.TimeSeries.version` attribute on *ts*.

        Returns
        -------
        None

        See also
        --------
        ts_set_as_default
        """

    @abstractmethod
    def check_out(self, ts: TimeSeries, timeseries_only):
        """Check out *ts* for modification.

        Parameters
        ----------
        timeseries_only : bool
            ???

        Returns
        -------
        None
        """

    @abstractmethod
    def commit(self, ts: TimeSeries, comment):
        """Commit changes to *ts*.

        ts_init **may** modify the :attr:`~.TimeSeries.version` attribute of
        *ts*.

        Parameters
        ----------
        comment : str
            Description of the changes being committed.

        Returns
        -------
        None
        """

    @abstractmethod
    def get_data(self, ts: TimeSeries, region, variable, unit, year):
        """Retrieve time-series data.

        Parameters
        ----------
        region : list of str
            Region names to filter results.
        variable : list of str
            Variable names to filter results.
        unit : list of str
            Unit symbols to filter results.
        year : list of str
            Years to filter results.

        Yields
        ------
        tuple
            The members of each tuple are:

            ======== ===== ===
            ID       Type  Description
            ======== ===== ===
            region   str   Region name
            variable str   Variable name
            unit     str   Unit symbol
            year     int   Year
            value    float Data value
            ======== ===== ===
        """

    @abstractmethod
    def get_geo(self, ts: TimeSeries):
        """Retrieve time-series 'geodata'.

        Yields
        ------
        tuple
            The members of each tuple are:

            ======== ==== ===
            ID       Type Description
            ======== ==== ===
            region   str  Region name
            variable str  Variable name
            time     str  Time period
            year     int  Year
            value    str  Value
            unit     str  Unit symbol
            meta     bool :obj:`True` if the data is marked as metadata
            ======== ==== ===
        """

    @abstractmethod
    def set_data(self, ts: TimeSeries, region, variable, data, unit, meta):
        """Store *data*.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        time : str
            Time period.
        unit : str
            Unit symbol.
        data : dict (int -> float)
            Mapping from year to value.
        meta : bool
            :obj:`True` to mark *data* as metadata.
        """

    @abstractmethod
    def set_geo(self, ts: TimeSeries, region, variable, time, year, value,
                unit, meta):
        """Store time-series 'geodata'.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        time : str
            Time period.
        year : int
            Year.
        value : str
            Data value.
        unit : str
            Unit symbol.
        meta : bool
            :obj:`True` to mark *data* as metadata.
        """

    @abstractmethod
    def delete(self, ts: TimeSeries, region, variable, years, unit):
        """Remove data values.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        years : Iterable of int
            Years.
        unit : str
            Unit symbol.

        Returns
        -------
        None
        """

    @abstractmethod
    def delete_geo(self, ts: TimeSeries, region, variable, time, years, unit):
        """Remove 'geodata' values.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        time : str
            Time period.
        years : Iterable of int
            Years.
        unit : str
            Unit symbol.

        Returns
        -------
        None
        """

    @abstractmethod
    def discard_changes(self, ts: TimeSeries):
        """Discard changes to *ts* since the last :meth:`ts_check_out`.

        Returns
        -------
        None
        """

    @abstractmethod
    def set_as_default(self, ts: TimeSeries):
        """Set the current :attr:`.TimeSeries.version` as the default.

        Returns
        -------
        None

        See also
        --------
        ts_is_default
        ts_get
        s_get
        """

    @abstractmethod
    def is_default(self, ts: TimeSeries):
        """Return :obj:`True` if *ts* is the default version.

        Returns
        -------
        bool

        See also
        --------
        ts_set_as_default
        ts_get
        s_get
        """

    @abstractmethod
    def last_update(self, ts: TimeSeries):
        """Return the date of the last modification of the *ts*.

        Returns
        -------
        str
        """

    @abstractmethod
    def run_id(self, ts: TimeSeries):
        """Return the run ID for the *ts*.

        Returns
        -------
        int
        """

    def preload(self, ts: TimeSeries):  # pragma: no cover
        """OPTIONAL: Load *ts* data into memory."""
        pass

    # Methods for ixmp.Scenario

    @abstractmethod
    def init_s(self, s: Scenario, scheme, annotation):
        """Initialize the Scenario *s*.

        s_init **may** modify the :attr:`~.TimeSeries.version` attribute of
        *s*.

        Parameters
        ----------
        scheme : str
            Description of the scheme of the Scenario.
        annotation : str
            Description of the Scenario.

        Returns
        -------
        None
        """

    @abstractmethod
    def clone(self, s: Scenario, platform_dest, model, scenario, annotation,
              keep_solution, first_model_year=None):
        """Clone *s*.

        Parameters
        ----------
        platform_dest : :class:`.Platform`
            Target backend. May be the same as *s.platform*.
        model : str
            New model name.
        scenario : str
            New scenario name.
        annotation : str
            Description for the creation of the new scenario.
        keep_solution : bool
            If :obj:`True`, model solution data is also cloned. If
            :obj:`False`, it is discarded.
        first_model_year : int or None
            If :class:`int`, must be greater than the first model year of *s*.

        Returns
        -------
        Same class as *s*
            The cloned Scenario.
        """

    @abstractmethod
    def has_solution(self, s: Scenario):
        """Return `True` if Scenario *s* has been solved.

        If :obj:`True`, model solution data is available from the Backend.
        """

    @abstractmethod
    def list_items(self, s: Scenario, type):
        """Return a list of items of *type*.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ'

        Return
        ------
        list of str
        """

    @abstractmethod
    def init_item(self, s: Scenario, type, name):
        """Initialize an item *name* of *type*.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ'
        name : str
            Name for the new item.

        Return
        ------
        None
        """

    @abstractmethod
    def delete_item(self, s: Scenario, type, name):
        """Remove an item *name* of *type*.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ'
        name : str
            Name of the item to delete.

        Return
        ------
        None
        """

    @abstractmethod
    def item_index(self, s: Scenario, name, sets_or_names):
        """Return the index sets or names of item *name*.

        Parameters
        ----------
        sets_or_names : 'sets' or 'names'

        Returns
        -------
        list of str
        """

    @abstractmethod
    def item_get_elements(self, s: Scenario, type, name, filters=None):
        """Return elements of item *name*.

        Parameters
        ----------
        type : 'equ' or 'par' or 'set' or 'var'
        name : str
            Name of the item.
        filters : dict (str -> list), optional
            If provided, a mapping from dimension names to allowed values
            along that dimension.

            item_get_elements **must** silently accept values that are *not*
            members of the set indexing a dimension. Elements which are not
            :type:`str` **must** be handled as equivalent to their string
            representation; i.e. item_get_elements must return the same data
            for `filters={'foo': [42]}` and `filters={'foo': ['42']}`.

        Returns
        -------
        pandas.Series
            When *type* is 'set' and *name* an index set (not indexed by other
            sets).
        dict
            When *type* is 'equ', 'par', or 'set' and *name* is scalar (zero-
            dimensional). The value has the keys 'value' and 'unit' (for 'par')
            or 'lvl' and 'mrg' (for 'equ' or 'var').
        pandas.DataFrame
            For mapping sets, or all 1+-dimensional values. The dataframe has
            one column per index name with dimension values; plus the columns
            'value' and 'unit' (for 'par') or 'lvl' and 'mrg' (for 'equ' or
            'var').
        """

    @abstractmethod
    def item_set_elements(self, s: Scenario, type, name, elements):
        """Add keys or values to item *name*.

        Parameters
        ----------
        type : 'par' or 'set'
        name : str
            Name of the items.
        elements : iterable of 4-tuple
            The members of each tuple are:

            ======= ========================== ===
            ID      Type                       Description
            ======= ========================== ===
            key     str or list of str or None Set elements or value indices
            value   float or None              Parameter value
            unit    str or None                Unit symbol
            comment str or None                Description of the change
            ======= ========================== ===

            If *name* is indexed by other set(s), then the number of elements
            of each *key*, and their contents, must match the index set(s).
            When *type* is 'set', *value* and *unit* **must** be :obj:`None`.

        Raises
        ------
        ValueError
            If *elements* contain invalid values, e.g. key values not in the
            index set(s).
        Exception
            If the Backend encounters any error adding the elements.

        See also
        --------
        s_init_item
        s_item_delete_elements
        """

    @abstractmethod
    def item_delete_elements(self, s: Scenario, type, name, keys):
        """Remove elements of item *name*.

        Parameters
        ----------
        type : 'par' or 'set'
        name : str
        keys : iterable of iterable of str
            If *name* is indexed by other set(s), then the number of elements
            of each key in *keys*, and their contents, must match the index
            set(s).
            If *name* is a basic set, then each key must be a list containing a
            single str, which must exist in the set.

        See also
        --------
        s_init_item
        s_item_set_elements
        """

    @abstractmethod
    def get_meta(self, s: Scenario):
        """Return all metadata.

        Returns
        -------
        dict (str -> any)
            Mapping from metadata keys to values.

        See also
        --------
        s_get_meta
        """

    @abstractmethod
    def set_meta(self, s: Scenario, name, value):
        """Set a single metadata key.

        Parameters
        ----------
        name : str
            Metadata key name.
        value : any
            Value for *name*.

        Returns
        -------
        None
        """

    @abstractmethod
    def clear_solution(self, s: Scenario, from_year=None):
        """Remove data associated with a model solution.

        .. todo:: Document.
        """

    # Methods for message_ix.Scenario

    @abstractmethod
    def cat_list(self, ms: Scenario, name):
        """Return list of categories in mapping *name*.

        Parameters
        ----------
        name : str
            Name of the category mapping set.

        Returns
        -------
        list of str
            All categories in *name*.
        """

    @abstractmethod
    def cat_get_elements(self, ms: Scenario, name, cat):
        """Get elements of a category mapping.

        Parameters
        ----------
        name : str
            Name of the category mapping set.
        cat : str
            Name of the category within *name*.

        Returns
        -------
        list of str
            All set elements mapped to *cat* in *name*.
        """

    @abstractmethod
    def cat_set_elements(self, ms: Scenario, name, cat, keys, is_unique):
        """Add elements to category mapping.

        Parameters
        ----------
        name : str
            Name of the category mapping set.
        cat : str
            Name of the category within *name*.
        keys : iterable of str or list of str
            Keys to add to *cat*.
        is_unique : bool
            If :obj:`True`:

            - *keys* **must** contain only one key.
            - The Backend **must** remove any existing member of *cat*, so that
              it has only one element.

        Returns
        -------
        None
        """


class CachingBackend(Backend):
    """Backend with additional features for caching data."""

    #: Cache of values. Keys are given by :meth:`_cache_key`; values depend on
    #: the subclass' usage of the cache.
    _cache = {}

    #: Count of number of times a value was retrieved from cache successfully
    #: using :meth:`cache_get`.
    _cache_hit = {}

    def __init__(self):
        super().__init__()

        # Empty the cache
        self._cache = {}
        self._cache_hit = {}

    @classmethod
    def _cache_key(self, ts, ix_type, name, filters=None):
        """Return a hashable cache key.

        ixmp *filters* (a :class:`dict` of :class:`list`) are converted to a
        unique id that is hashable.

        Parameters
        ----------
        ts : .TimeSeries
        ix_type : str
        name : str
        filters : dict (str -> list of hashable)

        Returns
        -------
        tuple
            A hashable key with 4 elements for *ts*, *ix_type*, *name*, and
            *filters*.
        """
        ts = id(ts)
        if filters is None or len(filters) == 0:
            return (ts, ix_type, name)
        else:
            # Convert filters into a hashable object
            filters = hash(json.dumps(sorted(filters.items())))
            return (ts, ix_type, name, filters)

    def cache_get(self, ts, ix_type, name, filters):
        """Retrieve value from cache.

        The value in :attr:`_cache` is copied to avoid cached values being
        modified by user code. :attr:`_cache_hit` is incremented.

        Raises
        ------
        KeyError
            If the key for *ts*, *ix_type*, *name* and *filters* is not in the
            cache.
        """
        key = self._cache_key(ts, ix_type, name, filters)

        if key in self._cache:
            self._cache_hit[key] = self._cache_hit.setdefault(key, 0) + 1
            return copy(self._cache[key])
        else:
            raise KeyError(ts, ix_type, name, filters)

    def cache(self, ts, ix_type, name, filters, value):
        """Store *value* in cache.

        Returns
        -------
        bool
            :obj:`True` if the key was already in the cache and its value was
            overwritten.
        """
        key = self._cache_key(ts, ix_type, name, filters)

        refreshed = key in self._cache
        self._cache[key] = value

        return refreshed

    def cache_invalidate(self, ts, ix_type=None, name=None, filters=None):
        """Invalidate cached values.

        With all arguments given, single key/value is removed from the cache.
        Otherwise, multiple keys/values are removed:

        - *ts* only: all cached values associated with the :class:`.TimeSeries`
          or :class:`.Scenario` object.
        - *ts*, *ix_type*, and *name*: all cached values associated with the
          ixmp item, whether filtered or unfiltered.
        """
        key = self._cache_key(ts, ix_type, name, filters)

        if filters is None:
            i = slice(1) if (ix_type is name is None) else slice(3)
            to_remove = filter(lambda k: k[i] == key[i], self._cache.keys())
        else:
            to_remove = [key]

        for key in list(to_remove):
            self._cache.pop(key)
