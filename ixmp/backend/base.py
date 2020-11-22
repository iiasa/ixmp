import json
from abc import ABC, abstractmethod
from copy import copy
from typing import Dict, Generator

from ixmp.core import Scenario, TimeSeries

from . import ItemType
from .io import s_read_excel, s_write_excel, ts_read_file


class Backend(ABC):
    """Abstract base class for backends."""

    def __init__(self):
        """OPTIONAL: Initialize the backend."""

    def __call__(self, obj, method, *args, **kwargs):
        """Call the backend method *method* for *obj*.

        The class attribute obj._backend_prefix is used to determine a prefix
        for the method name, e.g. 'ts_{method}'.
        """
        return getattr(self, method)(obj, *args, **kwargs)

    # Platform methods

    def set_log_level(self, level):
        """OPTIONAL: Set logging level for the backend and other code.

        The default implementation has no effect.

        Parameters
        ----------
        level : int or Python logging level

        See also
        --------
        get_log_level
        """

    def get_log_level(self):
        """OPTIONAL: Get logging level for the backend and other code.

        The default implementation has no effect.

        Returns
        -------
        str
            Name of a :py:ref:`Python logging level <levels>`.

        See also
        --------
        set_log_level
        """

    @abstractmethod
    def set_doc(self, domain, docs):
        """Save documentation to database

        Parameters
        ----------
        domain : str
            Documentation domain, e.g. model, scenario etc
        docs : dict or array of tuples
            Dictionary or tuple array containing mapping between name of domain
            object (e.g. model name) and string representing fragment
            of documentation
        """

    @abstractmethod
    def get_doc(self, domain, name=None):
        """Read documentation from database

        Parameters
        ----------
        domain : str
            Documentation domain, e.g. model, scenario etc
        name : str, optional
            Name of domain entity (e.g. model name).

        Returns
        -------
        str or dict
            String representing fragment of documentation if name is passed as
            parameter or dictionary containing mapping between name of domain
            object (e.g. model name) and string representing fragment when
            name parameter is omitted.
        """

    def open_db(self):
        """OPTIONAL: (Re-)open database connection(s).

        A backend **may** connect to a database server. This method opens the
        database connection if it is closed.
        """

    def close_db(self):
        """OPTIONAL: Close database connection(s).

        Close any database connection(s), if open.
        """

    def get_auth(self, user, models, kind):
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
    def get_timeslices(self):
        """Iterate over subannual timeslices defined on the Platform instance.

        Yields
        -------
        tuple
            The members of each tuple are:

            ========= =========== ===
            ID        Type        Description
            ========= =========== ===
            name      str         Time slice name
            category  str         Time slice category
            duration  float       Time slice duration (fraction of year)
            ========= =========== ===

        See also
        --------
        set_timeslice
        """

    @abstractmethod
    def set_timeslice(self, name, category, duration):
        """Add a subannual time slice to the Platform.

        Parameters
        ----------
        name : str
           Node name.
        category : str
           Time slice category.
        duration : float
           Time slice duration (a fraction of a year).

        See also
        --------
        get_timeslices
        """

    @abstractmethod
    def add_model_name(self, name: str):
        """Add (register) new model name.

        Parameters
        ----------
        name : str
            New model name
        """

    @abstractmethod
    def add_scenario_name(self, name: str):
        """Add (register) new scenario name.

        Parameters
        ----------
        name : str
            New scenario name
        """

    @abstractmethod
    def get_model_names(self) -> Generator[str, None, None]:
        """List existing model names.

        Returns
        -------
        list of str
            List of the retrieved model names.
        """

    @abstractmethod
    def get_scenario_names(self) -> Generator[str, None, None]:
        """List existing scenario names.

        Returns
        -------
        list of str
            List of the retrieved scenario names.
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

    def read_file(self, path, item_type: ItemType, **kwargs):
        """OPTIONAL: Read Platform, TimeSeries, or Scenario data from file.

        A backend **may** implement read_file for one or more combinations of
        the `path` and `item_type` methods. For all other combinations, it
        **must** raise :class:`NotImplementedError`.

        The default implementation supports:

        - `path` ending in '.xlsx', `item_type` is ItemType.MODEL: read a
          single Scenario given by kwargs['filters']['scenario'] from file
          using :meth:`pandas.DataFrame.read_excel`.

        Parameters
        ----------
        path : os.PathLike
            File for input. The filename suffix determines the input format:

            ====== ===
            Suffix Format
            ====== ===
            .csv   Comma-separated values
            .gdx   GAMS data exchange
            .xlsx  Microsoft Office Open XML spreadsheet
            ====== ===

        item_type : ItemType
            Type(s) of items to read.

        Raises
        ------
        ValueError
            If `ts` is not None and 'scenario' is a key in `filters`.
        NotImplementedError
            If input of the specified items from the file format is not
            supported.

        See also
        --------
        write_file
        """
        s, filters = self._handle_rw_filters(kwargs.pop("filters", {}))
        if path.suffix in (".csv", ".xlsx") and item_type is ItemType.TS and s:
            ts_read_file(s, path, **kwargs)
        elif path.suffix == ".xlsx" and item_type is ItemType.MODEL and s:
            s_read_excel(self, s, path, **kwargs)
        else:
            raise NotImplementedError

    def write_file(self, path, item_type: ItemType, **kwargs):
        """OPTIONAL: Write Platform, TimeSeries, or Scenario data to file.

        A backend **may** implement write_file for one or more combinations of
        the `path` and `item_type` methods. For all other combinations, it
        **must** raise :class:`NotImplementedError`.

        The default implementation supports:

        - `path` ending in '.xlsx', `item_type` is either :attr:`.MODEL` or
          :attr:`.SET` | :attr:`.PAR`: write a single Scenario given by
          kwargs['filters']['scenario'] to file using
          :meth:`pandas.DataFrame.to_excel`.

        Parameters
        ----------
        path : os.PathLike
            File for output. The filename suffix determines the output format.
        item_type : ItemType
            Type(s) of items to write.

        Raises
        ------
        ValueError
            If `ts` is not None and 'scenario' is a key in `filters`.
        NotImplementedError
            If output of the specified items to the file format is not
            supported.

        See also
        --------
        read_file
        """
        # Use the "scenario" filter to retrieve the Scenario `s` to be written;
        # reappend any other filters
        s, kwargs["filters"] = self._handle_rw_filters(kwargs.pop("filters", {}))

        xlsx_types = (ItemType.SET | ItemType.PAR, ItemType.MODEL)
        if path.suffix == ".xlsx" and item_type in xlsx_types and s:
            s_write_excel(self, s, path, item_type, **kwargs)
        else:
            # All other combinations of arguments
            raise NotImplementedError

    # Methods for ixmp.TimeSeries

    @abstractmethod
    def init(self, ts: TimeSeries, annotation):
        """Create a new TimeSeries (or Scenario) *ts*.

        init **may** modify the :attr:`~TimeSeries.version` attribute of
        *ts*.

        If *ts* is a :class:`Scenario`; the Backend **must** store the
        :attr:`.Scenario.scheme` attribute.

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
    def get(self, ts: TimeSeries):
        """Retrieve the existing TimeSeries (or Scenario) *ts*.

        The TimeSeries is identified based on the unique combination of the
        attributes of *ts*:

        - :attr:`~.TimeSeries.model`,
        - :attr:`~.TimeSeries.scenario`, and
        - :attr:`~.TimeSeries.version`.

        If :attr:`.version` is :obj:`None`, the Backend **must** return the
        version marked as default, and **must** set the attribute value.

        If *ts* is a Scenario, :meth:`get` **must** set the
        :attr:`~.Scenario.scheme` attribute with the value previously passed to
        :meth:`init`.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If :attr:`~.TimeSeries.model` or :attr:`~.TimeSeries.scenario` does
            not exist on the Platform.

        See also
        --------
        is_default
        set_as_default
        """

    def del_ts(self, ts: TimeSeries):
        """OPTIONAL: Free memory associated with the TimeSeries *ts*.

        The default implementation has no effect.
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
        get
        is_default
        """

    @abstractmethod
    def is_default(self, ts: TimeSeries):
        """Return :obj:`True` if *ts* is the default version.

        Returns
        -------
        bool

        See also
        --------
        get
        set_as_default
        """

    @abstractmethod
    def last_update(self, ts: TimeSeries):
        """Return the date of the last modification of the *ts*.

        Returns
        -------
        str or None
        """

    @abstractmethod
    def run_id(self, ts: TimeSeries):
        """Return the run ID for the *ts*.

        Returns
        -------
        int
        """

    def preload(self, ts: TimeSeries):
        """OPTIONAL: Load *ts* data into memory."""

    @staticmethod
    def _handle_rw_filters(filters: dict):
        """Helper for :meth:`read_file` and :meth:`write_file`.

        The `filters` argument is unpacked if the 'scenarios' key is a single
        :class:`TimeSeries` object. A 2-tuple is returned of the object (or
        :obj:`None`) and the remaining filters.
        """
        ts = None
        filters = copy(filters)
        try:
            if isinstance(filters["scenario"], TimeSeries):
                ts = filters.pop("scenario")
        except KeyError:
            pass  # Don't modify filters at all

        return ts, filters

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

            ========= ==== ===
            ID        Type Description
            ========= ==== ===
            region    str  Region name
            variable  str  Variable name
            year      int  Year
            value     str  Value
            unit      str  Unit symbol
            subannual str  Name of time slice
            meta      bool :obj:`True` if the data is marked as metadata
            ========= ==== ===
        """

    @abstractmethod
    def set_data(self, ts: TimeSeries, region, variable, data, unit, subannual, meta):
        """Store *data*.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        subannual : str
            Name of time slice.
        unit : str
            Unit symbol.
        data : dict (int -> float)
            Mapping from year to value.
        meta : bool
            :obj:`True` to mark *data* as metadata.
        """

    @abstractmethod
    def set_geo(
        self, ts: TimeSeries, region, variable, subannual, year, value, unit, meta
    ):
        """Store time-series 'geodata'.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        subannual : str
            Name of time slice.
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
    def delete(self, ts: TimeSeries, region, variable, subannual, years, unit):
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
        subannual : str
            Name of time slice.

        Returns
        -------
        None
        """

    @abstractmethod
    def delete_geo(self, ts: TimeSeries, region, variable, subannual, years, unit):
        """Remove 'geodata' values.

        Parameters
        ----------
        region : str
            Region name.
        variable : str
            Variable name.
        subannual : str
            Name of time slice.
        years : Iterable of int
            Years.
        unit : str
            Unit symbol.

        Returns
        -------
        None
        """

    # Methods for ixmp.Scenario

    @abstractmethod
    def clone(
        self,
        s: Scenario,
        platform_dest,
        model,
        scenario,
        annotation,
        keep_solution,
        first_model_year=None,
    ):
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
    def init_item(self, s: Scenario, type, name, idx_sets, idx_names):
        """Initialize an item *name* of *type*.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ' or 'var'
        name : str
            Name for the new item.
        idx_sets : sequence of str
            If empty, a 0-dimensional/scalar item is initialized. Otherwise, a
            1+-dimensional item is initialized.
        idx_names : sequence of str or None
            Optional names for the dimensions. If not supplied, the names of
            the *idx_sets* (if any) are used. If supplied, *idx_names* and
            *idx_sets* must be the same length.

        Return
        ------
        None

        Raises
        ------
        ValueError
            if any of the *idx_sets* is not an existing set in the Scenario;
            if *idx_names* and *idx_sets* are not the same length.
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
            :class:`str` **must** be handled as equivalent to their string
            representation; i.e. item_get_elements must return the same data
            for `filters={'foo': [42]}` and `filters={'foo': ['42']}`.

        Returns
        -------
        pandas.Series
            When *type* is 'set' and *name* an index set (not indexed by other
            sets).
        dict
            When *type* is 'equ', 'par', or 'var' and *name* is scalar (zero-
            dimensional). The value has the keys 'value' and 'unit' (for 'par')
            or 'lvl' and 'mrg' (for 'equ' or 'var').
        pandas.DataFrame
            For mapping sets, or all 1+-dimensional values. The dataframe has
            one column per index name with dimension values; plus the columns
            'value' and 'unit' (for 'par') or 'lvl' and 'mrg' (for 'equ' or
            'var').

        Raises
        ------
        KeyError
            If *name* does not exist in *s*.
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
    def get_meta(self, model: str, scenario: str, version: int, strict: bool) -> dict:
        """Retrieve meta indicators.

        Parameters
        ----------
        model : str, optional
            filter meta by a model
        scenario : str, optional
            filter meta by a scenario
        version : int or str, optional
            retrieve meta of a specific model/scenario run version
        strict : bool, optional
            only retrieve indicators from the requested model-scenario-version
            level

        Returns
        -------
        dict (str -> any)
            Mapping from meta category keys to values.

        Raises
        ------
        ValueError
            On unsupported model-scenario-version combinations.
            Supported combinations are: (model), (scenario), (model, scenario),
            (model, scenario, version)
        """

    @abstractmethod
    def set_meta(self, meta: dict, model: str, scenario: str, version: int):
        """Set meta categories.

        Parameters
        ----------
        meta : dict
            containing meta key/value category pairs
        model : str, optional
            model name that meta should be attached to
        scenario : str, optional
            scenario name that meta should be attached to
        version : int, optional
            run version that meta should be attached to

        Returns
        -------
        None

        Raises
        ------
        ValueError
            On unsupported model-scenario-version combinations.
            Supported combinations are: (model), (scenario), (model, scenario),
            (model, scenario, version)
        """

    @abstractmethod
    def remove_meta(self, categories: list, model: str, scenario: str, version: int):
        """Remove meta categories.

        Parameters
        ----------
        categories : list of str
            meta-category keys to remove
        model : str, optional
            only remove meta of a specific model
        scenario : str, optional
            only remove meta of a specific scenario
        version : int, optional
            only remove meta of a specific model/scenario run version

        Returns
        -------
        None

        Raises
        ------
        ValueError
            On unsupported model-scenario-version combinations.
            Supported combinations are: (model), (scenario), (model, scenario),
            (model, scenario, version)
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

    #: :obj:`True` if caching is enabled.
    cache_enabled = True

    #: Cache of values. Keys are given by :meth:`_cache_key`; values depend on
    #: the subclass' usage of the cache.
    _cache: Dict[str, object] = {}

    #: Count of number of times a value was retrieved from cache successfully
    #: using :meth:`cache_get`.
    _cache_hit: Dict[str, int] = {}

    # Backend API methods

    def __init__(self, cache_enabled=True):
        super().__init__()

        self.cache_enabled = cache_enabled

        # Empty the cache
        self._cache = {}
        self._cache_hit = {}

    def del_ts(self, ts: TimeSeries):
        """Invalidate cache entries associated with *ts*."""
        self.cache_invalidate(ts)

    # New methods for CachingBackend

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

        if self.cache_enabled and key in self._cache:
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
        if not self.cache_enabled:
            # Don't store anything if cache is disabled
            return False

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
