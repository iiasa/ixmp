from functools import lru_cache
from abc import ABC, abstractmethod


#: Lists of field names for tuples returned by Backend API methods.
FIELDS = {
    'get_nodes': ('region', 'mapped_to', 'parent', 'hierarchy'),
    'get_scenarios': ('model', 'scenario', 'scheme', 'is_default',
                      'is_locked', 'cre_user', 'cre_date', 'upd_user',
                      'upd_date', 'lock_user', 'lock_date', 'annotation',
                      'version'),
    'ts_get': ('region', 'variable', 'unit', 'year', 'value'),
    'ts_get_geo': ('region', 'variable', 'time', 'year', 'value', 'unit',
                   'meta'),
}


class Backend(ABC):
    """Abstract base class for backends."""
    # NB non-abstract methods like close_db() are marked '# pragma: no cover'.
    #    In order to cover these with tests, define a MemoryBackend or similar
    #    that provides implementations of all the abstract methods but does
    #    NOT override the non-abstract methods; then call those.

    def __init__(self):  # pragma: no cover
        """OPTIONAL: Initialize the backend."""
        pass

    @classmethod
    @lru_cache()  # Don't recompute
    def __method(backend_cls, cls, name):
        for c in cls.__mro__[:-1]:
            try:
                return getattr(backend_cls, f'{c._backend_prefix}_{name}')
            except AttributeError:
                pass
        raise AttributeError(f"backend method '{{prefix}}_{name}'")

    def __call__(self, obj, method, *args, **kwargs):
        """Call the backend method *method* for *obj*.

        The class attribute obj._backend_prefix is used to determine a prefix
        for the method name, e.g. 'ts_{method}'.
        """
        method = self.__method(obj.__class__, method)
        return method(self, obj, *args, **kwargs)

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
    def ts_init(self, ts, annotation=None):
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
    def ts_get(self, ts, version):
        """Retrieve the existing TimeSeries *ts*.

        The TimeSeries is identified based on its (:attr:`~.TimeSeries.model`,
        :attr:`~.TimeSeries.scenario`) and *version*.

        Parameters
        ----------
        version : str or None
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
    def ts_check_out(self, ts, timeseries_only):
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
    def ts_commit(self, ts, comment):
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
    def ts_get_data(self, ts, region, variable, unit, year):
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
    def ts_get_geo(self, ts):
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
    def ts_set_data(self, ts, region, variable, data, unit, meta):
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
    def ts_set_geo(self, ts, region, variable, time, year, value, unit, meta):
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
    def ts_delete(self, ts, region, variable, years, unit):
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
    def ts_delete_geo(self, ts, region, variable, time, years, unit):
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
    def ts_discard_changes(self, ts):
        """Discard changes to *ts* since the last :meth:`ts_check_out`.

        Returns
        -------
        None
        """

    @abstractmethod
    def ts_set_as_default(self, ts):
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
    def ts_is_default(self, ts):
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
    def ts_last_update(self, ts):
        """Return the date of the last modification of the *ts*.

        Returns
        -------
        str
        """

    @abstractmethod
    def ts_run_id(self, ts):
        """Return the run ID for the *ts*.

        Returns
        -------
        int
        """

    def ts_preload(self, ts):  # pragma: no cover
        """OPTIONAL: Load *ts* data into memory."""
        pass

    # Methods for ixmp.Scenario

    @abstractmethod
    def s_init(self, s, scheme, annotation):
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
    def s_get(self, s, version):
        """Retrieve the existing Scenario *s*.

        The Scenario is identified based on its (:attr:`~.TimeSeries.model`,
        :attr:`~.TimeSeries.scenario`) and *version*. s_get **must** set
        the :attr:`.Scenario.scheme` attribute on *s*.

        Parameters
        ----------
        version : str or None
            If :obj:`None`, the version marked as the default is returned, and
            s_get **must** set :attr:`.TimeSeries.version` attribute on *s*.

        Returns
        -------
        None

        See also
        --------
        ts_set_as_default
        """

    @abstractmethod
    def s_clone(self, s, platform_dest, model, scenario, annotation,
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
    def s_has_solution(self, s):
        """Return `True` if Scenario *s* has been solved.

        If :obj:`True`, model solution data is available from the Backend.
        """

    @abstractmethod
    def s_list_items(self, s, type):
        """Return a list of items of *type*.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ'

        Return
        ------
        list of str
        """

    @abstractmethod
    def s_init_item(self, s, type, name):
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
    def s_delete_item(self, s, type, name):
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
    def s_item_index(self, s, name, sets_or_names):
        """Return the index sets or names of item *name*.

        Parameters
        ----------
        sets_or_names : 'sets' or 'names'

        Returns
        -------
        list of str
        """

    @abstractmethod
    def s_item_get_elements(self, s, type, name, filters=None):
        """Return elements of item *name*.

        Parameters
        ----------
        type : 'equ' or 'par' or 'set' or 'var'
        name : str
            Name of the item.
        filters : dict (str -> list of str), optional
            If provided, a mapping from dimension names to allowed values
            along that dimension.

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
    def s_item_set_elements(self, s, type, name, elements):
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
    def s_item_delete_elements(self, s, type, name, keys):
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
    def s_get_meta(self, s):
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
    def s_set_meta(self, s, name, value):
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
    def s_clear_solution(self, s, from_year=None):
        """Remove data associated with a model solution.

        .. todo:: Document.
        """

    # Methods for message_ix.Scenario

    @abstractmethod
    def ms_cat_list(self, ms, name):
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
    def ms_cat_get_elements(self, ms, name, cat):
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
    def ms_cat_set_elements(self, ms, name, cat, keys, is_unique):
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
