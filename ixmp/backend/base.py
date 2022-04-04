import json
from abc import ABC, abstractmethod
from copy import copy
from os import PathLike
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Hashable,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import pandas as pd

from ixmp.backend import ItemType
from ixmp.backend.io import s_read_excel, s_write_excel, ts_read_file
from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries


class Backend(ABC):
    """Abstract base class for backends."""

    # Typing:
    # - All methods MUST be fully typed.
    # - Use more permissive types, e.g. Sequence[str], for inputs.
    # - Use precise types, e.g. List[str], for return values.
    # - Backend subclasses do not need to repeat the type annotations; these are implied
    #   by this parent class.
    #
    # Docstrings:
    # - The "Returns" section is OPTIONAL. Do not include it if the method returns None.
    #   Otherwise, include it when necessary to disambiguate if/when different types or
    #   values are returned.
    # - Use "OPTIONAL:" for methods that are not @abstractmethod.

    def __init__(self):
        """OPTIONAL: Initialize the backend."""

    def __call__(self, obj, method, *args, **kwargs):
        """Call the backend method `method` for `obj`.

        The class attribute obj._backend_prefix is used to determine a prefix for the
        method name, e.g. 'ts_{method}'.
        """
        return getattr(self, method)(obj, *args, **kwargs)

    # Platform methods

    @classmethod
    def handle_config(cls, args: Sequence, kwargs: MutableMapping) -> Dict[str, Any]:
        """OPTIONAL: Handle platform/backend config arguments.

        Returns a :class:`dict` to be stored in the configuration file. This
        :class:`dict` **must** be valid as keyword arguments to the :meth:`__init__` of
        a backend subclass.

        The default implementation expects both `args` and `kwargs` to be empty.

        See also
        --------
        Config.add_platform
        """
        msg = "Unhandled {} args to Backend.handle_config(): {!r}"
        if len(args):
            raise ValueError(msg.format("positional", args))
        if len(kwargs):
            raise ValueError(msg.format("keyword", kwargs))
        return dict()

    def set_log_level(self, level: int) -> None:
        """OPTIONAL: Set logging level for the backend and other code.

        The default implementation has no effect.

        Parameters
        ----------
        level : int or Python logging level

        See also
        --------
        get_log_level
        """

    def get_log_level(self) -> str:
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
    def set_doc(self, domain: str, docs) -> None:
        """Save documentation to database

        Parameters
        ----------
        domain : str
            Documentation domain, e.g. model, scenario, etc.
        docs : dict or array of tuples
            Dictionary or tuple array containing mapping between name of domain object
            (e.g. model name) and string representing fragment of documentation.
        """

    @abstractmethod
    def get_doc(self, domain: str, name: str = None) -> Union[str, Dict]:
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
            String representing fragment of documentation if name is passed as parameter
            or dictionary containing mapping between name of domain object (e.g. model
            name) and string representing fragment when name parameter is omitted.
        """

    def open_db(self) -> None:
        """OPTIONAL: (Re-)open database connection(s).

        A backend **may** connect to a database server. This method opens the database
        connection if it is closed.
        """

    def close_db(self) -> None:
        """OPTIONAL: Close database connection(s).

        Close any database connection(s), if open.
        """

    def get_auth(self, user: str, models: Sequence[str], kind: str) -> Dict[str, bool]:
        """OPTIONAL: Return user authorization for `models`.

        If the Backend implements access control, this method **must** indicate whether
        `user` has permission `kind` for each of `models`.

        `kind` **may** be 'read'/'view', 'write'/'modify', or other values;
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
            Mapping of `model name (str)` → :class:`bool`; :obj:`True` if the user is
            authorized for the model.
        """
        return {model: True for model in models}

    @abstractmethod
    def set_node(
        self, name: str, parent: str = None, hierarchy: str = None, synonym: str = None
    ) -> None:
        """Add a node name to the Platform.

        This method **must** have one of two effects, depending on the
        arguments:

        - With `parent` and `hierarchy`: `name` is added as a child of `parent` in the
          named `hierarchy`.
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
    def get_nodes(self) -> Iterable[Tuple[str, Optional[str], str, str]]:
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
    def get_timeslices(self) -> Iterable[Tuple[str, str, float]]:
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
    def set_timeslice(self, name: str, category: str, duration: float) -> None:
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
    def add_model_name(self, name: str) -> None:
        """Add (register) new model name.

        Parameters
        ----------
        name : str
            New model name
        """

    @abstractmethod
    def add_scenario_name(self, name: str) -> None:
        """Add (register) new scenario name.

        Parameters
        ----------
        name : str
            New scenario name
        """

    @abstractmethod
    def get_model_names(self) -> Iterable[str]:
        """List existing model names.

        Returns
        -------
        list of str
            List of the retrieved model names.
        """

    @abstractmethod
    def get_scenario_names(self) -> Iterable[str]:
        """List existing scenario names.

        Returns
        -------
        list of str
            List of the retrieved scenario names.
        """

    @abstractmethod
    def get_scenarios(
        self, default: bool, model: Optional[str], scenario: Optional[str]
    ) -> Iterable[
        Tuple[str, str, str, bool, bool, str, str, str, str, str, str, str, int]
    ]:
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
    def set_unit(self, name: str, comment: str) -> None:
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
    def get_units(self) -> List[str]:
        """Return all registered symbols for units of measurement.

        Returns
        -------
        list of str

        See also
        --------
        set_unit
        """

    def read_file(self, path: PathLike, item_type: ItemType, **kwargs) -> None:
        """OPTIONAL: Read Platform, TimeSeries, or Scenario data from file.

        A backend **may** implement read_file for one or more combinations of the `path`
        and `item_type` methods. For all other combinations, it **must** raise
        :class:`NotImplementedError`.

        The default implementation supports:

        - `path` ending in '.xlsx', `item_type` is ItemType.MODEL: read a single
          Scenario given by ``kwargs['filters']['scenario']`` from file, using
          :func:`.s_read_excel`.

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
            If input of the specified items from the file format is not supported.

        See also
        --------
        write_file
        """
        s, filters = self._handle_rw_filters(kwargs.pop("filters", {}))
        path = Path(path)
        if path.suffix in (".csv", ".xlsx") and item_type is ItemType.TS and s:
            ts_read_file(s, path, **kwargs)
        elif path.suffix == ".xlsx" and item_type is ItemType.MODEL and s:
            s_read_excel(self, s, path, **kwargs)
        else:
            raise NotImplementedError

    def write_file(self, path: PathLike, item_type: ItemType, **kwargs) -> None:
        """OPTIONAL: Write Platform, TimeSeries, or Scenario data to file.

        A backend **may** implement write_file for one or more combinations of the
        `path` and `item_type` methods. For all other combinations, it **must** raise
        :class:`NotImplementedError`.

        The default implementation supports:

        - `path` ending in '.xlsx', `item_type` is either :attr:`.MODEL` or :attr:`.SET`
          | :attr:`.PAR`: write a single Scenario given by
          ``kwargs['filters']['scenario']`` to file using :func:`.s_write_excel`.

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
            If output of the specified items to the file format is not supported.

        See also
        --------
        read_file
        """
        # Use the "scenario" filter to retrieve the Scenario `s` to be written; reappend
        # any other filters
        s, kwargs["filters"] = self._handle_rw_filters(kwargs.pop("filters", {}))

        xlsx_types = (ItemType.SET | ItemType.PAR, ItemType.MODEL)
        path = Path(path)
        if path.suffix == ".xlsx" and item_type in xlsx_types and s:
            s_write_excel(self, s, path, item_type, **kwargs)
        else:
            # All other combinations of arguments
            raise NotImplementedError

    # Methods for ixmp.TimeSeries

    @abstractmethod
    def init(self, ts: TimeSeries, annotation: str) -> None:
        """Create a new TimeSeries (or Scenario) `ts`.

        init **may** modify the :attr:`~TimeSeries.version` attribute of `ts`.

        If `ts` is a :class:`Scenario`; the Backend **must** store the
        :attr:`.Scenario.scheme` attribute.

        Parameters
        ----------
        annotation : str
            If `ts` is newly-created, the Backend **must** store this annotation with
            the TimeSeries.
        """

    @abstractmethod
    def get(self, ts: TimeSeries) -> None:
        """Retrieve the existing TimeSeries (or Scenario) `ts`.

        The TimeSeries is identified based on the unique combination of the attributes
        of `ts`:

        - :attr:`~.TimeSeries.model`,
        - :attr:`~.TimeSeries.scenario`, and
        - :attr:`~.TimeSeries.version`.

        If :attr:`.version` is :obj:`None`, the Backend **must** return the version
        marked as default, and **must** set the attribute value.

        If `ts` is a Scenario, :meth:`get` **must** set the :attr:`~.Scenario.scheme`
        attribute with the value previously passed to :meth:`init`.

        Raises
        ------
        ValueError
            If :attr:`~.TimeSeries.model` or :attr:`~.TimeSeries.scenario` does not
            exist on the Platform.

        See also
        --------
        is_default
        set_as_default
        """

    def del_ts(self, ts: TimeSeries) -> None:
        """OPTIONAL: Free memory associated with the TimeSeries `ts`.

        The default implementation has no effect.
        """

    @abstractmethod
    def check_out(self, ts: TimeSeries, timeseries_only: bool) -> None:
        """Check out `ts` for modification.

        Parameters
        ----------
        timeseries_only : bool
            ???
        """

    @abstractmethod
    def commit(self, ts: TimeSeries, comment: str) -> None:
        """Commit changes to `ts`.

        ts_init **may** modify the :attr:`~.TimeSeries.version` attribute of `ts`.

        Parameters
        ----------
        comment : str
            Description of the changes being committed.
        """

    @abstractmethod
    def discard_changes(self, ts: TimeSeries) -> None:
        """Discard changes to `ts` since the last :meth:`ts_check_out`."""

    @abstractmethod
    def set_as_default(self, ts: TimeSeries) -> None:
        """Set the current :attr:`.TimeSeries.version` as the default.

        See also
        --------
        get
        is_default
        """

    @abstractmethod
    def is_default(self, ts: TimeSeries) -> bool:
        """Return :obj:`True` if `ts` is the default version for its (model, scenario).

        See also
        --------
        get
        set_as_default
        """

    @abstractmethod
    def last_update(self, ts: TimeSeries) -> Optional[str]:
        """Return the date of the last modification of the `ts`, if any."""

    @abstractmethod
    def run_id(self, ts: TimeSeries) -> int:
        """Return the run ID of the `ts`."""

    def preload(self, ts: TimeSeries) -> None:
        """OPTIONAL: Load `ts` data into memory."""

    @staticmethod
    def _handle_rw_filters(filters: dict) -> Tuple[Optional[TimeSeries], Dict]:
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
    def get_data(
        self,
        ts: TimeSeries,
        region: Sequence[str],
        variable: Sequence[str],
        unit: Sequence[str],
        year: Sequence[str],
    ) -> Iterable[Tuple[str, str, str, int, float]]:
        """Retrieve time series data.

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
    def get_geo(
        self, ts: TimeSeries
    ) -> Iterable[Tuple[str, str, int, str, str, str, bool]]:
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
    def set_data(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        data: Dict[int, float],
        unit: str,
        subannual: str,
        meta: bool,
    ) -> None:
        """Store `data`.

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
            :obj:`True` to mark `data` as metadata.
        """

    @abstractmethod
    def set_geo(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        year: int,
        value: str,
        unit: str,
        meta: bool,
    ) -> None:
        """Store time series geodata.

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
            :obj:`True` to mark `data` as metadata.
        """

    @abstractmethod
    def delete(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        years: Iterable[int],
        unit: str,
    ) -> None:
        """Remove time series data.

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
        """

    @abstractmethod
    def delete_geo(
        self,
        ts: TimeSeries,
        region: str,
        variable: str,
        subannual: str,
        years: Iterable[int],
        unit: str,
    ) -> None:
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
        """

    # Methods for ixmp.Scenario

    def clone(
        self,
        ts: TimeSeries,
        platform_dest: Platform,
        model: str,
        scenario: str,
        annotation: str,
        keep_solution: bool,
        first_model_year: Optional[int],
    ) -> Union[TimeSeries, Scenario]:
        """Clone `ts`.

        Parameters
        ----------
        platform_dest : :class:`.Platform`
            Target backend. May be the same as ``s.platform``.
        model : str
            New model name.
        scenario : str
            New scenario name.
        annotation : str
            Description for the creation of the new scenario.
        keep_solution : bool
            If :obj:`True`, model solution data is also cloned. If :obj:`False`, it is
            discarded.
        first_model_year : int or None
            If :class:`int`, must be greater than the first model year of `s`.

        Returns
        -------
        Same class as `s`
            The cloned Scenario.
        """
        be_dest = platform_dest._backend
        cls = type(ts)

        # Create the target object
        target = cls(
            mp=platform_dest,
            model=model,
            scenario=scenario,
            version="new",
            annotation=annotation,
            scheme=getattr(ts, "scheme", None),
        )

        # TODO copy lists of identifiers
        # TODO copy time series data
        # TODO copy geodata

        # Copy set, parameter data
        # TODO copy VAR, EQU data
        for item_type in (
            (ItemType.SET, ItemType.PAR) if issubclass(cls, Scenario) else ()
        ):
            if TYPE_CHECKING:
                ts = cast(Scenario, ts)
                target = cast(Scenario, target)

            type_ = ItemType(item_type).name.lower()
            for name in self.list_items(ts, type_):
                # Contents, dimensions, and index
                data = self.item_get_elements(ts, type_, name)
                dims = self.item_index(ts, name, "names")
                sets = self.item_index(ts, name, "sets")

                try:
                    # Create the item in `target`
                    be_dest.init_item(target, type_, name, sets, dims)
                except ValueError as e:
                    if "already exists" in e.args[0]:
                        pass
                    else:
                        raise

                # Munge data. This shouldn't be necessary; output types of
                # item_get_elements should correspond to input types of
                # item_set_elements
                if item_type is ItemType.SET:
                    elements: Iterable[
                        Tuple[Any, Optional[float], Optional[str], Optional[str]]
                    ] = map(lambda v: (v, None, None, ""), cast(pd.Series, data).values)
                elif item_type is ItemType.PAR and len(dims):
                    elements = map(
                        lambda obs: (
                            tuple(getattr(obs, d) for d in dims),
                            obs.value,
                            obs.unit,
                            "",
                        ),
                        cast(pd.DataFrame, data).itertuples(),
                    )
                elif item_type is ItemType.PAR:
                    elements = [(tuple(), data["value"], data["unit"], "")]

                # Add the data to `target`
                be_dest.item_set_elements(target, type_, name, elements)

        # Store
        target.commit(
            f"Clone from ixmp://{ts.platform.name}/{ts.model}/{ts.scenario}"
            f"#{ts.version}"
        )

        return target

    @abstractmethod
    def has_solution(self, s: Scenario) -> bool:
        """Return `True` if Scenario `s` has been solved.

        If :obj:`True`, model solution data is available from the Backend.
        """

    @abstractmethod
    def list_items(self, s: Scenario, type: str) -> List[str]:
        """Return a list of names of items of `type`.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ'
        """

    @abstractmethod
    def init_item(
        self,
        s: Scenario,
        type: str,
        name: str,
        idx_sets: Sequence[str],
        idx_names: Optional[Sequence[str]],
    ) -> None:
        """Initialize an item `name` of `type`.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ' or 'var'
        name : str
            Name for the new item.
        idx_sets : sequence of str
            If empty, a 0-dimensional/scalar item is initialized. Otherwise, a
            1+-dimensional item is initialized.
        idx_names : sequence of str or None
            Optional names for the dimensions. If not supplied, the names of the
            `idx_sets` (if any) are used. If supplied, `idx_names` and `idx_sets` must
            be the same length.

        Raises
        ------
        ValueError
            if any of the `idx_sets` is not an existing set in the Scenario; if
            `idx_names` and `idx_sets` are not the same length.
        """

    @abstractmethod
    def delete_item(self, s: Scenario, type: str, name: str) -> None:
        """Remove an item `name` of `type`.

        Parameters
        ----------
        type : 'set' or 'par' or 'equ'
        name : str
            Name of the item to delete.
        """

    @abstractmethod
    def item_index(self, s: Scenario, name: str, sets_or_names: str) -> List[str]:
        """Return the index sets or names of item `name`.

        Parameters
        ----------
        sets_or_names : 'sets' or 'names'

        Returns
        -------
        list of str
        """

    @abstractmethod
    def item_get_elements(
        self, s: Scenario, type: str, name: str, filters: Dict[str, List[Any]] = None
    ) -> Union[Dict[str, Any], pd.Series, pd.DataFrame]:
        """Return elements of item `name`.

        Parameters
        ----------
        type : 'equ' or 'par' or 'set' or 'var'
        name : str
            Name of the item.
        filters : dict (str -> list), optional
            If provided, a mapping from dimension names to allowed values along that
            dimension.

            item_get_elements **must** silently accept values that are *not* members of
            the set indexing a dimension. Elements which are not :class:`str` **must**
            be handled as equivalent to their string representation; i.e.
            item_get_elements must return the same data for ``filters={'foo': [42]}``
            and ``filters={'foo': ['42']}``.

        Returns
        -------
        pandas.Series
            When `type` is 'set' and `name` an index set (not indexed by other sets).
        dict
            When `type` is 'equ', 'par', or 'var' and `name` is scalar (zero-
            dimensional). The value has the keys 'value' and 'unit' (for 'par') or 'lvl'
            and 'mrg' (for 'equ' or 'var').
        pandas.DataFrame
            For mapping sets, or all 1+-dimensional values. The dataframe has one
            column per index name with dimension values; plus the columns 'value' and
            'unit' (for 'par') or 'lvl' and 'mrg' (for 'equ' or 'var').

        Raises
        ------
        KeyError
            If `name` does not exist in `s`.
        """

    @abstractmethod
    def item_set_elements(
        self,
        s: Scenario,
        type: str,
        name: str,
        elements: Iterable[Tuple[Any, Optional[float], Optional[str], Optional[str]]],
    ) -> None:
        """Add keys or values to item `name`.

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

            If `name` is indexed by other set(s), then the number of elements
            of each `key`, and their contents, must match the index set(s).
            When `type` is 'set', `value` and `unit` **must** be :obj:`None`.

        Raises
        ------
        ValueError
            If `elements` contain invalid values, e.g. key values not in the
            index set(s).
        Exception
            If the Backend encounters any error adding the elements.

        See also
        --------
        s_init_item
        s_item_delete_elements
        """

    @abstractmethod
    def item_delete_elements(self, s: Scenario, type: str, name: str, keys) -> None:
        """Remove elements of item `name`.

        Parameters
        ----------
        type : 'par' or 'set'
        name : str
        keys : iterable of iterable of str
            If `name` is indexed by other set(s), then the number of elements of each
            key in `keys`, and their contents, must match the index set(s).
            If `name` is a basic set, then each key must be a list containing a single
            str, which must exist in the set.

        See also
        --------
        s_init_item
        s_item_set_elements
        """

    @abstractmethod
    def get_meta(
        self,
        model: Optional[str],
        scenario: Optional[str],
        version: Optional[int],
        strict: bool,
    ) -> Dict[str, Any]:
        """Retrieve all metadata attached to a specific target.

        Depending on which of `model`, `scenario`, `version` are :obj:`None`, metadata
        attached to one of the four kinds of metadata targets (see :ref:`data-meta`) is
        returned.

        If `strict` is :obj:`False`, then :func:`get_meta` **must** also return metadata
        attached to less specific or “higher level” targets:

        - For (model, scenario, version), these are (model, scenario); (model,); and
          (scenario).
        - For (model, scenario), these are (model,) and (scenario,).
        - For (model,) or (scenario,), there are no less specific targets.

        Parameters
        ----------
        model : str or None
            Model name of metadata target.
        scenario : str or None
            Scenario name of metadata target.
        version : int or None
            :attr:`.TimeSeries.version` of metadata target.
        strict : bool
            Only retrieve metadata from the specified target.

        Returns
        -------
        dict (str -> any)
            Mapping from metadata names/identifiers to values.

        Raises
        ------
        ValueError
            on unsupported (`model`, `scenario`, `version`) combinations.
        """

    @abstractmethod
    def set_meta(
        self,
        meta: dict,
        model: Optional[str],
        scenario: Optional[str],
        version: Optional[int],
    ) -> None:
        """Set metadata on a target.

        Parameters
        ----------
        meta : dict
            Mapping from metadata names/identifiers to values.
        model : str or None
            Model name of metadata target.
        scenario : str or None
            Scenario name of metadata target.
        version : int or None
            :attr:`.TimeSeries.version` of metadata target.

        Raises
        ------
        ValueError
            on unsupported (`model`, `scenario`, `version`) combinations.

        See also
        --------
        get_meta
        """

    @abstractmethod
    def remove_meta(
        self,
        names: list,
        model: Optional[str],
        scenario: Optional[str],
        version: Optional[int],
    ) -> None:
        """Remove metadata attached to a target.

        Parameters
        ----------
        names : list of str
            Metadata names/identifiers to remove.
        model : str or None
            Model name of metadata target.
        scenario : str or None
            Scenario name of metadata target.
        version : int or None
            :attr:`.TimeSeries.version` of metadata target.

        Raises
        ------
        ValueError
            on unsupported (`model`, `scenario`, `version`) combinations.

        See also
        --------
        get_meta
        """

    @abstractmethod
    def clear_solution(self, s: Scenario, from_year=None):
        """Remove data associated with a model solution.

        .. todo:: Document.
        """

    # Methods for message_ix.Scenario

    @abstractmethod
    def cat_list(self, ms: Scenario, name: str) -> List[str]:
        """Return list of categories in mapping `name`.

        Parameters
        ----------
        name : str
            Name of the category mapping set.

        Returns
        -------
        list of str
            All categories in `name`.
        """

    @abstractmethod
    def cat_get_elements(self, ms: Scenario, name: str, cat: str) -> List[str]:
        """Get elements of a category mapping.

        Parameters
        ----------
        name : str
            Name of the category mapping set.
        cat : str
            Name of the category within `name`.

        Returns
        -------
        list of str
            All set elements mapped to `cat` in `name`.
        """

    @abstractmethod
    def cat_set_elements(
        self,
        ms: Scenario,
        name: str,
        cat: str,
        keys: Union[str, Sequence[str]],
        is_unique: bool,
    ) -> None:
        """Add elements to category mapping.

        Parameters
        ----------
        name : str
            Name of the category mapping set.
        cat : str
            Name of the category within `name`.
        keys : iterable of str or list of str
            Keys to add to `cat`.
        is_unique : bool
            If :obj:`True`:

            - `keys` **must** contain only one key.
            - The Backend **must** remove any existing member of `cat`, so that it has
              only one element.
        """


class CachingBackend(Backend):
    """Backend with additional features for caching data."""

    #: :obj:`True` if caching is enabled.
    cache_enabled = True

    #: Cache of values. Keys are given by :meth:`_cache_key`; values depend on the
    #: subclass' usage of the cache.
    _cache: Dict[Tuple, object] = {}

    #: Count of number of times a value was retrieved from cache successfully
    #: using :meth:`cache_get`.
    _cache_hit: Dict[Tuple, int] = {}

    # Backend API methods

    def __init__(self, cache_enabled=True):
        super().__init__()

        self.cache_enabled = cache_enabled

        # Empty the cache
        self._cache = {}
        self._cache_hit = {}

    def del_ts(self, ts: TimeSeries):
        """Invalidate cache entries associated with `ts`."""
        self.cache_invalidate(ts)

    # New methods for CachingBackend

    @classmethod
    def _cache_key(
        self,
        ts: TimeSeries,
        ix_type: Optional[str],
        name: Optional[str],
        filters: Optional[Dict[str, Hashable]] = None,
    ) -> Tuple[Hashable, ...]:
        """Return a hashable cache key.

        ixmp `filters` (a :class:`dict` of :class:`list`) are converted to a unique id
        that is hashable.

        Returns
        -------
        tuple
            A hashable key with 4 elements for `ts`, `ix_type`, `name`, and `filters`.
        """
        ts_id = id(ts)
        if filters is None or len(filters) == 0:
            return (ts_id, ix_type, name)
        else:
            # Convert filters into a hashable object
            return (ts_id, ix_type, name, hash(json.dumps(sorted(filters.items()))))

    def cache_get(
        self, ts: TimeSeries, ix_type: str, name: str, filters: Dict
    ) -> Optional[Any]:
        """Retrieve value from cache.

        The value in :attr:`_cache` is copied to avoid cached values being modified by
        user code. :attr:`_cache_hit` is incremented.

        Raises
        ------
        KeyError
            If the key for `ts`, `ix_type`, `name` and `filters` is not in the cache.
        """
        key = self._cache_key(ts, ix_type, name, filters)

        if self.cache_enabled and key in self._cache:
            self._cache_hit[key] = self._cache_hit.setdefault(key, 0) + 1
            return copy(self._cache[key])
        else:
            raise KeyError(ts, ix_type, name, filters)

    def cache(
        self, ts: TimeSeries, ix_type: str, name: str, filters: Dict, value: Any
    ) -> bool:
        """Store `value` in cache.

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

    def cache_invalidate(
        self,
        ts: TimeSeries,
        ix_type: str = None,
        name: str = None,
        filters: Dict = None,
    ) -> None:
        """Invalidate cached values.

        With all arguments given, single key/value is removed from the cache. Otherwise,
        multiple keys/values are removed:

        - `ts` only: all cached values associated with the :class:`.TimeSeries` or
          :class:`.Scenario` object.
        - `ts`, `ix_type`, and `name`: all cached values associated with the item,
          whether filtered or unfiltered.
        """
        key = self._cache_key(ts, ix_type, name, filters)

        if filters is None:
            i = slice(1) if (ix_type is name is None) else slice(3)
            to_remove: Iterable[Tuple] = filter(
                lambda k: k[i] == key[i], self._cache.keys()
            )
        else:
            to_remove = [key]

        for key in list(to_remove):
            self._cache.pop(key, None)
