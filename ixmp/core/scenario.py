import logging
from collections.abc import (
    Callable,
    Iterable,
    Iterator,
    MutableSequence,
    Sequence,
)
from functools import partialmethod
from itertools import zip_longest
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload
from warnings import warn

import pandas as pd

# TODO Import from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp.backend.common import CrossPlatformClone, ItemType
from ixmp.core.item import Equation, Parameter, Set, Variable
from ixmp.core.platform import Platform
from ixmp.core.timeseries import TimeSeries
from ixmp.util import as_str_list, check_year
from ixmp.util.ixmp4 import is_ixmp4backend

if TYPE_CHECKING:
    from ixmp.types import (
        Filters,
        ModelItemType,
        ParData,
        ScalarParData,
        ScenarioInitKwargs,
        SetData,
        SolutionData,
        VersionType,
        WriteFilters,
    )

log = logging.getLogger(__name__)


class Scenario(TimeSeries):
    """Collection of model-related data.

    See :class:`.TimeSeries` for the meaning of parameters `mp`, `model`, `scenario`,
    `version`, and `annotation`.

    Parameters
    ----------
    scheme : str, optional
        Use an explicit scheme to initialize the new scenario. The
        :meth:`~.base.Model.initialize` method of the corresponding :class:`.Model`
        class in :data:`.MODELS` is used to initialize items in the Scenario.
    cache:
        .. deprecated:: 3.0
           The `cache` keyword argument to :class:`.Scenario` has no effect and raises a
           warning. Use `cache` as one of the `backend_args` to :class:`Platform` to
           disable/enable caching for storage backends that support it. Use
           :meth:`load_scenario_data` to load all data in the Scenario into an in-memory
           cache.
    """

    #: Scheme of the Scenario.
    scheme: str | None = None

    def __init__(
        self,
        mp: Platform,
        model: str,
        scenario: str,
        version: "VersionType" = None,
        scheme: str | None = None,
        annotation: str | None = None,
        **model_init_args: Unpack["ScenarioInitKwargs"],
    ) -> None:
        from ixmp.model import get_model

        # Check arguments
        if version == "new" and scheme is None:
            log.info(f"No scheme for new Scenario {model}/{scenario}")
            scheme = ""

        if "cache" in model_init_args:
            warn(
                "Scenario(…, cache=…) is deprecated; use Platform(…, cache=…) instead",
                DeprecationWarning,
            )
            model_init_args.pop("cache")

        # Call the parent constructor
        super().__init__(
            mp=mp,
            model=model,
            scenario=scenario,
            version=version,
            scheme=scheme,
            annotation=annotation,
        )

        if self.scheme == "MESSAGE" and self.__class__ is Scenario:
            # Loaded scenario has an improper scheme
            raise RuntimeError(
                f"{model}/{scenario} is a MESSAGE-scheme scenario; use "
                "message_ix.Scenario()"
            )

        # Retrieve the Model class correlating to the *scheme*
        model_class = get_model(self.scheme).__class__

        # Use the model class to initialize the Scenario
        # TODO How to convince type checker that if 'cache' is in model_init_args, it
        # is removed above?
        model_class.initialize(self, **model_init_args)  # type: ignore[misc]

        if is_ixmp4backend(self.platform._backend) and version == "new":
            run = self.platform._backend.index[self]

            # NOTE initialize() may call commit() or so which unlocks the underlying Run
            if not run.owns_lock:
                run._lock()

    def check_out(self, timeseries_only: bool = False) -> None:
        """Check out the Scenario.

        Raises
        ------
        ValueError
            If :meth:`has_solution` is :obj:`True`.

        See Also
        --------
        TimeSeries.check_out
        util.maybe_check_out
        """
        if not timeseries_only and self.has_solution():
            raise ValueError(
                "This Scenario has a solution, "
                "use `Scenario.remove_solution()` or "
                "`Scenario.clone(..., keep_solution=False)`"
            )
        super().check_out(timeseries_only)

    def load_scenario_data(self) -> None:
        """Load all Scenario data into memory.

        Raises
        ------
        ValueError
            If the Scenario was instantiated with ``cache=False``.
        """
        if not getattr(self.platform._backend, "cache_enabled", False):
            raise ValueError("Cache must be enabled to load scenario data")

        for ix_type in "equ", "par", "set", "var":
            log.debug(f"Cache {repr(ix_type)} data")
            get_func = getattr(self, ix_type)
            for name in getattr(self, "{}_list".format(ix_type))():
                get_func(name)

    def idx_sets(self, name: str) -> list[str]:
        """Return the list of index sets for an item (set, par, var, equ).

        Parameters
        ----------
        name : str
            name of the item
        """
        return self.platform._backend.item_index(self, name, "sets")

    def idx_names(self, name: str) -> list[str]:
        """Return the list of index names for an item (set, par, var, equ).

        Parameters
        ----------
        name : str
            name of the item
        """
        return self.platform._backend.item_index(self, name, "names")

    def _keys(
        self,
        name: str,
        key_or_keys: str | Sequence[str] | dict[str, Any] | pd.DataFrame | range | None,
    ) -> list[str] | list[list[str]]:
        if isinstance(key_or_keys, (list, pd.Series)):
            return as_str_list(key_or_keys)
        elif isinstance(key_or_keys, (pd.DataFrame, dict)):
            if isinstance(key_or_keys, dict):
                key_or_keys = pd.DataFrame.from_dict(key_or_keys, orient="columns")
            idx_names = self.idx_names(name)
            return [
                as_str_list(row, idx_names)
                for row in key_or_keys.itertuples(index=False)
            ]
        else:
            return [str(key_or_keys)]

    def set(self, name: str, filters: "Filters" = None) -> "SetData":
        """Return the (filtered) elements of a set.

        Parameters
        ----------
        name : str
            Name of the set.
        filters : dict
            Mapping of `dimension_name` → `elements`, where `dimension_name` is one of
            the `idx_names` given when the set was initialized (see :meth:`init_set`),
            and `elements` is an iterable of labels to include in the return value.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        return self.platform._backend.item_get_elements(self, "set", name, filters)

    # FIXME reduce complexity 18 → ≤13
    def add_set(  # noqa: C901
        self,
        name: str,
        key: int
        | str
        | Iterable[object]
        | dict[str, Sequence[int] | Sequence[str]]
        | pd.DataFrame,
        comment: str | Sequence[str] | None = None,
    ) -> None:
        """Add elements to an existing set.

        Parameters
        ----------
        name : str
            Name of the set.
        key : str or iterable of str or dict or :class:`pandas.DataFrame`
            Element(s) to be added. If `name` exists, the elements are appended to
            existing elements.
        comment : str or iterable of str, optional
            Comment describing the element(s). If given, there must be the same number
            of comments as elements.

        Raises
        ------
        KeyError
            If the set `name` does not exist. :meth:`init_set` must be called  before
            :meth:`add_set`.
        ValueError
            For invalid forms or combinations of `key` and `comment`.
        """
        # TODO expand docstring (here or in doc/source/api.rst) with examples, per
        #      test_scenario.test_add_set.

        if isinstance(key, list) and len(key) == 0:
            return  # No elements to add
        elif comment and isinstance(key, (dict, pd.DataFrame)) and "comment" in key:
            # Ambiguous arguments
            raise ValueError("ambiguous; both key['comment'] and comment= given")

        # Get index names for set *name*, may raise KeyError
        idx_names = self.idx_names(name)

        # List of keys
        keys: MutableSequence[str | list[str]] = []
        # List of comments for each key
        comments: list[str | None] = []

        # Check arguments and convert to two lists: keys and comments
        if len(idx_names) == 0:
            # Basic set not indexed by others. Keys must be strings.
            if isinstance(key, (dict, pd.DataFrame)):
                raise TypeError(
                    f"keys for basic set {name!r} must be str or list of str; got "
                    f"{type(key)}"
                )

            # Ensure keys is a list of str
            keys.extend(as_str_list(key))
        elif isinstance(key, pd.DataFrame):
            # DataFrame of key values and perhaps comments
            try:
                # Pop a 'comment' column off the DataFrame, convert to list
                comments.extend(key.pop("comment"))
            except KeyError:
                pass

            # Convert key to list of list of key values
            for row in key.to_dict(orient="records"):
                keys.append(as_str_list(row, idx_names=idx_names))
        elif isinstance(key, dict):
            # Dict of lists of key values

            # Pop a 'comment' list from the dict
            # NOTE This converts an int-comment to str, too
            _comment = list(map(str, key.pop("comment", [])))
            comments.extend(_comment)

            # Convert to list of list of key values
            keys.extend(map(as_str_list, zip(*[key[i] for i in idx_names])))
        elif isinstance(key, (int, str)) and len(idx_names) == 1:
            # Bare key given for a 1D set; wrap for convenience
            keys.append([str(key)])
        elif hasattr(key, "__getitem__") and isinstance(key[0], (int, str)):
            # List of key values; wrap
            keys.append(as_str_list(key))
        elif hasattr(key, "__getitem__") and isinstance(key[0], list):
            # List of lists of key values; convert to list of list of str
            # TODO Not sure why mypy complains here. as_str_list can handle all
            # types of key and returns list[str], which can be used to extend keys
            keys.extend(map(as_str_list, key))  # type: ignore[arg-type]
        else:
            # Other, invalid value
            raise ValueError(key)

        if isinstance(comment, str) or comment is None:
            comments.append(comment)
        else:
            # Sequence of comments
            comments.extend(comment)

        # Convert a None value into a list of None matching `keys`
        if comments == [None]:
            comments = comments * len(keys)

        # Elements to send to backend
        elements: list[tuple[Any, float | None, str | None, str | None]] = []

        # Combine iterators to tuples. If the lengths are mismatched, the sentinel
        # value 'False' is filled in
        for k, c in list(zip_longest(keys, comments, fillvalue=(False,))):
            # Check for sentinel value
            if k == (False,):
                raise ValueError(f"Comment {c!r} without matching key")
            elif c == (False,):
                raise ValueError(f"Key {k!r} without matching comment")
            elif len(idx_names) and len(idx_names) != len(k):
                raise ValueError(
                    f"{len(k)}-D key {k!r} invalid for {len(idx_names)}-D set "
                    f"{name}{idx_names!r}"
                )

            # Convince type checker that fillvalues are discarded
            assert not isinstance(k, tuple) and not isinstance(c, tuple)
            elements.append((k, None, None, c))

        # Send to backend
        self.platform._backend.item_set_elements(self, Set, name, elements)

    def remove_set(
        self,
        name: str,
        key: str | Sequence[str] | dict[str, Any] | pd.DataFrame | None = None,
    ) -> None:
        """Delete set elements or an entire set.

        Parameters
        ----------
        name : str
            Name of the set to remove (if `key` is :obj:`None`) or from which to remove
            elements.
        key : :class:`pandas.DataFrame` or list of str, optional
            Elements to be removed from set `name`.
        """
        if key is None:
            self.platform._backend.delete_item(self, "set", name)
        else:
            self.platform._backend.item_delete_elements(
                self, "set", name, self._keys(name, key)
            )

    def par(self, name: str, filters: "Filters" = None, **kwargs: Any) -> "ParData":
        """Return parameter data.

        If `filters` is provided, only a subset of data, matching the filters, is
        returned.

        Parameters
        ----------
        name : str
            Name of the parameter
        filters : dict, optional
            Keys are index names. Values are lists of index set elements. Elements not
            appearing in the respective index set(s) are silently ignored.
        """
        if len(kwargs):
            warn(
                "ignored kwargs to Scenario.par(); will raise TypeError in 4.0",
                DeprecationWarning,
            )
        return self.platform._backend.item_get_elements(self, "par", name, filters)

    def items(
        self,
        type: ItemType = ItemType.PAR,
        *,
        indexed_by: str | None = None,
        **kwargs: Any,
    ) -> Iterator[str]:
        """Iterate over model data items.

        Parameters
        ----------
        type : .ItemType, optional
            Types of items to iterate, for instance :attr:`.ItemType.PAR` for
            parameters.
        indexed_by : str, optional
            If given, only iterate over items where one of the item dimensions is
            `indexed_by` the set of this name.

        Yields
        ------
        str
            Names of items.
        """
        # Handle deprecated items
        if bool(kwargs.get("filters")):
            warn(
                "Scenario.items(…, filters=…) keyword argument; use "
                "Scenario.iter_par_data()",
                DeprecationWarning,
                2,
            )
        elif kwargs.get("par_data", None) is not None:
            warn(
                "Scenario.items(…, par_data=True); use Scenario.iter_par_data()",
                DeprecationWarning,
                2,
            )

        # Iterate over items
        for name in sorted(self.platform._backend.list_items(self, type.name.lower())):
            # Skip if `indexed_by` is given but is not in the index sets of `name`
            if indexed_by not in set(self.idx_sets(name)) | {None}:
                continue

            yield name

    def iter_par_data(
        self, filters: "Filters" = None, *, indexed_by: str | None = None
    ) -> Iterator[tuple[str, "ParData"]]:
        """Iterate over tuples of parameter names and data.

        Parameters
        ----------
        filters : dict, optional
            Filters for values along dimensions; same as the `filters` argument to
            :meth:`par`. Only valid for :attr:`.ItemType.PAR`.
        indexed_by : str, optional
            If given, only iterate over items where one of the item dimensions is
            `indexed_by` the set of this name.

        Yields
        ------
        tuple
            containing:

            1. Parameter name.
            2. Parameter data.
        """
        # Handle `filters` argument
        filters = filters or dict()

        for name in self.items(ItemType.PAR, indexed_by=indexed_by):
            idx_names = set(self.idx_names(name))

            # Skip if no overlap between given filters and this item's dimensions
            if filters and not set(filters) & idx_names:
                continue

            # Reduce the filters to only the dimensions of the item
            _filters = {k: v for k, v in filters.items() if k in idx_names}

            # Retrieve the data
            yield (name, self.par(name, filters=_filters))

    @overload
    def iter_item_data(
        self,
        item_type: Literal[ItemType.SET],
        filters: "Filters" = None,
        *,
        indexed_by: str | None = None,
    ) -> Iterator[tuple[str, "SetData"]]: ...

    @overload
    def iter_item_data(
        self,
        item_type: Literal[ItemType.PAR],
        filters: "Filters" = None,
        *,
        indexed_by: str | None = None,
    ) -> Iterator[tuple[str, "ParData"]]: ...

    @overload
    def iter_item_data(
        self,
        item_type: Literal[ItemType.EQU, ItemType.VAR],
        filters: "Filters" = None,
        *,
        indexed_by: str | None = None,
    ) -> Iterator[tuple[str, "SolutionData"]]: ...

    def iter_item_data(
        self,
        item_type: "ModelItemType",
        filters: "Filters" = None,
        *,
        indexed_by: str | None = None,
    ) -> Iterator[tuple[str, "ParData | SetData | SolutionData"]]:
        filters = filters or dict()

        data_function_map: dict[
            "ModelItemType",
            Callable[[str, "Filters"], "ParData | SetData | SolutionData"],
        ] = {
            ItemType.SET: self.set,
            ItemType.PAR: self.par,
            ItemType.EQU: self.equ,
            ItemType.VAR: self.var,
        }

        data_function = data_function_map[item_type]

        for name in self.items(type=item_type, indexed_by=indexed_by):
            idx_names = set(self.idx_names(name))

            if filters and not set(filters) & idx_names:
                continue

            _filters = {k: v for k, v in filters.items() if k in idx_names}

            yield (name, data_function(name, _filters))

    # NOTE Changing the default here since that seems to be unused/untested
    def has_item(self, name: str, item_type: "ModelItemType" = ItemType.PAR) -> bool:
        """Check whether the Scenario has an item `name` of `item_type`.

        In general, user code **should** call one of :meth:`.has_equ`, :meth:`.has_par`,
        :meth:`.has_set`, or :meth:`.has_var` instead of calling this method directly.

        Returns
        -------
        True
            if the Scenario contains an item of `item_type` with name `name`.
        False
            otherwise

        See also
        --------
        items
        """
        return name in self.items(item_type)

    #: Check whether the scenario has a equation `name`. See :meth:`has_item`.
    has_equ = partialmethod(has_item, item_type=ItemType.EQU)
    #: Check whether the scenario has a parameter `name`. See :meth:`has_item`.
    has_par = partialmethod(has_item, item_type=ItemType.PAR)
    #: Check whether the scenario has a set `name`. See :meth:`has_item`.
    has_set = partialmethod(has_item, item_type=ItemType.SET)
    #: Check whether the scenario has a variable `name`. See :meth:`has_item`.
    has_var = partialmethod(has_item, item_type=ItemType.VAR)

    def init_item(
        self,
        item_type: "ModelItemType",
        name: str,
        idx_sets: Sequence[str] | None = None,
        idx_names: Sequence[str] | None = None,
    ) -> None:
        """Initialize a new item `name` of type `item_type`.

        In general, user code **should** call one of :meth:`.init_set`,
        :meth:`.init_par`, :meth:`.init_var`, or :meth:`.init_equ` instead of calling
        this method directly.

        Parameters
        ----------
        item_type : .ItemType
            The type of the item.
        name : str
            Name of the item.
        idx_sets : sequence of str or str, optional
            Name(s) of index sets for a 1+-dimensional item. If none are given, the item
            is scalar (zero dimensional).
        idx_names : sequence of str or str, optional
            Names of the dimensions indexed by `idx_sets`. If given, they must be the
            same length as `idx_sets`.

        Raises
        ------
        ValueError
            - if `idx_names` are given but do not match the length of `idx_sets`.
            - if an item with the same `name`, of any `item_type`, already exists.
        RuntimeError
            if the Scenario is not checked out (see :meth:`~TimeSeries.check_out`).
        """
        idx_sets = as_str_list(idx_sets) or []
        idx_names = as_str_list(idx_names)

        if idx_names and len(idx_names) != len(idx_sets):
            raise ValueError(
                f"index names {repr(idx_names)} must have the same length as index sets"
                f" {repr(idx_sets)}"
            )

        # NOTE Convince type checker that all ItemType names are expected Literals
        assert item_type.name is not None
        _type: Literal["set", "par", "equ", "var"] = item_type.name.lower()  # type: ignore [assignment]
        return self.platform._backend.init_item(self, _type, name, idx_sets, idx_names)

    #: Initialize a new equation. See :meth:`init_item`.
    init_equ = partialmethod(init_item, ItemType.EQU)
    #: Initialize a new parameter. See :meth:`init_item`.
    init_par = partialmethod(init_item, ItemType.PAR)
    #: Initialize a new set. See :meth:`init_item`.
    init_set = partialmethod(init_item, ItemType.SET)
    #: Initialize a new variable. See :meth:`init_item`.
    init_var = partialmethod(init_item, ItemType.VAR)

    def list_items(
        self, item_type: "ModelItemType", indexed_by: str | None = None
    ) -> list[str]:
        """List all defined items of type `item_type`.

        See also
        --------
        items
        """
        return list(self.items(item_type, indexed_by=indexed_by))

    #: List all defined equations. See :meth:`list_items`.
    equ_list = partialmethod(list_items, ItemType.EQU)
    #: List all defined parameters. See :meth:`list_items`.
    par_list = partialmethod(list_items, ItemType.PAR)
    #: List all defined sets. See :meth:`list_items`.
    set_list = partialmethod(list_items, ItemType.SET)
    #: List all defined variables. See :meth:`list_items`.
    var_list = partialmethod(list_items, ItemType.VAR)

    # FIXME reduce complexity 15 → ≤13
    def add_par(  # noqa: C901
        self,
        name: str,
        key_or_data: str
        | Sequence[str]
        | dict[str, Any]
        | pd.DataFrame
        | range
        | None = None,
        value: float | Sequence[float] | None = None,
        unit: str | Sequence[str] | None = None,
        comment: str | Sequence[str] | None = None,
    ) -> None:
        """Set the values of a parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key_or_data : str or iterable of str or range or dict or pandas.DataFrame
            Element(s) to be added.
        value : float or iterable of float, optional
            Values.
        unit : str or iterable of str, optional
            Unit symbols.
        comment : str or iterable of str, optional
            Comment(s) for the added values.
        """
        # Number of dimensions in the index of *name*
        idx_names = self.idx_names(name)
        N_dim = len(idx_names)

        # Convert valid forms of arguments to pd.DataFrame
        if isinstance(key_or_data, dict):
            # dict containing data
            data = pd.DataFrame.from_dict(key_or_data, orient="columns")
        elif isinstance(key_or_data, pd.DataFrame):
            data = key_or_data.copy()
            if value is not None:
                if "value" in data.columns:
                    raise ValueError("both key_or_data.value and value supplied")
                else:
                    data["value"] = value
        else:
            # One or more keys; convert to a list of strings
            if isinstance(key_or_data, range):
                key_or_data = list(map(str, key_or_data))
            keys = self._keys(name, key_or_data)

            # Check the type of value
            if isinstance(value, (float, int)):
                # Single value

                if N_dim > 1 and len(keys) == N_dim:
                    # Ambiguous case: ._key() above returns ['dim_0', 'dim_1'], when we
                    # really want [['dim_0', 'dim_1']]
                    # TODO Adjust ignore comment once parametrized generics can be
                    # checked
                    keys = [keys]  # type: ignore[assignment]

                # Use the same value for all keys
                values: list[Any] = [float(value)] * len(keys)
            else:
                # Multiple values
                values = list(value) if value else []

            data = pd.DataFrame(zip_longest(keys, values), columns=["key", "value"])  # type: ignore[arg-type]
            if data.isna().any(axis=None):
                raise ValueError("Length mismatch between keys and values")

        # Column types
        types: dict[str, type[float] | type[str] | type[object]] = {
            "key": str if N_dim == 1 else object,
            "value": float,
            "unit": str,
            "comment": str,
        }

        # Further handle each column
        if "key" not in data.columns:
            # Form the 'key' column from other columns
            if N_dim > 1 and len(data):
                data["key"] = (
                    data[idx_names].astype(str).agg(lambda s: s.tolist(), axis=1)
                )
            else:
                data["key"] = data[idx_names[0]]

        if "value" not in data.columns:
            raise ValueError("no parameter values supplied")

        if "unit" not in data.columns:
            # Broadcast single unit across all observations. Pandas raises ValueError if
            # `unit` is iterable but the wrong length.
            data = data.assign(unit=unit or "???")

        if "comment" not in data.columns:
            if not comment:
                # Don't apply a dtype to None values
                types.pop("comment")
            # Broadcast a single comment value across all observations. Pandas raises
            # ValueError if `comment` is iterable but the wrong length.
            data = data.assign(comment=comment or None)

        # Convert types, generate tuples
        elements = map(
            lambda e: (e.key, e.value, e.unit, e.comment),
            data.astype(types).itertuples(),
        )

        # Store
        # TODO Not sure how to tell type checker, but columns are always converted to
        # tuple[str | object, float, str, str]
        self.platform._backend.item_set_elements(self, Parameter, name, elements)  # type: ignore[arg-type]

    def init_scalar(
        self,
        name: str,
        val: float | int,
        unit: str,
        comment: str | None = None,
    ) -> None:
        """Initialize a new scalar and set its value.

        Parameters
        ----------
        name : str
            Name of the scalar
        val : float or int
            Initial value of the scalar.
        unit : str
            Unit of the scalar.
        comment : str, optional
            Description of the scalar.
        """
        self.init_par(name, [], [])
        self.change_scalar(name, val, unit, comment)

    def scalar(self, name: str) -> "ScalarParData":
        """Return the value and unit of a scalar parameter.

        Parameters
        ----------
        name : str
            Name of the scalar.

        Returns
        -------
        dict
            with the keys "value" and "unit".
        """
        data = self.platform._backend.item_get_elements(self, "par", name, None)
        assert isinstance(data, dict)
        return data

    def change_scalar(
        self,
        name: str,
        val: float | int,
        unit: str,
        comment: str | None = None,
    ) -> None:
        """Set the value and unit of a scalar.

        Parameters
        ----------
        name : str
            Name of the scalar.
        val : float or int
            New value of the scalar.
        unit : str
            New unit of the scalar.
        comment : str, optional
            Description of the change.
        """
        self.platform._backend.item_set_elements(
            self, Parameter, name, [(None, float(val), unit, comment)]
        )

    def remove_par(
        self,
        name: str,
        key: pd.DataFrame | list[str] | dict[str, list[str]] | None = None,
    ) -> None:
        """Remove parameter values or an entire parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key : pandas.DataFrame or list or str, optional
            Elements to be removed. If a :class:`pandas.DataFrame`, must contain the
            same columns (indices/dimensions) as the parameter. If a :class:`list`, a
            single key for a single data point; the individual elements must correspond
            to the indices/dimensions of the parameter.
        """
        if key is None:
            self.platform._backend.delete_item(self, "par", name)
        else:
            self.platform._backend.item_delete_elements(
                self, "par", name, self._keys(name, key)
            )

    # FIXME What ensures that filters has the correct type?
    def var(
        self, name: str, filters: "Filters" = None, **kwargs: Any
    ) -> "SolutionData":
        """Return a dataframe of (filtered) elements for a specific variable.

        Parameters
        ----------
        name : str
            name of the variable
        filters : dict
            index names mapped list of index set elements
        """
        return self.platform._backend.item_get_elements(self, "var", name, filters)

    def equ(
        self, name: str, filters: "Filters" = None, **kwargs: Any
    ) -> "SolutionData":
        """Return a dataframe of (filtered) elements for a specific equation.

        Parameters
        ----------
        name : str
            name of the equation
        filters : dict
            index names mapped list of index set elements
        """
        return self.platform._backend.item_get_elements(self, "equ", name, filters)

    def clone(
        self,
        model: str | None = None,
        scenario: str | None = None,
        annotation: str | None = None,
        keep_solution: bool = True,
        shift_first_model_year: int | None = None,
        platform: Platform | None = None,
    ) -> "Scenario":
        """Clone the current scenario and return the clone.

        If the (`model`, `scenario`) given already exist on the :class:`.Platform`, the
        `version` for the cloned Scenario follows the last existing version. Otherwise,
        the `version` for the cloned Scenario is 1.

        .. note::
            :meth:`clone` does not set or alter default versions. This means that a
            clone to new (`model`, `scenario`) names has no default version, and will
            not be returned by :meth:`Platform.scenario_list` unless `default=False` is
            given.

        Parameters
        ----------
        model : str, optional
            New model name. If not given, use the existing model name.
        scenario : str, optional
            New scenario name. If not given, use the existing scenario name.
        annotation : str, optional
            Explanatory comment for the clone commit message to the database.
        keep_solution : bool, optional
            If :obj:`True`, include all timeseries data and the solution (vars and
            equs) from the source scenario in the clone. If :obj:`False`, only
            include timeseries data marked `meta=True` (see :meth:`.add_timeseries`).
        shift_first_model_year: int, optional
            If given, all timeseries data in the Scenario is omitted from the clone for
            years from `first_model_year` onwards. Timeseries data with the `meta` flag
            (see :meth:`.add_timeseries`) are cloned for all years.
        platform : :class:`Platform`, optional
            Platform to clone to (default: current platform)
        """
        if shift_first_model_year is not None:
            if keep_solution:
                log.warning("Override keep_solution=True for shift_first_model_year")
                keep_solution = False

        platform = platform or self.platform
        model = model or self.model
        scenario = scenario or self.scenario

        try:
            # Use the Backend implementation to clone:
            # - Within the same Backend.
            # - Across 2 Backends of the same type, if supported.
            # - Across 2 Backends of differing type, if supported.
            return self.platform._backend.clone(
                self,
                platform_dest=platform,
                model=model,
                scenario=scenario,
                annotation=annotation,
                keep_solution=keep_solution,
                first_model_year=shift_first_model_year
                if check_year(shift_first_model_year, "first_model_year")
                else None,
            )
        except CrossPlatformClone:
            # Use a generic, Backend-unaware clone method
            return _clone(self, platform, model, scenario, keep_solution)

    def has_solution(self) -> bool:
        """Return :obj:`True` if the Scenario contains model solution data."""
        return self.platform._backend.has_solution(self)

    def remove_solution(self, first_model_year: int | None = None) -> None:
        """Remove the solution from the scenario.

        This function removes the solution (variables and equations) and timeseries
        data marked as `meta=False` from the scenario (see :meth:`.add_timeseries`).

        Parameters
        ----------
        first_model_year: int, optional
            If given, timeseries data marked as `meta=False` is removed only for years
            from `first_model_year` onwards.

        Raises
        ------
        ValueError
            If Scenario has no solution or if `first_model_year` is not `int`.
        """
        if self.has_solution():
            check_year(first_model_year, "first_model_year")
            self.platform._backend.clear_solution(self, first_model_year)
        else:
            raise ValueError("This Scenario does not have a solution!")

    def solve(
        self,
        model: str | None = None,
        callback: Callable[["Scenario"], bool] | None = None,
        cb_kwargs: dict[str, Any] = {},
        **model_options: Any,
    ) -> None:
        """Solve the model and store output.

        ixmp 'solves' a model by invoking the run() method of a :class:`.Model`
        subclass—for instance, :meth:`.GAMSModel.run`. Depending on the underlying
        model code, different steps are taken; see each model class for details. In
        general:

        1. Data from the Scenario are written to a **model input file**.
        2. Code or an external program is invoked to perform calculations or
           optimizations, **solving the model**.
        3. Data representing the model outputs or solution are read from a **model
           output file** and stored in the Scenario.

        If the optional argument `callback` is given, additional steps are performed:

        4. Execute the `callback` with the Scenario as an argument. The Scenario has an
           `iteration` attribute that stores the number of times the underlying model
           has been solved (#2).
        5. If the `callback` returns :obj:`False` or similar, iterate by repeating from
           step #1. Otherwise, exit.

        Parameters
        ----------
        model : str
            model (e.g., MESSAGE) or GAMS file name (excluding '.gms')
        callback : callable, optional
            Method to execute arbitrary non-model code. Must accept a single argument:
            the Scenario. Must return a non-:obj:`False` value to indicate convergence.
        cb_kwargs : dict, optional
            Keyword arguments to pass to `callback`.
        model_options :
            Keyword arguments specific to the `model`. See :class:`.GAMSModel`.

        Warns
        -----
        UserWarning
            If `callback` is given and returns :obj:`None`. This may indicate that the
            user has forgotten a ``return`` statement, in which case the iteration will
            continue indefinitely.

        Raises
        ------
        ValueError
            If the Scenario has already been solved.
        """
        from ixmp.model import get_model

        if self.has_solution():
            raise ValueError(
                "Scenario contains a model solution; call .remove_solution() before "
                "solve()"
            )

        # Instantiate a model
        model_obj = get_model(model or self.scheme, **model_options)

        # Validate `callback`
        if callback is not None:
            if not callable(callback):
                raise ValueError(f"callback={repr(callback)} is not callable")
            cb = callback
        else:

            def cb(scenario: Scenario) -> bool:
                return True

        # NOTE This can never happen based on annotations
        # Flag to warn if the *callback* appears not to return anything
        warn_none = True

        # Iterate until convergence
        while True:
            model_obj.run(self)

            # Store an iteration number to help the callback
            if not hasattr(self, "iteration"):
                self.iteration = 0

            self.iteration += 1

            # Invoke the callback
            cb_result = cb(self, **cb_kwargs)

            # NOTE This can never happen based on annotations, but we still test this
            if cb_result is None and warn_none:  # type: ignore[unreachable]
                warn(  # type: ignore[unreachable]
                    "solve(callback=...) argument returned None; will loop "
                    "indefinitely unless True is returned."
                )
                # Don't repeat the warning
                warn_none = False

            if cb_result:
                # Callback indicates convergence is reached
                break

    # Input and output
    def to_excel(
        self,
        path: PathLike[str],
        items: ItemType = ItemType.SET | ItemType.PAR,
        filters: "WriteFilters | None" = None,
        max_row: int | None = None,
    ) -> None:
        """Write Scenario to a Microsoft Excel file.

        Parameters
        ----------
        path : os.PathLike
            File to write. Must have suffix :file:`.xlsx`.
        items : .ItemType, optional
            Types of items to write. Either :attr:`.SET` | :attr:`.PAR` (i.e. only sets
            and parameters), or :attr:`.MODEL` (also variables and equations, i.e.
            model solution data).
        filters : dict, optional
            Filters for values along dimensions; same as the `filters` argument to
            :meth:`par`.
        max_row: int, optional
            Maximum number of rows in each sheet. If the number of elements in an item
            exceeds this number or :data:`.EXCEL_MAX_ROWS`, then an item is written to
            multiple sheets named, e.g. 'foo', 'foo(2)', 'foo(3)', etc.

        See also
        --------
        :ref:`excel-data-format`
        read_excel
        """
        # Default filters: empty dict
        filters = filters or dict()

        # Select the current scenario
        filters["scenario"] = self

        # Invoke the backend method
        self.platform._backend.write_file(
            Path(path), items, filters=filters, max_row=max_row
        )

    def read_excel(
        self,
        path: PathLike[str],
        add_units: bool = False,
        init_items: bool = False,
        commit_steps: bool = False,
    ) -> None:
        """Read a Microsoft Excel file into the Scenario.

        Parameters
        ----------
        path : os.PathLike
            File to read. Must have suffix '.xlsx'.
        add_units : bool, optional
            Add missing units, if any, to the Platform instance.
        init_items : bool, optional
            Initialize sets and parameters that do not already exist in the Scenario.
        commit_steps : bool, optional
            Commit changes after every data addition.

        See also
        --------
        :ref:`excel-data-format`
        .TimeSeries.read_file
        to_excel
        """
        self.platform._backend.read_file(
            Path(path),
            ItemType.MODEL,
            filters=dict(scenario=self),
            add_units=add_units,
            init_items=init_items,
            commit_steps=commit_steps,
        )


def _clone(
    s: Scenario, platform_dest: Platform, model: str, scenario: str, keep_solution: bool
) -> Scenario:
    """:class:`Backend`-unaware implementation of :meth:`Scenario.clone`.

    This function uses only the Scenario and Backend APIs.
    """
    from . import timeseries

    # - Create a clone of the same class as `s` on `platform_dest`.
    # - Clone time series data.
    s_dest = timeseries._clone(s, platform_dest, model, scenario)
    # Direct reference to the backend storing s_dest
    b_dest = s_dest.platform._backend

    def _data_to_elements(
        data: dict[str, Any] | pd.DataFrame,
    ) -> Iterator[tuple[Any, ...]]:
        """Convert data to elements for :meth:`.Backend.item_set_elements`."""
        if isinstance(data, dict):
            # Identify element value fields for (equ, var) or (par, set)
            c0, c1 = ("lvl", "mrg") if "lvl" in data else ("value", "unit")
            # Yield a single element
            yield (None, data[c0], data[c1], "")
        else:
            c0, c1 = ("lvl", "mrg") if "lvl" in data.columns else ("value", "unit")
            cols = data.columns.to_list()
            # Indices of key colums, for pd.DataFrame.take()
            i_key = list(range(len(cols)))
            # Indices of value fields
            i0, i1 = cols.index(c0), cols.index(c1)
            # Exclude these from the key
            i_key.remove(i0)
            i_key.remove(i1)

            yield from [
                (row.take(i_key).to_list(), row.iloc[i0], row.iloc[i1], "")
                for _, row in data.iterrows()
            ]

    def _maybe_init(it: ItemType, name: str) -> None:
        """Initialize item `name` on `s_dest`, if it does not already exist."""
        if name in existing[it]:
            return
        idx_sets, idx_names = s.idx_sets(name), s.idx_names(name)

        b_dest.init_item(s_dest, type_.ix_type, name, idx_sets, idx_names)

    # Clone optimization data

    # Identify items already present in the Scenario. These may be created by a
    # Model.initialize() class method.
    existing = {
        item_type: set(s_dest.list_items(item_type))  # type: ignore [arg-type]
        for item_type in (ItemType.EQU, ItemType.PAR, ItemType.SET, ItemType.VAR)
    }

    with s_dest.transact():
        # Iterate over item names and data
        for name, set_data in s.iter_item_data(ItemType.SET):
            _maybe_init(ItemType.SET, name)
            s_dest.add_set(name, set_data)

        for item_type, type_, condition in (
            (ItemType.PAR, Parameter, True),
            # Only clone EQU and VAR data if keep_solution is True
            (ItemType.EQU, Equation, keep_solution),
            (ItemType.VAR, Variable, keep_solution),
        ):
            if not condition:
                continue
            for name, data in s.iter_item_data(item_type):  # type: ignore [call-overload]
                _maybe_init(item_type, name)
                b_dest.item_set_elements(s_dest, type_, name, _data_to_elements(data))

    return s_dest
