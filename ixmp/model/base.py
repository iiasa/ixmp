import logging
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from ixmp.backend.common import ItemType
from ixmp.util import maybe_check_out, maybe_commit

if TYPE_CHECKING:
    from ixmp.core.scenario import Scenario
    from ixmp.types import InitializeItemsKwargs

log = logging.getLogger(__name__)


class ModelError(Exception):
    """Error in model code—that is, :meth:`.Model.run` or other code called by it."""


class Model(ABC):
    #: Name of the model.
    name: str = "base"

    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Constructor.

        **Required.**

        Parameters
        ----------
        kwargs :
            Model options, passed directly from :meth:`.Scenario.solve`.

            Model subclasses MUST document acceptable option values.
        """

    @classmethod
    def clean_path(cls, value: str) -> str:
        """Substitute invalid characters in `value` with "_"."""
        chars = r'<>"/\|?*' + (":" if os.name == "nt" else "")
        return re.sub("[{}]+".format(re.escape(chars)), "_", value)

    @staticmethod
    def enforce(scenario: "Scenario") -> None:
        """Enforce data consistency in `scenario`.

        **Optional**; the default implementation does nothing. Subclass implementations
        of :meth:`enforce`:

        - **should** modify the contents of sets and parameters so that `scenario`
          contains structure and data that is consistent with the underlying model.
        - **must not** add or remove sets or parameters; for that, use
          :meth:`initialize`.

        :meth:`enforce` is always called by :meth:`run` before the model is run or
        solved; it **may** be called manually at other times.

        Parameters
        ----------
        scenario : Scenario
            Object on which to enforce data consistency.
        """

    @classmethod
    def initialize(cls, scenario: "Scenario") -> None:
        """Set up *scenario* with required items.

        **Optional**; the default implementation does nothing. Subclass implementations
        of :meth:`initialize`:

        - **may** add sets, set elements, and/or parameter values.
        - **may** accept any number of keyword arguments to control behaviour.
        - **must not** modify existing parameter data in *scenario*, either by deleting
          or overwriting values; for that, use :meth:`enforce`.

        Parameters
        ----------
        scenario : Scenario
            Object to initialize.

        See also
        --------
        initialize_items
        """
        log.debug(f"No initialization for {repr(scenario.scheme)}-scheme Scenario")

    @classmethod
    def initialize_items(
        cls, scenario: "Scenario", items: Mapping[str, "InitializeItemsKwargs"]
    ) -> None:
        """Helper for :meth:`initialize`.

        All of the `items` are added to `scenario`. Existing items are not modified.
        Warnings are logged if the description in `items` conflicts with the index
        set(s) and/or index name(s) of existing items.

        initialize_items may perform one commit. `scenario` is in the same state
        (checked in, or checked out) after initialize_items is complete.

        Parameters
        ----------
        scenario : Scenario
            Object to initialize.
        items :
            Keys are names of ixmp items (set, parameter, equation, or variable) to
            initialize. Values are :class:`dict`, and each **must** have the key
            'ix_type' (one of 'set', 'par', 'equ', or 'var'); other keys ("idx_sets" and
            optionally "idx_names") are keyword arguments to :meth:`.init_item`.

        Raises
        ------
        ValueError
            if `scenario` has a solution, i.e. :meth:`~.Scenario.has_solution` is
            :obj:`True`.

        See also
        --------
        Scenario.init_item
        """
        # Don't know if the Scenario is checked out
        checkout = None

        # Lists of items in the Scenario
        existing_items = dict()

        # List of items initialized
        items_initialized = []

        for name, info in items.items():
            # Keyword args to Scenario.init_item()
            init_kw: dict[str, Any] = dict(name=name)

            # Convert str into a member of the ix_type Enum
            _t = ItemType[info["ix_type"].upper()]
            assert ItemType.is_model_data(_t)

            # Store a list of items of `ix_type`
            if _t not in existing_items:
                existing_items[_t] = scenario.list_items(_t)

            exists: bool = name in existing_items[_t]
            for key, method in (
                ("idx_sets", scenario.idx_sets),
                ("idx_names", scenario.idx_names),
            ):
                # Copy the values from `info` to `init_kw`
                if values := cast(Sequence[str] | None, info.get(key)):
                    init_kw[key] = tuple(values)

                # If the item exists; check its index sets/names
                if exists:
                    existing, arg = tuple(method(name)), init_kw.get(key, ())
                    if existing != arg and not (key == "idx_names" and arg == ()):
                        log.warning(
                            f"Existing index {key.split('_')[-1]} of {name!r} "
                            f"{existing!r} do not match {arg!r}"
                        )

            # Can't do anything to existing items
            if exists:
                continue

            # Item doesn't exist and must be initialized

            # Check out the Scenario, if not already checked out
            try:
                checkout = maybe_check_out(scenario, checkout)
            except ValueError as exc:  # pragma: no cover
                # The Scenario has a solution. This indicates an inconsistent situation:
                # the Scenario lacks the item *name*, but somehow it was successfully
                # solved without it, and the solution stored. Can't proceed further.
                log.error(str(exc))
                return

            # Initialize the item
            log.info(f"Initialize {_t} {name!r} as {info}")
            scenario.init_item(_t, **init_kw)

            # Record
            items_initialized.append(name)

        maybe_commit(
            scenario, bool(items_initialized), f"{cls.__name__}.initialize_items"
        )

        if items_initialized and not checkout:
            # Scenario was originally in a checked out state; restore
            maybe_check_out(scenario)

    @abstractmethod
    def run(self, scenario: "Scenario") -> None:
        """Execute the model.

        **Required.** Implementations of :meth:`run`:

        - **must** call :meth:`enforce`.


        Parameters
        ----------
        scenario : Scenario
            Scenario object to solve by running the Model.
        """
