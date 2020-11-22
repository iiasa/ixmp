import logging
from abc import ABC, abstractmethod

from ixmp.utils import maybe_check_out, maybe_commit

log = logging.getLogger(__name__)


class Model(ABC):
    #: Name of the model.
    name = "base"

    @abstractmethod
    def __init__(self, name, **kwargs):
        """Constructor.

        Parameters
        ----------
        kwargs :
            Model options, passed directly from :meth:`.Scenario.solve`.

            Model subclasses MUST document acceptable option values.
        """

    @classmethod
    def initialize(cls, scenario):
        """Set up *scenario* with required items.

        Implementations of :meth:`initialize`:

        - **may** add sets, set elements, and/or parameter values.
        - **may** accept any number of keyword arguments to control behaviour.
        - **must not** modify existing parameter data in *scenario*, either by
          deleting or overwriting values.

        Parameters
        ----------
        scenario : .Scenario
            Object to initialize.

        See also
        --------
        initialize_items
        """
        log.debug(f"No initialization for {repr(scenario.scheme)}-scheme Scenario")

    @classmethod
    def initialize_items(cls, scenario, items):
        """Helper for :meth:`initialize`.

        All of the `items` are added to `scenario`. Existing items are not
        modified. Errors are logged if the description in `items` conflicts
        with the index set(s) and/or index name(s) of existing items.

        initialize_items may perform one commit. `scenario` is in the same
        state (checked in, or checked out) after initialize_items is complete.

        Parameters
        ----------
        scenario : .Scenario
            Object to initialize.
        items : dict of (str -> dict)
            Each key is the name of an ixmp item (set, parameter, equation, or
            variable) to initialize. Each dict **must** have the key 'ix_type';
            one of 'set', 'par', 'equ', or 'var'; any other entries are keyword
            arguments to the methods :meth:`.init_set` etc.

        Raises
        ------
        ValueError
            if `scenario` has a solution, i.e. :meth:`~.Scenario.has_solution`
            is :obj:`True`.

        See also
        --------
        .init_equ
        .init_par
        .init_set
        .init_var
        """
        # Don't know if the Scenario is checked out
        checkout = None

        # Lists of items in the Scenario
        existing_items = dict()

        # Lists of items initialized
        items_initialized = []

        for name, item_info in items.items():
            # Copy so that pop() below does not modify *items*
            item_info = item_info.copy()

            # Check that the item exists
            ix_type = item_info.pop("ix_type")

            if ix_type not in existing_items:
                # Store a list of items of *ix_type*
                method = getattr(scenario, f"{ix_type}_list")
                existing_items[ix_type] = method()

            # Item must be initialized if it does not exist

            if name in existing_items[ix_type]:
                # Item exists; check its index sets and names
                for key, values in item_info.items():
                    values = values or []
                    existing = getattr(scenario, key)(name)
                    if existing != list(values):
                        # The existing index sets or names do not match
                        log.warning(
                            f"Existing index {key.split('_')[-1]} of "
                            f"{repr(name)} {repr(existing)} do not match "
                            f"{repr(values)}"
                        )

                # Skip; can't do anything to existing items
                continue

            # Item doesn't exist and must be initialized

            # Possibly check out the Scenario
            try:
                checkout = maybe_check_out(scenario, checkout)
            except ValueError as exc:  # pragma: no cover
                # The Scenario has a solution. This indicates an inconsistent
                # situation: the Scenario lacks the item *name*, but somehow it
                # was successfully solved without it, and the solution stored.
                # Can't proceed further.
                log.error(str(exc))
                return

            # Get the appropriate method, e.g. init_set, and add the item
            log.info(f"Initialize {ix_type} {repr(name)} as {item_info}")
            getattr(scenario, f"init_{ix_type}")(name=name, **item_info)

            # Record
            items_initialized.append(name)

        maybe_commit(
            scenario, len(items_initialized), f"{cls.__name__}.initialize_items"
        )

        if len(items_initialized) and not checkout:
            # Scenario was originally in a checked out state; restore
            maybe_check_out(scenario)

    @abstractmethod
    def run(self, scenario):
        """Execute the model.

        Parameters
        ----------
        scenario : .Scenario
            Scenario object to solve by running the Model.
        """
