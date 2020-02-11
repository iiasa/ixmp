import logging

from abc import ABC, abstractmethod


log = logging.getLogger(__name__)


class Model(ABC):  # pragma: no cover
    #: Name of the model.
    name = 'base'

    @abstractmethod
    def __init__(self, name, **kwargs):
        """Constructor.

        Parameters
        ----------
        kwargs :
            Model options, passed directly from :meth:`.Scenario.solve`.

            Model subclasses MUST document acceptable option values.
        """
        pass

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
            Scenario object to initialize.

        See also
        --------
        initialize_items
        """
        log.debug('No initialization for {!r}-scheme Scenario'
                  .format(scenario.scheme))

    @classmethod
    def initialize_items(cls, scenario, items):
        """Helper for :meth:`initialize`.

        All of the *items* are added to *scenario*. Existing items are not
        modified.

        Parameters
        ----------
        scenario : .Scenario
            Scenario object to initialize.
        items : dict of (str -> dict)
            Each key is the name of an ixmp item (set, parameter, equation, or
            variable) to initialize. Each dict **must** have the key 'ix_type';
            one of 'set', 'par', 'equ', or 'var'; any other entries are keyword
            arguments to the methods :meth:`.init_set` etc.

        See also
        --------
        .init_equ
        .init_par
        .init_set
        .init_var
        """
        try:
            # If *scenario* is already committed to the Backend, it must be
            # checked out.
            scenario.check_out()
        except RuntimeError:
            # If *scenario* is new (has not been committed), the checkout
            # attempt raises an exception
            pass

        for name, item_info in items.items():
            # Copy so that pop() below does not modify *items*
            item_info = item_info.copy()

            # Get the appropriate method, e.g. init_set or init_par
            ix_type = item_info.pop('ix_type')
            init_method = getattr(scenario, 'init_{}'.format(ix_type))

            try:
                # Add the item
                init_method(name=name, **item_info)
            except ValueError:
                # Item already exists
                pass

    @abstractmethod
    def run(self, scenario):
        """Execute the model.

        Parameters
        ----------
        scenario : .Scenario
            Scenario object to solve by running the Model.
        """
        pass
