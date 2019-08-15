from abc import ABC, abstractmethod


class Backend(ABC):
    """Abstract base class for backends.

    Some methods below are decorated as @abstractmethod; this means they MUST
    be overridden by a subclass of Backend. Others that are not decorated
    mean that the behaviour here is the default behaviour; subclasses MAY
    leave, replace or extend this behaviour as needed.

    """

    def __init__(self):
        """Initialize the backend."""
        pass

    @abstractmethod
    def set_log_level(self, level):
        """Set logging level for the backend."""
        pass

    def open_db(self):
        """(Re-)open the database connection.

        The database connection is opened automatically for many operations.
        After calling :meth:`close_db`, it must be re-opened.
        """
        pass

    def close_db(self):
        """Close the database connection.

        Some backend database connections can only be used by one
        :class:`Backend` instance at a time. Any existing connection must be
        closed before a new one can be opened.
        """
        pass

    @abstractmethod
    def units(self):
        """Return all units described in the database.

        Returns
        -------
        list
        """
        pass

    @abstractmethod
    def ts_init(self, ts, annotation=None):
        """Initialize the ixmp.TimeSeries *ts*.

        The method MAY:

        - Modify the version attr of the returned object.
        """
        pass

    @abstractmethod
    def ts_check_out(self, ts, timeseries_only):
        """Check out the ixmp.TimeSeries *s* for modifications.

        Parameters
        ----------
        timeseries_only : bool
            ???
        """
        pass

    @abstractmethod
    def ts_commit(self, ts, comment):
        """Commit changes to the ixmp.TimeSeries *s* since the last check_out.

        The method MAY:

        - Modify the version attr of *ts*.
        """
        pass

    @abstractmethod
    def s_init(self, s, annotation=None):
        """Initialize the ixmp.Scenario *s*.

        The method MAY:

        - Modify the version attr of the returned object.
        """
        pass

    @abstractmethod
    def s_has_solution(self):
        """Return :obj:`True` if the Scenario has been solved.

        If :obj:`True`, model solution data exists in the database.
        """
        pass

    @abstractmethod
    def s_list_items(self, s, type):
        """Return a list of items of *type* in the Scenario *s*."""
        pass

    @abstractmethod
    def s_init_item(self, s, type, name):
        """Initialize or create a new item *name* of *type* in Scenario *s*."""
        pass

    @abstractmethod
    def s_item_index(self, s, name, sets_or_names):
        """Return the index sets or names of item *name*.

        Parameters
        ----------
        sets_or_names : 'sets' or 'names'
        """
        pass

    @abstractmethod
    def s_item_elements(self, s, type, name, filters=None, has_value=False,
                        has_level=False):
        """Return elements of item *name* in Scenario *s*.

        The return type varies according to the *type* and contents:

        - Scalars vs. parameters.
        - Lists, e.g. set elements.
        - Mapping sets.
        - Multi-dimensional parameters, equations, or variables.
        """
        # TODO exactly specify the return types in the docstring using MUST,
        # MAY, etc. terms
        pass

    @abstractmethod
    def s_add_set_elements(self, s, name, elements):
        """Add elements to set *name* in Scenario *s*.

        Parameters
        ----------
        elements : list of 2-tuples
            The first element of each tuple is a key (str or list of str).
            The number and order of key dimensions must match the index of
            *name*, if any. The second element is a str comment describing the
            key, or None.

        Raises
        ------
        ValueError
            If *elements* contain invalid values, e.g. for an indexed set,
            values not in the index set(s).
        Exception
            If the Backend encounters any error adding the key.
        """
        pass
