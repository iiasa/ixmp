from abc import ABC, abstractmethod


class Backend(ABC):
    """Abstract base classe for backends.

    Some methods below are decorated as @abstractmethod; this means they MUST
    be overridden by a subclass of Backend. Others that are not decorated
    mean that the behaviour here is the default behaviour; subclasses can
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
    def s_init(self, s, annotation=None):
        """Initialize the ixmp.Scenario *s*.

        The method MAY:

        - Modify the version attr of the returned object.
        """
        pass
