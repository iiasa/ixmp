from abc import ABC, abstractmethod


class Backend(ABC):
    """Abstract base classe for backends."""

    @abstractmethod
    def __init__(self):
        """Initialize the backend."""
        pass

    @abstractmethod
    def set_log_level(self, level):
        """Set logging level for the backend."""
        pass

    @abstractmethod
    def open_db(self):
        """(Re-)open the database connection.

        The database connection is opened automatically for many operations.
        After calling :meth:`close_db`, it must be re-opened.
        """
        pass

    @abstractmethod
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
