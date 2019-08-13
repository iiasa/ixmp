from abc import ABC, abstractmethod


class Backend(ABC):
    """Abstract base classe for backends."""

    @abstractmethod
    def __init__(self):
        """Initialize the backend."""
        pass
