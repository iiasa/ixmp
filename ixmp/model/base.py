from abc import ABC, abstractmethod


class Model(ABC):
    @abstractmethod
    def __init__(self, name, **kwargs):
        pass

    @abstractmethod
    def run(self, scenario):
        """Execute the model."""
        pass
