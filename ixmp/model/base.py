from abc import ABC, abstractmethod


class Model(ABC):
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

    @abstractmethod
    def run(self, scenario):
        """Execute the model.

        Parameters
        ----------
        scenario : .Scenario
            Scenario object to solve by running the Model.
        """
        pass
