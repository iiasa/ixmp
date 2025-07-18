from dataclasses import dataclass, field
from typing import ClassVar


@dataclass
class Item:
    """Description of an :mod:`ixmp` data ‘item’.

    Instances of this class carry only structural information, not data.
    """

    #: Identifier understood by :mod:`ixmp.backend`.
    ix_type: ClassVar[str] = "item"

    #: Name of the item.
    name: str

    #: Coordinates of the item; that is, the names of sets that index its dimensions.
    #: The same set name may be repeated if it indexes multiple dimensions.
    coords: tuple[str, ...] = field(default_factory=tuple)

    #: Dimensions of the item.
    dims: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.dims == self.coords:
            # No distinct dimension names; don't store these
            self.dims = tuple()

    @classmethod
    def is_model_data(cls) -> bool:
        return cls in (Equation, Parameter, Set, Variable)


@dataclass
class TimeSeries(Item):
    ix_type = "ts"

    dims = ("model", "scenario", "region", "variable", "unit", "year")


@dataclass
class Set(Item):
    ix_type = "set"


@dataclass
class Parameter(Item):
    ix_type = "par"


@dataclass
class Variable(Item):
    ix_type = "var"


@dataclass
class Equation(Item):
    ix_type = "equ"


#: Mapping from :py:`ix_type` strings to subclasses of :class:`.Item`.
CLASS = {
    c.ix_type: c
    for c in globals().values()
    if isinstance(c, type) and issubclass(c, Item)
}
