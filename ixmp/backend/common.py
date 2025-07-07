"""Common structures shared by all backends."""

from enum import IntFlag, auto
from typing import TYPE_CHECKING, Union

# Compatibility with Python 3.9
# TODO Use "from typing import …" when dropping support for Python 3.9
from typing_extensions import TypeGuard

if TYPE_CHECKING:
    from ixmp.types import ModelItemType

#: Lists of field names for tuples returned by Backend API methods.
#:
#: The key "write_file" refers to the columns appearing in the CSV output from
#: :meth:`.export_timeseries_data` when using :class:`.JDBCBackend`.
#:
#: .. todo:: Make this consistent with other dimension orders and with :data:`IAMC_IDX`.
FIELDS = {
    "get_nodes": ("region", "mapped_to", "parent", "hierarchy"),
    "get_timeslices": ("name", "category", "duration"),
    "get_scenarios": (
        "model",
        "scenario",
        "scheme",
        "is_default",
        "is_locked",
        "cre_user",
        "cre_date",
        "upd_user",
        "upd_date",
        "lock_user",
        "lock_date",
        "annotation",
        "version",
    ),
    "ts_get": ("region", "variable", "unit", "subannual", "year", "value"),
    "ts_get_geo": ("region", "variable", "subannual", "year", "value", "unit", "meta"),
    "write_file": (
        "MODEL",
        "SCENARIO",
        "VERSION",
        "VARIABLE",
        "UNIT",
        "REGION",
        "META",
        "SUBANNUAL",
        "YEAR",
        "VALUE",
    ),
}

#: Partial list of dimensions for the IAMC data structure, or “IAMC format”. This omits
#: "year" and "subannual" which appear in some variants of the structure, but not in
#: others.
IAMC_IDX: list[Union[str, int]] = ["model", "scenario", "region", "variable", "unit"]


class ItemType(IntFlag):
    """Type of data items in :class:`.ixmp.TimeSeries` and :class:`.ixmp.Scenario`."""

    #: Time series data variable.
    TS = auto()

    #: Set.
    SET = auto()

    #: Parameter.
    PAR = auto()

    #: Model variable.
    VAR = auto()

    #: Equation.
    EQU = auto()

    #: All kinds of model-related data, i.e. :attr:`SET`, :attr:`PAR`, :attr:`VAR` and
    # :attr:`EQU`.
    MODEL = SET | PAR | VAR | EQU

    #: Model solution data, i.e. :attr:`VAR` and :attr:`EQU`.
    SOLUTION = VAR | EQU

    #: All data, i.e. :attr:`MODEL` and :attr:`TS`.
    ALL = TS | MODEL

    @property
    def name(self) -> str:
        return str(super().name)

    @staticmethod
    def is_model_data(value: "ItemType") -> TypeGuard["ModelItemType"]:
        return bool(value & ItemType.MODEL)
