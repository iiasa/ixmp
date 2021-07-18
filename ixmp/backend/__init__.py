from enum import IntFlag
from typing import Dict, List, Type, Union

#: Lists of field names for tuples returned by Backend API methods.
#:
#: The key "write_file" refers to the columns appearing in the CSV output from
#: :meth:`export_timeseries_data` when using :class:`.JDBCBackend`.
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

#: Partial dimensions for “IAMC format”.
IAMC_IDX: List[Union[str, int]] = ["model", "scenario", "region", "variable", "unit"]


#: Mapping from names to available backends. To register additional backends, add
#: entries to this dictionary.
BACKENDS: Dict[str, Type] = {}


class ItemType(IntFlag):
    """Type of data items in :class:`.TimeSeries` and :class:`.Scenario`."""

    # NB the docstring comments ('#:') are placed as they are to ensure the
    #    output is readable.

    TS = 1
    #: Time series data variable.
    T = TS

    SET = 2
    #: Set.
    S = SET

    PAR = 4
    #: Parameter.
    P = PAR

    VAR = 8
    #: Model variable.
    V = VAR

    EQU = 16
    #: Equation.
    E = EQU

    MODEL = SET + PAR + VAR + EQU
    #: All kinds of model-related data, i.e. :attr:`SET`, :attr:`PAR`,
    #: :attr:`VAR` and :attr:`EQU`.
    M = MODEL

    #: Model solution data, i.e. :attr:`VAR` and :attr:`EQU`.
    SOLUTION = VAR + EQU

    ALL = TS + MODEL
    #: All data, i.e. :attr:`MODEL` and :attr:`TS`.
    A = ALL
