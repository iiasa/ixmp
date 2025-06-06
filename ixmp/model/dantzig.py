from collections import ChainMap
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ixmp.types import InitializeItemsKwargs
from ixmp.util import maybe_check_out, maybe_commit, update_par

from .gams import GAMSModel

if TYPE_CHECKING:
    from ixmp.core.scenario import Scenario


ITEMS: dict[str, InitializeItemsKwargs] = {
    # Plants
    "i": dict(ix_type="set"),
    # Markets
    "j": dict(ix_type="set"),
    # Capacity of plant i in cases
    "a": dict(ix_type="par", idx_sets=["i"]),
    # Demand at market j in cases
    "b": dict(ix_type="par", idx_sets=["j"]),
    # Distance between plant i and market j
    "d": dict(ix_type="par", idx_sets=["i", "j"]),
    # Transport cost per case per 1000 miles
    "f": dict(ix_type="par", idx_sets=None),
    # Decision variables and equations
    "x": dict(ix_type="var", idx_sets=["i", "j"]),
    "z": dict(ix_type="var", idx_sets=None),
    "cost": dict(ix_type="equ", idx_sets=None),
    "demand": dict(ix_type="equ", idx_sets=["j"]),
    "supply": dict(ix_type="equ", idx_sets=["i"]),
}

INDEXSET_DATA: dict[str, list[str]] = {
    "i": ["seattle", "san-diego"],
    "j": ["new-york", "chicago", "topeka"],
}

TABLE_DATA: dict[str, pd.DataFrame] = {
    "a": pd.DataFrame(
        [
            ["seattle", 350, "cases"],
            ["san-diego", 600, "cases"],
        ],
        columns="i value unit".split(),
    ),
    "b": pd.DataFrame(
        [
            ["new-york", 325, "cases"],
            ["chicago", 300, "cases"],
            ["topeka", 275, "cases"],
        ],
        columns="j value unit".split(),
    ),
    "d": pd.DataFrame(
        [
            ["seattle", "new-york", 2.5, "km"],
            ["seattle", "chicago", 1.7, "km"],
            ["seattle", "topeka", 1.8, "km"],
            ["san-diego", "new-york", 2.5, "km"],
            ["san-diego", "chicago", 1.8, "km"],
            ["san-diego", "topeka", 1.4, "km"],
        ],
        columns="i j value unit".split(),
    ),
}

SCALAR_DATA: dict[str, tuple[float, str]] = {"f": (90.0, "USD/km")}


class DantzigModel(GAMSModel):
    """Dantzig's cannery/transport problem as a :class:`.GAMSModel`.

    Provided for testing :mod:`ixmp` code.
    """

    name = "dantzig"

    defaults = ChainMap(
        {
            # Override keys from GAMSModel
            "model_file": Path(__file__).with_name("dantzig.gms"),
        },
        GAMSModel.defaults,
    )

    @classmethod
    def initialize(cls, scenario: "Scenario", with_data: bool = False) -> None:
        """Initialize the problem.

        If *with_data* is :obj:`True` (default: :obj:`False`), the set and parameter
        values from the original problem are also populated. Otherwise, the sets and
        parameters are left empty.
        """
        # Initialize the ixmp items
        cls.initialize_items(scenario, ITEMS)

        if not with_data:
            return

        checkout = maybe_check_out(scenario)

        # Add set elements
        scenario.add_set("i", INDEXSET_DATA["i"])
        scenario.add_set("j", INDEXSET_DATA["j"])

        # Add parameter values
        update_par(scenario, "a", TABLE_DATA["a"])
        update_par(scenario, "b", TABLE_DATA["b"])
        update_par(scenario, "d", TABLE_DATA["d"])

        # TODO avoid overwriting the existing value
        scenario.change_scalar("f", *SCALAR_DATA["f"])

        maybe_commit(scenario, checkout, f"{cls.__name__}.initialize")
