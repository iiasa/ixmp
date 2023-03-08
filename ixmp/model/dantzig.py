from collections import ChainMap
from functools import lru_cache
from pathlib import Path
from typing import Dict

import pandas as pd

from ixmp.utils import maybe_check_out, maybe_commit, update_par

from .gams import GAMSModel
from .pyomo import PyomoModel

ITEMS: Dict[str, dict] = {
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

DATA = {
    "i": ["seattle", "san-diego"],
    "j": ["new-york", "chicago", "topeka"],
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
    "f": (90.0, "USD/km"),
}


class DantzigModel:
    name = "dantzig"

    @classmethod
    def initialize(cls, scenario, with_data=False):
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
        scenario.add_set("i", DATA["i"])
        scenario.add_set("j", DATA["j"])

        # Add parameter values
        update_par(scenario, "a", DATA["a"])
        update_par(scenario, "b", DATA["b"])
        update_par(scenario, "d", DATA["d"])

        # TODO avoid overwriting the existing value
        scenario.change_scalar("f", *DATA["f"])

        maybe_commit(scenario, checkout, f"{cls.__name__}.initialize")


class DantzigGAMSModel(DantzigModel, GAMSModel):
    """Dantzig's cannery/transport problem as a :class:`GAMSModel`.

    Provided for testing :mod:`ixmp` code.
    """

    name = "dantzig-gams"

    defaults = ChainMap(
        {
            # Override keys from GAMSModel
            "model_file": Path(__file__).with_name("dantzig.gms"),
        },
        GAMSModel.defaults,
    )


def supply(model, i):
    return sum(model.x[i, j] for j in model.j) <= model.a[i]


def demand(model, j):
    return sum(model.x[i, j] for i in model.i) >= model.b[j]


@lru_cache()
def c(model, i, j):
    return model.f * model.d[i, j] / 1000


def cost(model):
    return sum(c(model, i, j) * model.x[i, j] for i in model.i for j in model.j)


class DantzigPyomoModel(DantzigModel, PyomoModel):
    """Dantzig's cannery/transport problem as a :class:`PyomoModel`.

    Provided for testing :mod:`ixmp` code.
    """

    name = "dantzig-pyomo"
    items = ITEMS
    equation = dict(
        supply=supply,
        demand=demand,
        cost=cost,
    )
    component_kwargs = dict(
        x=dict(bounds=(0.0, None)),
    )
    objective = "cost"
