# Methods are in alphabetical order
from itertools import product
from math import ceil
from typing import Any, List

import numpy as np
import pandas as pd
import pint
import xarray as xr

from ixmp import Platform, Scenario, TimeSeries
from ixmp.backend import IAMC_IDX
from ixmp.reporting import Quantity

#: Common (model name, scenario name) pairs for testing.
SCEN = {
    "dantzig": dict(model="canning problem", scenario="standard"),
    "h2g2": dict(model="Douglas Adams", scenario="Hitchhiker"),
}
models = SCEN

_MS: List[Any] = [models["dantzig"]["model"], models["dantzig"]["scenario"]]
HIST_DF = pd.DataFrame(
    [_MS + ["DantzigLand", "GDP", "USD", 850.0, 900.0, 950.0]],
    columns=IAMC_IDX + [2000, 2005, 2010],
)
INP_DF = pd.DataFrame(
    [_MS + ["DantzigLand", "Demand", "cases", 850.0, 900.0]],
    columns=IAMC_IDX + [2000, 2005],
)
TS_DF = (
    pd.concat([HIST_DF, INP_DF], sort=False)
    .sort_values(by="variable")
    .reset_index(drop=True)
)

# Test data used by test_scenario.py and test_timeseries.py.
#
# NB the columns are in a specific order; model and scenario come last in the data
#    returned by ixmp.
# TODO fix this; model and scenario should come first, matching the IAMC order.
DATA = {
    0: pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2010, 2020],
            value=[23.7, 23.8],
            model="model name",
            scenario="scenario name",
        )
    ),
    2010: pd.DataFrame.from_dict(
        {
            "region": ["World"],
            "variable": ["Testing"],
            "unit": ["???"],
            "2010": [23.7],
            "2020": [23.8],
        }
    ),
    2030: pd.DataFrame.from_dict(
        dict(
            region="World",
            variable=["Testing", "Testing", "Testing2"],
            unit="???",
            year=[2020, 2030, 2030],
            value=[24.8, 24.9, 25.1],
            model="model name",
            scenario="scenario name",
        )
    ),
    2050: pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2000, 2010, 2020, 2030, 2040, 2050],
            value=[21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
            model="model name",
            scenario="scenario name",
        )
    ),
    # NB the columns for geodata methods are inconsistent with those for time-series
    #    data
    "geo": pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="var1",
            subannual="Year",
            year=[2000, 2010, 2020],
            value=["test", "more-test", "2020-test"],
            unit="score",
            meta=0,
        )
    ),
    "timeseries": pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2010, 2020],
            value=[23.7, 23.8],
        )
    ),
}


def add_random_model_data(scenario, length):
    """Add a set and parameter with given *length* to *scenario*.

    The set is named 'random_set'. The parameter is named 'random_par', and has two
    dimensions indexed by 'random_set'.
    """
    set_data, par_data = random_model_data(length)
    scenario.init_set("random_set")
    scenario.add_set("random_set", set_data)
    scenario.init_par(
        "random_par",
        idx_sets=["random_set", "random_set"],
        idx_names=["random_set0", "random_set1"],
    )
    scenario.add_par("random_par", par_data)
    return len(par_data)


def add_test_data(scen: Scenario):
    # New sets
    t_foo = ["foo{}".format(i) for i in (1, 2, 3)]
    t_bar = ["bar{}".format(i) for i in (4, 5, 6)]
    t = t_foo + t_bar
    y = list(map(str, range(2000, 2051, 10)))

    # Add to scenario
    scen.init_set("t")
    scen.add_set("t", t)
    scen.init_set("y")
    scen.add_set("y", y)

    # Data
    ureg = pint.get_application_registry()
    x = Quantity(
        xr.DataArray(np.random.rand(len(t), len(y)), coords=[("t", t), ("y", y)]),
        units=ureg.kg,
    )

    # As a pd.DataFrame with units
    x_df = x.to_series().rename("value").reset_index()
    x_df["unit"] = "kg"

    scen.init_par("x", ["t", "y"])
    scen.add_par("x", x_df)

    return t, t_foo, t_bar, x


def make_dantzig(
    mp: Platform, solve: bool = False, quiet: bool = False, scheme="dantzig-gams"
) -> Scenario:
    """Return :class:`ixmp.Scenario` of Dantzig's canning/transport problem.

    Parameters
    ----------
    mp : .Platform
        Platform on which to create the scenario.
    solve : bool, optional
        If :obj:`True`. then solve the scenario before returning. Default :obj:`False`.
    quiet : bool, optional
        If :obj:`True`, suppress console output when solving.

    Returns
    -------
    .Scenario

    See also
    --------
    .DantzigModel
    """
    # add custom units and region for timeseries data
    try:
        mp.add_unit("USD/km")
    except Exception:
        # Unit already exists. Pending bugfix from zikolach
        pass
    mp.add_region("DantzigLand", "country")

    # Initialize a new Scenario, and use the DantzigModel class' initialize()
    # method to populate it
    annot = "Dantzig's transportation problem for illustration and testing"
    scen = Scenario(
        mp,
        **models["dantzig"],  # type: ignore [arg-type]
        version="new",
        annotation=annot,
        scheme=scheme,
        with_data=True,
    )

    # commit the scenario
    scen.commit("Import Dantzig's transport problem for testing.")

    # set this new scenario as the default version for the model/scenario name
    scen.set_as_default()

    if solve:
        # Solve the model using the GAMS code provided in the `tests` folder
        scen.solve(model="dantzig", case="transport_standard", quiet=quiet)

    # add timeseries data for testing `clone(keep_solution=False)`
    # and `remove_solution()`
    scen.check_out(timeseries_only=True)
    scen.add_timeseries(HIST_DF, meta=True)
    scen.add_timeseries(INP_DF)
    scen.commit("Import Dantzig's transport problem for testing.")

    return scen


def populate_test_platform(platform):
    """Populate `platform` with data for testing.

    Many of the tests in :mod:`ixmp.tests.core` depend on this set of data.

    The data consist of:

    - 3 versions of the Dantzig cannery/transport Scenario.

      - Version 2 is the default.
      - All have :obj:`HIST_DF` and :obj:`TS_DF` as time-series data.

    - 1 version of a TimeSeries with model name 'Douglas Adams' and scenario
      name 'Hitchhiker', containing 2 values.
    """
    s1 = make_dantzig(platform, solve=True, quiet=True)

    s2 = s1.clone()
    s2.set_as_default()

    s2.clone()

    s4 = TimeSeries(platform, **models["h2g2"], version="new")
    s4.add_timeseries(
        pd.DataFrame.from_dict(
            dict(
                region="World",
                variable="Testing",
                unit="???",
                year=[2010, 2020],
                value=[23.7, 23.8],
            )
        )
    )
    s4.commit("")
    s4.set_as_default()


def random_model_data(length):
    """Random (set, parameter) data with at least *length* elements.

    See also
    --------
    add_random_model_data
    """
    # Dimension size
    dim_len = ceil(length**0.5)
    set_data = list(str(i) for i in range(dim_len))

    # Revised length, possibly slightly higher than original
    length = dim_len**2

    par_data = pd.concat(
        [
            pd.DataFrame.from_dict(
                dict(region="World", value=np.random.rand(length), unit="GWa")
            ),
            pd.DataFrame(
                data=product(set_data, set_data), columns=["random_set0", "random_set1"]
            ),
        ],
        axis=1,
    )

    return set_data, par_data


def random_ts_data(length):
    """A :class:`pandas.DataFrame` of time series data with *length* rows.

    Suitable for passage to :meth:`TimeSeries.add_timeseries`.
    """
    return pd.DataFrame.from_dict(
        dict(
            region="World",
            variable=[f"foo|{i}" for i in range(int(length))],
            year=2020,
            value=np.random.rand(int(length)),
            unit="GWa",
        )
    )
