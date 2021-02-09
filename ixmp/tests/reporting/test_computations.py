import logging
from functools import partial

from genno import Quantity
from genno.testing import assert_qty_equal
import pandas as pd
import pytest

from ixmp import Scenario, Reporter
from ixmp.reporting.computations import map_as_qty, update_scenario
from ixmp.testing import assert_logs

pytestmark = pytest.mark.usefixtures("parametrize_quantity_class")


def test_map_as_qty():
    b = ["b1", "b2", "b3", "b4"]
    input = pd.DataFrame(
        [["f1", "b1"], ["f1", "b2"], ["f2", "b3"]], columns=["foo", "bar"]
    )

    result = map_as_qty(input, b)

    exp = Quantity(
        pd.DataFrame(
            [
                ["f1", "b1", 1],
                ["f1", "b2", 1],
                ["f2", "b3", 1],
                ["all", "b1", 1],
                ["all", "b2", 1],
                ["all", "b3", 1],
                ["all", "b4", 1],
            ],
            columns=["foo", "bar", "value"],
        ).set_index(["foo", "bar"])
    )

    assert_qty_equal(exp, result)


@pytest.mark.skip(reason="TODO update to use only ixmp features")
def test_update_scenario(request, caplog, test_mp):
    scen = Scenario(test_mp, model="update_scenario", scenario="update_scenario")

    # Number of rows in the 'demand' parameter
    N_before = len(scen.par("demand"))

    # A Reporter used as calculation engine
    calc = Reporter()

    # Target Scenario for updating data
    calc.add("target", scen)

    # Create a pd.DataFrame suitable for Scenario.add_par()
    units = "GWa"
    demand = make_df(
        "demand",
        node="World",
        commodity="electr",
        level="secondary",
        year=ScenarioInfo(scen).Y[:10],
        time="year",
        value=1.0,
        unit=units,
    )

    # Add to the Reporter
    calc.add("input", demand)

    # Task to update the scenario with the data
    calc.add("test 1", (partial(update_scenario, params=["demand"]), "target", "input"))

    # Trigger the computation that results in data being added
    with assert_logs(caplog, "'demand' ← 10 rows", at_level=logging.DEBUG):
        # Returns nothing
        assert calc.get("test 1") is None

    # Rows were added to the parameter
    assert len(scen.par("demand")) == N_before + len(demand)

    # Modify the data
    demand["value"] = 2.0
    demand = demand.iloc[:5]
    # Convert to a Quantity object
    input = Quantity(
        demand.set_index("node commodity level year time".split())["value"],
        name="demand",
        units=units,
    )
    # Re-add
    calc.add("input", input)

    # Revise the task; the parameter name ('demand')
    calc.add("test 2", (update_scenario, "target", "input"))

    # Trigger the computation
    with assert_logs(caplog, "'demand' ← 5 rows"):
        calc.get("test 2")

    # Only half the rows have been updated
    assert scen.par("demand")["value"].mean() == 1.5
