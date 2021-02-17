import logging
from functools import partial

import pandas as pd
import pytest
from genno import Computer, Quantity
from genno.testing import assert_qty_equal
from pandas.testing import assert_frame_equal

from ixmp.model.dantzig import DATA
from ixmp.reporting.computations import map_as_qty, update_scenario
from ixmp.testing import assert_logs, make_dantzig

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


def test_update_scenario(request, caplog, test_mp):
    scen = make_dantzig(test_mp)
    scen.check_out()
    scen.add_set("j", "toronto")
    scen.commit("Add j=toronto")

    # Number of rows in the 'd' parameter
    N_before = len(scen.par("d"))
    assert 6 == N_before

    # A Computer used as calculation engine
    c = Computer()

    # Target Scenario for updating data
    c.add("target", scen)

    # Create a pd.DataFrame suitable for Scenario.add_par()
    data = DATA["d"].query("j == 'chicago'").assign(j="toronto")
    data["value"] += 1.0

    # Add to the Reporter
    c.add("input", data)

    # Task to update the scenario with the data
    c.add("test 1", (partial(update_scenario, params=["d"]), "target", "input"))

    # Trigger the computation that results in data being added
    with assert_logs(caplog, f"'d' ← {len(data)} rows", at_level=logging.INFO):
        # Returns nothing
        assert c.get("test 1") is None

    # Rows were added to the parameter
    assert len(scen.par("d")) == N_before + len(data)

    # Modify the data
    data = pd.concat([DATA["d"], data]).reset_index(drop=True)
    data["value"] *= 2.0

    # Convert to a Quantity object and re-add
    q = Quantity(data.set_index(["i", "j"])["value"], name="d", units="km")
    c.add("input", q)

    # Revise the task; the parameter name ('demand') is read from the Quantity
    c.add("test 2", (update_scenario, "target", "input"))

    # Trigger the computation
    with assert_logs(caplog, f"'d' ← {len(data)} rows", at_level=logging.INFO):
        c.get("test 2")

    # All the rows have been updated
    assert_frame_equal(scen.par("d"), data)
