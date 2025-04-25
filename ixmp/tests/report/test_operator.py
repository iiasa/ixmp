import logging
import re
from functools import partial
from typing import cast

import genno
import pandas as pd
import pyam
import pytest
from genno import ComputationError, Computer
from genno.testing import assert_qty_equal
from pandas.testing import assert_frame_equal

from ixmp import Scenario, TimeSeries
from ixmp.model.dantzig import DATA as dantzig_data
from ixmp.report.operator import (
    from_url,
    get_ts,
    map_as_qty,
    remove_ts,
    store_ts,
    update_scenario,
)
from ixmp.testing import DATA as test_data
from ixmp.testing import assert_logs, make_dantzig

pytestmark = pytest.mark.usefixtures("parametrize_quantity_class")


def test_from_url(test_mp, request) -> None:
    ts = make_dantzig(test_mp, request=request)

    full_url = f"ixmp://{ts.platform.name}/{ts.url}"

    # Operator runs
    result = from_url(full_url)
    # Result is of the default class
    assert result.__class__ is TimeSeries
    # Same object was retrieved
    assert ts.url == result.url

    # Same, but specifying Scenario
    result = from_url(full_url, Scenario)
    assert result.__class__ is Scenario
    assert ts.url == result.url


# FIXME Not sure why introducing the Computer fails for IXMP4Backend
@pytest.mark.jdbc
def test_get_remove_ts(caplog, test_mp, request) -> None:
    ts = make_dantzig(test_mp, request=request)

    caplog.set_level(logging.INFO, "ixmp")

    # get_ts() runs
    result0 = get_ts(ts)
    assert_frame_equal(ts.timeseries(), result0)

    # Can be used through a Computer

    c = Computer()
    c.require_compat("ixmp.report.operator")
    c.add("scenario", ts)

    key = c.add("test1", "get_ts", "scenario", filters=dict(variable="GDP"))
    result1 = c.get(key)
    assert 3 == len(result1)

    # remove_ts() can be used through Computer
    key = c.add("test2", "remove_ts", "scenario", after=1964)

    # Task runs, logs
    with assert_logs(caplog, "Remove 5 of 5 (1964 <= year) rows of time series data"):
        c.get(key)

    # See comment above; only one row is removed
    assert 6 - 3 == len(ts.timeseries())

    # remove_ts() can be used directly
    remove_ts(ts)

    # All non-'meta' data were removed
    assert 3 == len(ts.timeseries())


def test_map_as_qty() -> None:
    b = ["b1", "b2", "b3", "b4"]
    input = pd.DataFrame(
        [["f1", "b1"], ["f1", "b2"], ["f2", "b3"]], columns=["foo", "bar"]
    )

    result = map_as_qty(input, b)

    exp = genno.Quantity(
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


# FIXME IXMP4Backend seems to return d with unexpected ordering
@pytest.mark.jdbc
def test_update_scenario(caplog, test_mp, request) -> None:
    scen = make_dantzig(test_mp, request=request)
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
    d = cast(pd.DataFrame, dantzig_data["d"])
    data = d.query("j == 'chicago'").assign(j="toronto")
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
    data = pd.concat([d, data]).reset_index(drop=True)
    data["value"] *= 2.0

    # Convert to a Quantity object and re-add
    q = genno.Quantity(data.set_index(["i", "j"])["value"], name="d", units="km")
    c.add("input", q)

    # Revise the task; the parameter name ('demand') is read from the Quantity
    c.add("test 2", (update_scenario, "target", "input"))

    # Trigger the computation
    with assert_logs(caplog, f"'d' ← {len(data)} rows", at_level=logging.INFO):
        c.get("test 2")

    # All the rows have been updated
    assert_frame_equal(scen.par("d"), data)


# FIXME Nots ure why this fails on IXMP4Backend
@pytest.mark.jdbc
def test_store_ts(request, caplog, test_mp) -> None:
    # Computer and target scenario
    c = Computer()

    # Target scenario
    model_name = __name__
    scenario_name = "test scenario"
    scen = Scenario(test_mp, model_name, scenario_name, version="new")
    scen.commit("Empty scenario")
    c.add("target", scen)

    # Add test data to the Computer: a pd.DataFrame
    input_1 = test_data[0].assign(variable="Foo")
    c.add("input 1", input_1)

    # A pyam.IamDataFrame
    input_2 = test_data[2050].assign(variable="Bar")
    c.add("input 2", pyam.IamDataFrame(input_2))

    # Expected results: same as input, but with the `model` and `scenario` columns
    # filled automatically.
    expected_1 = input_1.assign(model=model_name, scenario=scenario_name)
    expected_2 = input_2.assign(model=model_name, scenario=scenario_name)

    # Task to update the scenario with the data
    c.add("test 1", store_ts, "target", "input 1", "input 2")

    # Scenario starts empty of time series data
    assert 0 == len(scen.timeseries())

    # The computation runs successfully
    c.get("test 1")

    # All rows from both inputs are present
    assert len(input_1) + len(input_2) == len(scen.timeseries())

    # Input is stored exactly
    assert_frame_equal(expected_1, scen.timeseries(variable="Foo"))
    assert_frame_equal(expected_2, scen.timeseries(variable="Bar"))

    # Data with an unregistered region name
    c.add("input 3", test_data[0].assign(variable="Foo", region="Moon"))
    c.add("test 2", store_ts, "target", "input 3")

    # Succeeds with default strict=False
    caplog.clear()
    c.get("test 2")

    # A message is logged
    r = caplog.record_tuples[-1]
    assert (
        "ixmp.report.operator" == r[0]
        and logging.ERROR == r[1]
        and r[2].startswith("Failed with ValueError('region = Moon')")
    ), caplog.record_tuples

    caplog.clear()

    # with strict=True, the computation fails
    c.add("test 2", partial(store_ts, strict=True), "target", "input 3")
    with pytest.raises(
        ComputationError,
        match=re.compile("computing 'test 2' using:.*region = Moon", flags=re.DOTALL),
    ):
        c.get("test 2")
