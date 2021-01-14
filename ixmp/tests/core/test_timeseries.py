from contextlib import contextmanager
from datetime import datetime, timedelta

import pandas as pd
import pytest
from numpy import testing as npt
from pandas.testing import assert_frame_equal

from ixmp import Scenario, TimeSeries
from ixmp.core import IAMC_IDX

# Test data.
# NB the columns are in a specific order; model and scenario come last in the
#    data returned by ixmp.
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
    # NB the columns for geodata methods are inconsistent with those for time-
    #    series data
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
test_args = ("Douglas Adams", "Hitchhiker")

# string columns for timeseries checks
IDX_COLS = ["region", "variable", "unit", "year"]

COLS_FOR_YEARLY_DATA = ["model", "scenario"] + IDX_COLS + ["value"]
COLS_WITH_SUBANNUAL = COLS_FOR_YEARLY_DATA.copy()
COLS_WITH_SUBANNUAL.insert(4, "subannual")


# Utility methods
def expected(df, ts):
    """Modify *df* with the 'model' and 'scenario' name from *ts."""
    return df.assign(model=ts.model, scenario=ts.scenario)


def wide(df):
    """Transform *df* from long to wide format."""
    other_cols = [c for c in df.columns if c not in ["year", "value"] + IAMC_IDX]
    return (
        df.pivot_table(index=IAMC_IDX + other_cols, columns="year", values="value")
        .reset_index()
        .rename_axis(columns=None)
    )


def prepare_scenario(mp, args_all):
    scen = TimeSeries(mp, *args_all, version="new", annotation="nk")
    scen.add_timeseries(DATA[2010])
    scen.commit("updating timeseries in IAMC format")
    scen = TimeSeries(mp, *args_all)
    return scen


@contextmanager
def transact(ts, condition=True, commit_message=""):
    """Context manager to wrap in a 'transaction'.

    If *condition* is True, the :class:`.TimeSeries`/:class:`.Scenario` *ts* is
    checked out *before* the block begins, and afterwards a commit is made with
    the *commit_message*. If *condition* is False, nothing occurs.
    """
    if condition:
        ts.check_out()
    try:
        yield ts
    finally:
        if condition:
            ts.commit(commit_message)


# Tests of ixmp.TimeSeries.
#
# Since Scenario is a subclass of TimeSeries, all TimeSeries functionality
# should work exactly the same way on Scenario instances. The *ts* fixture is
# parametrized to yield both TimeSeries and Scenario objects, so every test
# is run on each type.


@pytest.fixture(scope="function", params=[TimeSeries, Scenario])
def ts(request, mp):
    """An empty TimeSeries with a temporary name on the test_mp."""
    # Use a hash of the pytest node ID to avoid exceeding the maximum
    # length for a scenario name
    node = hash(request.node.nodeid.replace("/", " "))
    # Class of object to yield
    cls = request.param
    yield cls(mp, model=f"test-{node}", scenario="test", version="new")


# Initialize TimeSeries
@pytest.mark.parametrize("cls", [TimeSeries, Scenario])
def test_init(test_mp, cls):
    # Something other than a Platform as mp argument
    with pytest.raises(TypeError):
        cls(None, "model name", "scenario name")

    # Invalid version argument
    with pytest.raises(ValueError):
        cls(test_mp, "model name", "scenario name", version=3.4)


# TimeSeries properties
def test_default(mp, ts):
    # NB this is required before the is_default method can be used
    # FIXME should return False regardless
    ts.commit("")

    # Temporary TimeSeries is has not been set_as_default
    assert not ts.is_default()

    ts.set_as_default()
    assert ts.is_default()

    # NB TimeSeries cannot be cloned, so create a new one with the same
    #    name
    ts2 = TimeSeries(mp, ts.model, ts.scenario, version="new")
    ts2.commit("")
    ts2.set_as_default()

    assert ts2.is_default()
    # Original TimeSeries is no longer default
    assert not ts.is_default()


def test_run_id(ts):
    # New, un-committed TimeSeries has run_id of -1
    assert ts.run_id() == -1

    # The run ID is a positive integer
    ts.commit("")
    assert ts.run_id() > 0 and isinstance(ts.run_id(), int)


def test_last_update(ts):
    # New, un-committed TimeSeries has no last update date
    assert ts.last_update() is None

    ts.commit("")

    # After committing, last_update() returns a string
    last_update = ts.last_update()
    actual = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S.%f")
    assert abs(actual - datetime.now()) < timedelta(seconds=1)


@pytest.mark.parametrize("fmt", ["long", "wide"])
def test_add_timeseries(ts, fmt):
    data = DATA[0] if fmt == "long" else wide(DATA[0])

    # Data added
    ts.add_timeseries(data)
    ts.commit("")

    # Error: column 'unit' is missing
    with pytest.raises(ValueError):
        ts.add_timeseries(DATA[0].drop("unit", axis=1))


@pytest.mark.parametrize("fmt", ["long", "wide"])
def test_add_and_remove_timeseries_with_long_variable_name(ts, fmt):
    data = (DATA[0] if fmt == "long" else wide(DATA[0])).copy()
    data.variable = "x" * 256  # use long variable name (max 256 chars)

    # Data added
    ts.add_timeseries(data)
    ts.commit("")

    data = ts.timeseries()
    ts.check_out()
    ts.remove_timeseries(data)
    ts.commit("")


@pytest.mark.parametrize("fmt", ["long", "wide"])
def test_add_timeseries_with_extra_col(caplog, ts, fmt):
    _data = DATA[0].copy()
    _data["climate_model"] = [0, 1]
    data = _data if fmt == "long" else wide(_data)

    # check that extra column wasn't dropped by `wide(_data)
    assert "climate_model" in data.columns

    # Data added
    ts.add_timeseries(data)
    # TODO: add check that warning message is displayed
    ts.commit("")
    assert ["dropping index columns ['climate_model'] from data"] == [
        rec.message for rec in caplog.records
    ]


@pytest.mark.parametrize("fmt", ["long", "wide"])
def test_get(ts, fmt):
    data = DATA[0] if fmt == "long" else wide(DATA[0])

    ts.add_timeseries(data)
    ts.commit("")

    exp = expected(data, ts)
    args = {}

    if fmt == "wide":
        args["iamc"] = True

    # Data can be retrieved and has the expected value
    obs = ts.timeseries(**args)

    # NB this included check_like=True to be tolerant of JDBCBackend returning columns
    #    in unpredictable order. In pandas 1.2.0, this caused an exception; see
    #    pandas-dev/pandas#39168. Removed until the upstream bug is fixed.
    assert_frame_equal(exp, obs)


@pytest.mark.parametrize("fmt", ["long", "wide"])
def test_edit(mp, ts, fmt):
    """Tests that data can be overwritten."""
    data = expected(DATA[0], ts)
    all_data = [data.loc[0:0, :]]
    args = {}

    if fmt == "wide":
        data = wide(data)
        args["iamc"] = True

    ts.add_timeseries(data)
    ts.commit("initial data")

    # Overwrite existing data
    with transact(ts, commit_message="overwrite existing data"):
        ts.add_timeseries(data)

    df = expected(DATA[2030], ts)
    all_data.append(df)
    if fmt == "wide":
        df = wide(df)

    # Overwrite and add new values at once
    with transact(ts, commit_message="overwrite and add data"):
        ts.add_timeseries(df)

    # Close and re-open database
    mp.close_db()
    mp.open_db()

    # All four rows are retrieved
    exp = pd.concat(all_data).reset_index(drop=True)
    if fmt == "wide":
        exp = wide(exp)
    assert_frame_equal(exp, ts.timeseries(**args))


@pytest.mark.parametrize("cls", [TimeSeries, Scenario])
def test_edit_with_region_synonyms(mp, ts, cls):
    info = dict(model=ts.model, scenario=ts.scenario)
    exp = expected(DATA[0], ts)

    mp.add_region_synonym("Hell", "World")

    ts.add_timeseries(DATA[0])
    ts.commit("updating timeseries in IAMC format")

    ts = cls(mp, **info)
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out(timeseries_only=True)
    df = wide(DATA[2050]).replace("World", "Hell")
    ts.preload_timeseries()
    ts.add_timeseries(df)
    ts.commit("updating timeseries in IAMC format")

    exp = expected(DATA[2050], ts)
    assert_frame_equal(exp, ts.timeseries())


# TODO parametrize format as wide/long
# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "commit",
    [
        pytest.param(True),
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                reason="TimeSeries must be checked in to " "retrieve data."
            ),
        ),
    ],
)
def test_remove(mp, ts, commit):
    df = expected(DATA[2050], ts)

    ts.add_timeseries(DATA[2050])
    ts.commit("")

    if not commit:
        ts.check_out()

    assert_frame_equal(df, ts.timeseries())

    # Remove a single data point
    with transact(ts, commit):
        ts.remove_timeseries(df[df.year == 2010])

    # Expected data remains
    exp = df[df.year != 2010].reset_index(drop=True)
    assert_frame_equal(exp, ts.timeseries())

    # Remove two data points
    with transact(ts, commit):
        ts.remove_timeseries(df[df.year.isin([2030, 2050])])

    # Expected data remains
    exp = df[~df.year.isin([2010, 2030, 2050])].reset_index(drop=True)
    assert_frame_equal(exp, ts.timeseries())

    # Remove all remaining data
    with transact(ts, commit):
        ts.remove_timeseries(df)

    # Result is empty
    assert ts.timeseries().empty


def test_add_geodata(ts):
    # Empty TimeSeries includes no geodata
    assert_frame_equal(DATA["geo"].loc[[False, False, False]], ts.get_geodata())

    # Data can be added
    ts.add_geodata(DATA["geo"])
    ts.commit("")

    # Added data can be retrieved
    obs = ts.get_geodata().sort_values("year").reset_index(drop=True)
    assert_frame_equal(DATA["geo"], obs)


@pytest.mark.parametrize(
    "rows", [(1,), (1, 2), (0, 1, 2)], ids=["single", "multiple", "all"]
)
def test_remove_geodata(ts, rows):
    ts.add_geodata(DATA["geo"])
    ts.remove_geodata(DATA["geo"].take(rows))
    ts.commit("")

    mask = [i not in rows for i in range(len(DATA["geo"]))]
    # Expected rows have been removed
    exp = DATA["geo"].iloc[mask].reset_index(drop=True)
    obs = ts.get_geodata().sort_values("year").reset_index(drop=True)
    assert_frame_equal(exp, obs)


def test_get_timeseries(mp):
    scen = TimeSeries(mp, *test_args)
    assert_timeseries(scen)


def test_get_timeseries_iamc(mp):
    scen = TimeSeries(mp, *test_args)
    obs = scen.timeseries(region="World", variable="Testing", iamc=True)

    exp = (
        DATA["timeseries"]
        .pivot_table(index=["region", "variable", "unit"], columns="year")["value"]
        .reset_index()
    )
    exp["model"] = "Douglas Adams"
    exp["scenario"] = "Hitchhiker"

    npt.assert_array_equal(exp[IAMC_IDX], obs[IAMC_IDX])
    npt.assert_array_almost_equal(exp[2010], obs[2010])


def test_new_timeseries_as_year_value(test_mp):
    scen = TimeSeries(test_mp, *test_args, version="new", annotation="fo")
    scen.add_timeseries(DATA["timeseries"])
    scen.commit("importing a testing timeseries")
    assert_timeseries(scen)


def test_new_timeseries_as_iamc(test_mp):
    scen = TimeSeries(test_mp, *test_args, version="new", annotation="fo")
    scen.add_timeseries(DATA["timeseries"].pivot_table(values="value", index=IDX_COLS))
    scen.commit("importing a testing timeseries")

    # compare returned dataframe - default behaviour set to 'auto'
    assert_timeseries(scen)
    # test behaviour of 'auto' explicitly
    assert_timeseries(scen, subannual="auto")
    # test behaviour of 'False' explicitly
    assert_timeseries(scen, subannual=False)

    # test behaviour of 'True' explicitly
    exp = DATA["timeseries"].pivot_table(values="value", index=IDX_COLS).reset_index()
    exp["model"] = "Douglas Adams"
    exp["scenario"] = "Hitchhiker"
    exp["subannual"] = "Year"
    assert_timeseries(scen, exp=exp, cols=COLS_WITH_SUBANNUAL, subannual=True)


def assert_timeseries(scen, exp=DATA["timeseries"], cols=None, subannual=None):
    """Asserts scenario timeseries are similar to expected

    Compares region, variable, unit, year and subannual (if available).
    By default it assumes that datasets are sorted in correct order to compare.

    :param scen:    scenario object
    :param exp:     expected timeseries data
    :param cols:    (optional) column list to sort by
    :param subannual:   passed to 'timeseries()'
    """
    if subannual is None:
        obs = scen.timeseries(region="World")
    else:
        obs = scen.timeseries(region="World", subannual=subannual)

    if cols is not None:
        obs = obs.sort_values(by=cols)
        exp = exp.sort_values(by=cols)
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])
    if "subannual" in exp.columns:
        npt.assert_array_equal(exp[["subannual"]], obs[["subannual"]])


def test_new_timeseries_error(test_mp):
    scen = TimeSeries(test_mp, *test_args, version="new", annotation="fo")
    df = {"year": [2010, 2020], "value": [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df["region"] = "World"
    df["variable"] = "Testing"
    # column `unit` is missing
    pytest.raises(ValueError, scen.add_timeseries, df)


def test_timeseries_edit(mp):
    scen = TimeSeries(mp, *test_args)
    df = {
        "region": ["World"] * 2,
        "variable": ["Testing"] * 2,
        "unit": ["???", "???"],
        "year": [2010, 2020],
        "value": [23.7, 23.8],
    }
    exp = pd.DataFrame.from_dict(df)
    obs = scen.timeseries()
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])

    scen.check_out(timeseries_only=True)
    df = {
        "region": ["World"] * 2,
        "variable": ["Testing"] * 2,
        "unit": ["???", "???"],
        "year": [2010, 2020],
        "value": [23.7, 23.8],
    }
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit("testing of editing timeseries (same years)")

    scen.check_out(timeseries_only=True)
    df = {
        "region": ["World"] * 3,
        "variable": ["Testing", "Testing", "Testing2"],
        "unit": ["???", "???", "???"],
        "year": [2020, 2030, 2030],
        "value": [24.8, 24.9, 25.1],
    }
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit("testing of editing timeseries (other years)")
    mp.close_db()

    mp.open_db()
    scen = TimeSeries(mp, *test_args)
    obs = scen.timeseries().sort_values(by=["year"])
    df = df.append(exp.loc[0]).sort_values(by=["year"])
    npt.assert_array_equal(df[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(df["value"], obs["value"])


def test_timeseries_edit_iamc(mp):
    args_all = ("Douglas Adams 1", "test_remove_all")
    scen = prepare_scenario(mp, args_all)
    obs = scen.timeseries()
    exp = pd.DataFrame.from_dict(
        {
            "region": ["World", "World"],
            "variable": ["Testing", "Testing"],
            "unit": ["???", "???"],
            "year": [2010, 2020],
            "value": [23.7, 23.8],
        }
    )
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])

    scen.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict(
        {
            "region": ["World"],
            "variable": ["Testing"],
            "unit": ["???"],
            "2000": [21.7],
            "2010": [22.7],
            "2020": [23.7],
            "2030": [24.7],
            "2040": [25.7],
            "2050": [25.8],
        }
    )
    scen.add_timeseries(df)
    scen.commit("updating timeseries in IAMC format")

    exp = pd.DataFrame.from_dict(
        {
            "region": ["World"] * 6,
            "variable": ["Testing"] * 6,
            "unit": ["???"] * 6,
            "year": [2000, 2010, 2020, 2030, 2040, 2050],
            "value": [21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
        }
    )
    obs = scen.timeseries()
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])
    mp.close_db()


def test_timeseries_edit_with_region_synonyms(mp):
    args_all = ("Douglas Adams 1", "test_remove_all")
    mp.add_region_synonym("Hell", "World")
    scen = prepare_scenario(mp, args_all)
    obs = scen.timeseries()
    exp = pd.DataFrame.from_dict(
        {
            "region": ["World"] * 2,
            "variable": ["Testing"] * 2,
            "unit": ["???", "???"],
            "year": [2010, 2020],
            "value": [23.7, 23.8],
        }
    )
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])

    scen.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict(
        {
            "region": ["Hell"],
            "variable": ["Testing"],
            "unit": ["???"],
            "2000": [21.7],
            "2010": [22.7],
            "2020": [23.7],
            "2030": [24.7],
            "2040": [25.7],
            "2050": [25.8],
        }
    )
    scen.preload_timeseries()
    scen.add_timeseries(df)
    scen.commit("updating timeseries in IAMC format")

    exp = pd.DataFrame.from_dict(
        {
            "region": ["World"] * 6,
            "variable": ["Testing"] * 6,
            "unit": ["???"] * 6,
            "year": [2000, 2010, 2020, 2030, 2040, 2050],
            "value": [21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
        }
    )
    obs = scen.timeseries()
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])
    mp.close_db()


def test_timeseries_remove_single_entry(mp):
    args_single = ("Douglas Adams", "test_remove_single")

    scen = Scenario(mp, *args_single, version="new", annotation="fo")
    scen.add_timeseries(DATA["timeseries"].pivot_table(values="value", index=IDX_COLS))
    scen.commit("importing a testing timeseries")

    scen = Scenario(mp, *args_single)
    assert_timeseries(scen, DATA["timeseries"])

    scen.check_out()
    scen.remove_timeseries(DATA["timeseries"][DATA["timeseries"].year == 2010])
    scen.commit("testing for removing a single timeseries data point")

    exp = DATA["timeseries"][DATA["timeseries"].year == 2020]
    assert_timeseries(scen, exp)


def test_timeseries_remove_all_data(mp):
    args_all = ("Douglas Adams", "test_remove_all")

    scen = Scenario(mp, *args_all, version="new", annotation="fo")
    scen.add_timeseries(DATA["timeseries"].pivot_table(values="value", index=IDX_COLS))
    scen.commit("importing a testing timeseries")

    scen = Scenario(mp, *args_all)
    assert_timeseries(scen, DATA["timeseries"])

    exp = DATA["timeseries"].copy()
    exp["variable"] = "Testing2"

    scen.check_out()
    scen.add_timeseries(exp)
    scen.remove_timeseries(DATA["timeseries"])
    scen.commit("testing for removing a full timeseries row")

    assert scen.timeseries(region="World", variable="Testing").empty
    assert_timeseries(scen, exp)


def test_new_subannual_timeseries_as_iamc(mp):
    mp.add_timeslice("Summer", "Season", 1.0 / 4)
    scen = TimeSeries(mp, *test_args, version="new", annotation="fo")
    timeseries = DATA["timeseries"].pivot_table(values="value", index=IDX_COLS)
    scen.add_timeseries(timeseries)
    scen.commit("adding yearly data")

    # add subannual timeseries data
    ts_summer = timeseries.copy()
    ts_summer["subannual"] = "Summer"
    scen.check_out()
    scen.add_timeseries(ts_summer)
    scen.commit("adding subannual data")

    # generate expected dataframe+
    ts_year = timeseries.copy()
    ts_year["subannual"] = "Year"
    exp = pd.concat([ts_year, ts_summer]).reset_index()
    exp["model"] = "Douglas Adams"
    exp["scenario"] = "Hitchhiker"

    # compare returned dataframe - default behaviour set to 'auto'
    assert_timeseries(scen, exp=exp[COLS_WITH_SUBANNUAL], cols=COLS_WITH_SUBANNUAL)
    # test behaviour of 'auto' explicitly
    assert_timeseries(
        scen, exp=exp[COLS_WITH_SUBANNUAL], cols=COLS_WITH_SUBANNUAL, subannual="auto"
    )
    # test behaviour of 'True' explicitly
    assert_timeseries(
        scen, exp=exp[COLS_WITH_SUBANNUAL], cols=COLS_WITH_SUBANNUAL, subannual=True
    )
    # setting False raises an error because subannual data exists
    pytest.raises(ValueError, scen.timeseries, subannual=False)


def test_fetch_empty_geodata(mp):
    scen = TimeSeries(mp, *test_args, version="new", annotation="fo")
    empty = scen.get_geodata()
    assert_geodata(empty, DATA["geo"].loc[[False, False, False]])


def test_remove_multiple_geodata(mp):
    scen = TimeSeries(mp, *test_args, version="new", annotation="fo")
    scen.add_geodata(DATA["geo"])
    row = DATA["geo"].loc[[False, True, True]]
    scen.remove_geodata(row)
    scen.commit("adding geodata (references to map layers)")
    assert_geodata(scen.get_geodata(), DATA["geo"].loc[[True, False, False]])


def assert_geodata(obs, exp):
    obs = obs.sort_values("year")
    exp = exp.sort_values("year")
    for column in obs.columns:
        npt.assert_array_equal(exp.get(column), obs.get(column))
