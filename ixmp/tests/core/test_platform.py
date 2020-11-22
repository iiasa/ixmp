"""Test functionality of ixmp.Platform."""
from sys import getrefcount
from weakref import getweakrefcount

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest import raises

import ixmp


def test_init():
    with pytest.raises(
        ValueError, match="backend class 'foo' not among " r"\['jdbc'\]"
    ):
        ixmp.Platform(backend="foo")


@pytest.fixture
def log_level_mp(test_mp):
    """A fixture that preserves the log level of *test_mp*."""
    tmp = test_mp.get_log_level()
    yield test_mp
    test_mp.set_log_level(tmp)


@pytest.mark.parametrize(
    "level, exc",
    [
        ("CRITICAL", None),
        ("ERROR", None),
        ("WARNING", None),
        ("INFO", None),
        ("DEBUG", None),
        ("NOTSET", None),
        # An unknown string fails
        ("FOO", ValueError),
    ],
)
def test_log_level(log_level_mp, level, exc):
    """Log level can be set and retrieved."""
    if exc is None:
        log_level_mp.set_log_level(level)
        assert log_level_mp.get_log_level() == level
    else:
        with pytest.raises(exc):
            log_level_mp.set_log_level(level)


def test_scenario_list(mp):
    scenario = mp.scenario_list(model="Douglas Adams")["scenario"]
    assert scenario[0] == "Hitchhiker"


def test_export_timeseries_data(mp, tmp_path):
    path = tmp_path / "export.csv"
    mp.export_timeseries_data(path, model="Douglas Adams", unit="???", region="World")

    columns = [
        "model",
        "scenario",
        "version",
        "variable",
        "unit",
        "region",
        "meta",
        "subannual",
        "year",
        "value",
    ]
    csv_dtype = dict(
        model=str,
        scenario=str,
        version=int,
        variable=str,
        unit=str,
        region=str,
        year=int,
        subannual=str,
        meta=int,
        value=float,
    )
    obs = pd.read_csv(path, index_col=False, header=0, names=columns, dtype=csv_dtype)
    exp = pd.DataFrame(
        [
            [
                "Douglas Adams",
                "Hitchhiker",
                1,
                "Testing",
                "???",
                "World",
                0,
                "Year",
                "2010",
                23.7,
            ],
            [
                "Douglas Adams",
                "Hitchhiker",
                1,
                "Testing",
                "???",
                "World",
                0,
                "Year",
                "2020",
                23.8,
            ],
        ],
        columns=[
            "model",
            "scenario",
            "version",
            "variable",
            "unit",
            "region",
            "meta",
            "subannual",
            "year",
            "value",
        ],
    ).astype(csv_dtype)
    assert_frame_equal(obs, exp)


def test_export_ts_wrong_params(mp, tmp_path):
    """Platform.export_timeseries_data to raise error with wrong parameters."""
    path = tmp_path / "export.csv"
    with raises(ValueError, match="Invalid arguments"):
        mp.export_timeseries_data(
            path,
            model="Douglas Adams",
            unit="???",
            region="World",
            export_all_runs=True,
        )


def test_export_ts_of_all_runs(mp, tmp_path):
    """Export timeseries of all runs."""
    path = tmp_path / "export.csv"

    # Add a new version of a run
    scen = ixmp.TimeSeries(
        mp, "Douglas Adams", "Hitchhiker", version="new", annotation="fo"
    )
    timeseries = pd.DataFrame.from_dict(
        dict(
            region="World",
            variable="Testing",
            unit="???",
            year=[2010, 2020],
            value=[24.7, 24.8],
        )
    )
    scen.add_timeseries(timeseries)
    scen.commit("create a new version")
    scen.set_as_default()

    columns = [
        "model",
        "scenario",
        "version",
        "variable",
        "unit",
        "region",
        "meta",
        "subannual",
        "year",
        "value",
    ]
    csv_dtype = dict(
        model=str,
        scenario=str,
        version=int,
        variable=str,
        unit=str,
        region=str,
        year=int,
        subannual=str,
        meta=int,
        value=float,
    )

    # Export all default model+scenario runs
    mp.export_timeseries_data(
        path, unit="???", region="World", default=True, export_all_runs=True
    )
    obs = pd.read_csv(path, index_col=False, header=0, names=columns, dtype=csv_dtype)
    assert len(obs.model) == 2
    assert pd.Series.all(obs.value == timeseries.value)

    # Export all model+scenario run versions (including non-default)
    mp.export_timeseries_data(
        path, unit="???", region="World", default=False, export_all_runs=True
    )
    obs = pd.read_csv(path, index_col=False, header=0, names=columns, dtype=csv_dtype)
    assert len(obs.model) == 4


def test_export_timeseries_data_empty(mp, tmp_path):
    """Dont export data if given models/scenarios do not have any runs."""
    path = tmp_path / "export.csv"
    model = "model-no-run"
    scenario = "scenario-no-run"
    mp.add_model_name(model)
    mp.add_scenario_name(scenario)

    mp.export_timeseries_data(path, model=model, unit="???", region="World")

    columns = [
        "model",
        "scenario",
        "version",
        "variable",
        "unit",
        "region",
        "meta",
        "subannual",
        "year",
        "value",
    ]
    csv_dtype = dict(
        model=str,
        scenario=str,
        version=int,
        variable=str,
        unit=str,
        region=str,
        year=int,
        subannual=str,
        meta=int,
        value=float,
    )
    obs = pd.read_csv(path, index_col=False, header=0, names=columns, dtype=csv_dtype)
    assert len(obs.model) == 0


def test_unit_list(test_mp):
    units = test_mp.units()
    assert ("cases" in units) is True


def test_add_unit(test_mp):
    test_mp.add_unit("test", "just testing")


def test_regions(test_mp):
    regions = test_mp.regions()

    # Result has the expected columns
    columns = ["region", "mapped_to", "parent", "hierarchy"]
    assert all(regions.columns == columns)

    # One row is as expected
    obs = regions[regions.region == "World"]
    assert all([list(obs.loc[0]) == ["World", None, "World", "common"]])


def test_add_region(test_mp):
    # Region can be added
    test_mp.add_region("foo", "bar", "World")

    # Region can be retrieved
    regions = test_mp.regions()
    obs = regions[regions["region"] == "foo"].reset_index(drop=True)
    assert all([list(obs.loc[0]) == ["foo", None, "World", "bar"]])


def test_add_region_synonym(test_mp):
    test_mp.add_region("foo", "bar", "World")
    test_mp.add_region_synonym("foo2", "foo")
    regions = test_mp.regions()
    obs = regions[regions.region.isin(["foo", "foo2"])].reset_index(drop=True)

    exp = pd.DataFrame(
        [
            ["foo", None, "World", "bar"],
            ["foo2", "foo", "World", "bar"],
        ],
        columns=["region", "mapped_to", "parent", "hierarchy"],
    )
    assert_frame_equal(obs, exp)


def test_timeslices(test_mp):
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == "Common"]
    # result has all attributes of time slice
    assert all(obs.columns == ["name", "category", "duration"])
    # result contains pre-defined YEAR time slice
    assert all([list(obs.iloc[0]) == ["Year", "Common", 1.0]])


def test_add_timeslice(test_mp):
    test_mp.add_timeslice("January, 1st", "Days", 1.0 / 366)
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == "Days"]
    # return only added time slice
    assert len(obs) == 1
    # returned time slice attributes have expected values
    assert all([list(obs.iloc[0]) == ["January, 1st", "Days", 1.0 / 366]])


def test_add_timeslice_duplicate_raise(test_mp):
    test_mp.add_timeslice("foo_slice", "foo_category", 0.2)
    # adding same name with different duration raises an error
    with raises(
        ValueError, match="timeslice `foo_slice` already defined with " "duration 0.2"
    ):
        test_mp.add_timeslice("foo_slice", "bar_category", 0.3)


def test_weakref():
    """Weak references allow Platforms to be del'd while Scenarios live."""
    mp = ixmp.Platform(
        backend="jdbc",
        driver="hsqldb",
        url="jdbc:hsqldb:mem:test_weakref",
    )

    # There is one reference to the Platform, and zero weak references
    assert getrefcount(mp) - 1 == 1
    assert getweakrefcount(mp) == 0

    # Create a single Scenario
    s = ixmp.Scenario(mp, "foo", "bar", version="new")

    # Still one reference to the Platform
    assert getrefcount(mp) - 1 == 1
    # …but additionally one weak reference
    assert getweakrefcount(mp) == 1

    # Make a local reference to the backend
    backend = mp._backend

    # Delete the Platform. Note that this only has an effect if there are no
    # existing references to it
    del mp

    # s.platform is a dead weak reference, so it can't be accessed
    with pytest.raises(ReferenceError):
        s.platform._backend

    # There is only one remaining reference to the backend: the *backend* name
    # in the local scope
    assert getrefcount(backend) - 1 == 1

    # The backend is garbage-collected at this point

    # The Scenario object still lives, but can't be used for anything
    assert s.model == "foo"

    # *s* is garbage-collected at this point


def test_add_model_name(mp):
    mp.add_model_name("new_model_name")
    assert "new_model_name" in mp.get_model_names()


def test_add_scenario_name(mp):
    mp.add_scenario_name("new_scenario_name")
    assert "new_scenario_name" in mp.get_scenario_names()
