"""Tests of :class:`ixmp.Platform`."""

import logging
import re
from collections.abc import Generator
from pathlib import Path
from sys import getrefcount
from typing import TYPE_CHECKING, Any, Literal
from weakref import getweakrefcount

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest import raises

import ixmp
from ixmp.backend.common import FIELDS
from ixmp.testing import DATA, assert_logs, min_ixmp4_version, models

if TYPE_CHECKING:
    pass


@pytest.mark.usefixtures("tmp_env")
class TestPlatform:
    def test_init0(self) -> None:
        with pytest.raises(
            ValueError, match=re.escape("backend class 'foo' not among")
        ):
            # Testing the wrong type on purpose
            ixmp.Platform(backend="foo")

        # name="default" is used, referring to "local"
        mp = ixmp.Platform()
        assert "local" == mp.name

    # NOTE Can't use 'backend' due to duplicate parametrization
    @pytest.mark.parametrize(
        "_backend, backend_args",
        (
            ("jdbc", dict(driver="hsqldb", url="jdbc:hsqldb:mem:TestPlatform")),
            pytest.param(
                "ixmp4", dict(ixmp4_name="ixmp4-local"), marks=min_ixmp4_version
            ),
        ),
    )
    def test_init1(
        self, _backend: Literal["jdbc", "ixmp4"], backend_args: dict[str, str]
    ) -> None:
        # Platform can be instantiated
        ixmp.Platform(backend=_backend, **backend_args)

    def test_getattr(self, test_mp: ixmp.Platform) -> None:
        """Test __getattr__."""
        with pytest.raises(AttributeError):
            test_mp.not_a_direct_backend_method

    def test_scenario_list(self, mp: ixmp.Platform) -> None:
        scenario = mp.scenario_list()
        assert isinstance(scenario, pd.DataFrame)


@pytest.fixture
def log_level_mp(test_mp: ixmp.Platform) -> Generator[ixmp.Platform, Any, None]:
    """A fixture that preserves the log level of *test_mp*."""
    tmp = test_mp.get_log_level()
    yield test_mp
    test_mp.set_log_level(tmp)


# TODO Not sure why 'NOTSET' fails on IXMP4Backend
@pytest.mark.jdbc
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
def test_log_level(log_level_mp: ixmp.Platform, level: str, exc) -> None:
    """Log level can be set and retrieved."""
    if exc is None:
        log_level_mp.set_log_level(level)
        assert log_level_mp.get_log_level() == level
    else:
        with pytest.raises(exc):
            log_level_mp.set_log_level(level)


def test_scenario_list(mp: ixmp.Platform) -> None:
    scenario = mp.scenario_list(model="Douglas Adams")["scenario"]
    assert scenario[0] == "Hitchhiker"


# TODO Not sure why this fails on IXMP4Backend
@pytest.mark.jdbc
def test_export_timeseries_data(mp: ixmp.Platform, tmp_path: Path) -> None:
    path = tmp_path / "export.csv"
    mp.export_timeseries_data(path, model="Douglas Adams", unit="???", region="World")

    obs = pd.read_csv(path, index_col=False, header=0)
    exp = (
        DATA[0]
        .assign(**models["h2g2"], version=1, subannual="Year", meta=0)
        .rename(columns=lambda c: c.upper())
        .reindex(columns=FIELDS["write_file"])
    )

    assert_frame_equal(exp, obs)


def test_export_ts_wrong_params(test_mp: ixmp.Platform, tmp_path: Path) -> None:
    """Platform.export_timeseries_data to raise error with wrong parameters."""
    path = tmp_path / "export.csv"
    with raises(ValueError, match="Invalid arguments"):
        test_mp.export_timeseries_data(
            path,
            model="Douglas Adams",
            unit="???",
            region="World",
            export_all_runs=True,
        )


# TODO Not sure why this fails on IXMP4Backend
@pytest.mark.jdbc
def test_export_ts_of_all_runs(mp: ixmp.Platform, tmp_path: Path) -> None:
    """Export timeseries of all runs."""
    path = tmp_path / "export.csv"

    # Add a new version of a run
    ts = ixmp.TimeSeries(mp, **models["h2g2"], version="new", annotation="fo")
    ts.add_timeseries(DATA[0])
    ts.commit("create a new version")
    ts.set_as_default()

    # Export all default model+scenario runs
    mp.export_timeseries_data(
        path, unit="???", region="World", default=True, export_all_runs=True
    )
    # Find the one default version of the expected scenario
    exp_default_version = mp.scenario_list(
        model=models["h2g2"]["model"], scen=models["h2g2"]["scenario"]
    )["version"].item()

    obs = pd.read_csv(path, index_col=False, header=0)
    exp = (
        DATA[0]
        .assign(**models["h2g2"], version=exp_default_version, subannual="Year", meta=0)
        .rename(columns=lambda c: c.upper())
        .reindex(columns=FIELDS["write_file"])
    )

    assert_frame_equal(exp, obs)

    # Export all model+scenario run versions (including non-default)
    mp.export_timeseries_data(
        path, unit="???", region="World", default=False, export_all_runs=True
    )
    obs = pd.read_csv(path, index_col=False, header=0)

    # We only have 1 model, scenario, region, variable, unit, so we expect lines equal
    # to number of versions (equals default version number) times number of years:
    expected = exp_default_version * len(DATA[0]["year"])  # Usually 4

    assert expected == len(obs)


def test_export_timeseries_data_empty(mp: ixmp.Platform, tmp_path: Path) -> None:
    """Dont export data if given models/scenarios do not have any runs."""
    path = tmp_path / "export.csv"
    model = "model-no-run"
    mp.add_model_name(model)
    mp.add_scenario_name("scenario-no-run")

    mp.export_timeseries_data(path, model=model, unit="???", region="World")

    assert 0 == len(pd.read_csv(path, index_col=False, header=0))


def test_unit_list(test_mp: ixmp.Platform) -> None:
    units = test_mp.units()
    assert ("cases" in units) is True


def test_add_unit(test_mp: ixmp.Platform) -> None:
    test_mp.add_unit("test", "just testing")


def test_regions(test_mp: ixmp.Platform) -> None:
    regions = test_mp.regions()

    # Result has the expected columns
    columns = ["region", "mapped_to", "parent", "hierarchy"]
    assert all(regions.columns == columns)

    # One row is as expected
    obs = regions[regions.region == "World"]
    assert all([list(obs.loc[0]) == ["World", None, "World", "common"]])


# NOTE IXMP4(Backend) doesn't store `Parent`; instead, it returns the region name itself
@pytest.mark.jdbc
def test_add_region(test_mp: ixmp.Platform) -> None:
    # Region can be added
    test_mp.add_region("foo", "bar", "World")

    # Region can be retrieved
    regions = test_mp.regions()
    obs = regions[regions["region"] == "foo"].reset_index(drop=True)
    assert all([list(obs.loc[0]) == ["foo", None, "World", "bar"]])


# NOTE IXMP4Backend doesn't handle synonyms, it might not ever
@pytest.mark.jdbc
def test_add_region_synonym(test_mp: ixmp.Platform) -> None:
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


# TODO Not yet implemented on IXMP4Backend
@pytest.mark.jdbc
def test_timeslices(test_mp: ixmp.Platform) -> None:
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == "Common"]
    # result has all attributes of time slice
    assert all(obs.columns == ["name", "category", "duration"])
    # result contains pre-defined Year time slice
    assert all([list(obs.iloc[0]) == ["Year", "Common", 1.0]])


# TODO Not yet implemented on IXMP4Backend
@pytest.mark.jdbc
def test_add_timeslice(test_mp: ixmp.Platform) -> None:
    test_mp.add_timeslice("January, 1st", "Days", 1.0 / 366)
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == "Days"]
    # return only added time slice
    assert len(obs) == 1
    # returned time slice attributes have expected values
    assert all([list(obs.iloc[0]) == ["January, 1st", "Days", 1.0 / 366]])


# TODO Not yet implemented on IXMP4Backend
@pytest.mark.jdbc
def test_add_timeslice_duplicate(caplog, test_mp: ixmp.Platform) -> None:
    test_mp.add_timeslice("foo_slice", "foo_category", 0.2)

    # Adding same name with different duration raises an error
    msg = "timeslice `foo_slice` already defined with duration 0.2"
    with raises(ValueError, match=re.escape(msg)):
        test_mp.add_timeslice("foo_slice", "bar_category", 0.3)

    # Re-adding with the same duration only logs a message
    with assert_logs(caplog, msg, at_level=logging.INFO):
        test_mp.add_timeslice("foo_slice", "bar_category", 0.2)


def test_weakref() -> None:
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

    # Delete the Platform. Note that this only has an effect if there are no existing
    # references to it
    del mp

    # s.platform is a dead weak reference, so it can't be accessed
    with pytest.raises(ReferenceError):
        s.platform._backend

    # There is only one remaining reference to the backend: the *backend* name in the
    # local scope
    assert getrefcount(backend) - 1 == 1

    # The backend is garbage-collected at this point

    # The Scenario object still lives, but can't be used for anything
    assert s.model == "foo"

    # *s* is garbage-collected at this point


def test_add_model_name(test_mp: ixmp.Platform) -> None:
    test_mp.add_model_name("new_model_name")
    assert "new_model_name" in test_mp.get_model_names()


def test_add_scenario_name(test_mp: ixmp.Platform) -> None:
    test_mp.add_scenario_name("new_scenario_name")
    assert "new_scenario_name" in test_mp.get_scenario_names()
