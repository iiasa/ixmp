import copy

import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp
from ixmp import Platform, Scenario, TimeSeries, config as ixmp_config
from ixmp.testing import make_dantzig, models, populate_test_platform
from ixmp.tests.core.test_timeseries import DATA, expected, wide


@pytest.fixture(scope="class")
def mp(request, tmp_env, tmp_path_factory):
    """An empty ixmp.Platform connected to a temporary, in-memory database.

    This is a lightly-modified clone of ixmp.testing.test_mp fixture.
    """
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace("/", " ")

    # Add a platform
    # ixmp_config.add_platform(platform_name, "dbapi", ":memory:")
    ixmp_config.add_platform(
        platform_name, "dbapi", tmp_path_factory.mktemp(platform_name) / "test.db"
    )

    # Launch Platform
    mp = Platform(name=platform_name)
    yield mp

    # Remove from config
    ixmp_config.remove_platform(platform_name)


@pytest.fixture(scope="function", params=[TimeSeries, Scenario])
def ts(request, mp):
    """Copied from :func:`core.test_timeseries.ts`."""
    # Use a hash of the pytest node ID to avoid exceeding the maximum length for a
    # scenario name
    node = hash(request.node.nodeid.replace("/", " "))
    # Class of object to yield
    cls = request.param
    yield cls(mp, model=f"test-{node}", scenario="test", version="new")


class TestDatabaseBackend:
    def test_init_backend(self, mp):
        """A Platform backed by DatabaseBackend can be initialized."""

    @pytest.mark.parametrize("kind", ("model", "scenario"))
    def test_names(self, mp, kind):
        setter = getattr(mp, f"add_{kind}_name")
        getter = getattr(mp, f"get_{kind}_names")

        items = ("foo", "bar", "baz")
        for i in items:
            setter(i)

        assert items == tuple(getter())

    def test_node(self, mp):
        """Nodes can be stored and retrieved."""
        mp.add_region("AT", "country")
        mp.regions()

    def test_unit(self, mp):
        """Units can be stored and retrieved."""
        units = ["kg", "km"]
        for unit in units:
            mp.add_unit(unit)

        assert units == mp.units()

        # NB only works when using a file-backed `mp` fixture. With :memory:, nothing
        # is persisted, so this doesn't work.
        mp.close_db()
        mp.open_db()

        assert units == mp.units()

    def test_timeslice(self, mp):
        """Time slices can be stored and retrieved."""
        items = (
            dict(name="Spring", category="season", duration=1.0 / 9),
            dict(name="Summer", category="season", duration=2.0 / 9),
            dict(name="Autumn", category="season", duration=4.0 / 9),
            dict(name="Winter", category="season", duration=2.0 / 9),
        )
        for item in items:
            mp.add_timeslice(**item)

        pdt.assert_frame_equal(pd.DataFrame(items), mp.timeslices())

    def test_ts(self, mp):
        """Test Backend.{init,set_as_default,is_default}."""
        args = dict(model="Foo model", scenario="Baz scenario", version="new")
        ts0 = TimeSeries(mp, **args)
        assert 1 == ts0.version

        ts1 = TimeSeries(mp, **args)
        assert 2 == ts1.version

        ts1.set_as_default()
        assert ts1.is_default()

        assert not ts0.is_default()

        del ts0, ts1

        args.pop("version")
        ts2 = TimeSeries(mp, **args)
        assert 2 == ts2.version
        assert ts2.is_default()

    @pytest.mark.parametrize("fmt", ["long", "wide"])
    def test_tsdata(self, ts, fmt):
        # Copied from core.test_timeseries.test_add_timeseries
        data = DATA[0] if fmt == "long" else wide(DATA[0])

        # Data added
        ts.add_timeseries(data)
        ts.commit("")

        # Error: column 'unit' is missing
        with pytest.raises(ValueError):
            ts.add_timeseries(DATA[0].drop("unit", axis=1))

        # Copied from core.test_timeseries.test_get
        exp = expected(data, ts)
        args = {}

        if fmt == "wide":
            args["iamc"] = True

        # Data can be retrieved and has the expected value
        obs = ts.timeseries(**args)

        pdt.assert_frame_equal(exp, obs)

    def test_geodata(self, ts):
        # Copied from core.test_timeseries.test_add_geodata

        # Empty TimeSeries includes no geodata
        pdt.assert_frame_equal(DATA["geo"].loc[[False, False, False]], ts.get_geodata())

        # Data can be added
        ts.add_geodata(DATA["geo"])
        ts.commit("")

        # Added data can be retrieved
        obs = ts.get_geodata().sort_values("year").reset_index(drop=True)
        pdt.assert_frame_equal(DATA["geo"], obs)

    @pytest.mark.parametrize(
        "solve",
        (
            False,
            pytest.param(
                True,
                marks=pytest.mark.xfail(
                    raises=NotImplementedError,
                    reason=".backend.io.s_write_gdx() is not implemented",
                ),
            ),
        ),
    )
    def test_make_dantzig(self, mp, solve):
        make_dantzig(mp, solve=solve)


@pytest.fixture(scope="class")
def mp_(mp):
    """A Platform containing test data."""
    populate_test_platform(mp, solve=False)
    yield mp


# Copied from core.test_meta

SAMPLE_META = {"sample_int": 3, "sample_string": "string_value", "sample_bool": False}
META_ENTRIES = [
    {"sample_int": 3},
    {"sample_string": "string_value"},
    {"sample_bool": False},
    {
        "sample_int": 3,
        "sample_string": "string_value",
        "sample_bool": False,
    },
    {"mixed_category": ["string", 0.01, 2, True]},
]
DANTZIG = models["dantzig"]


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_set_meta_missing_argument(mp_, meta):
    mp = mp_

    with pytest.raises(ValueError):
        mp.set_meta(meta)
    with pytest.raises(ValueError):
        mp.set_meta(meta, model=DANTZIG["model"], version=0)
    with pytest.raises(ValueError):
        mp.set_meta(meta, scenario=DANTZIG["scenario"], version=0)


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_set_get_meta(mp_, meta):
    """Assert that storing+retrieving meta yields expected values."""
    mp = mp_

    mp.set_meta(meta, model=DANTZIG["model"])
    obs = mp.get_meta(model=DANTZIG["model"])
    assert obs == meta


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_unique_meta(mp_, meta):
    """
    When setting a meta category on two distinct levels, a uniqueness error is
    expected.
    """
    mp = mp_

    scenario = ixmp.Scenario(mp, **DANTZIG, version="new")
    scenario.commit("save dummy scenario")
    mp.set_meta(meta, model=DANTZIG["model"])
    expected = (
        r"The meta category .* is already used at another level: "
        r"model canning problem, scenario null, version null"
    )
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG, version=scenario.version)
    scen = ixmp.Scenario(mp, **DANTZIG)
    with pytest.raises(Exception, match=expected):
        scen.set_meta(meta)
    # changing the category value type of an entry should also raise an error
    meta = {"sample_entry": 3}
    mp.set_meta(meta, **DANTZIG)
    meta["sample_entry"] = "test-string"
    expected = (
        r"The meta category .* is already used at another level: "
        r"model canning problem, scenario standard, version null"
    )
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG, version=scenario.version)


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_set_get_meta_equals(mp_, meta):
    mp = mp_

    initial_meta = mp.get_meta(scenario=DANTZIG["scenario"])
    mp.set_meta(meta, model=DANTZIG["model"])
    obs_meta = mp.get_meta(scenario=DANTZIG["scenario"])
    assert obs_meta == initial_meta


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_unique_meta_model_scenario(mp_, meta):
    """
    When setting a meta key for a Model, it shouldn't be possible to set it
    for a Model+Scenario then.
    """
    mp = mp_

    mp.set_meta(meta, model=DANTZIG["model"])
    expected = r"The meta category .* is already used at another level: "
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG)

    # Setting this meta category on a new model should fail too
    dantzig2 = {
        "model": "canning problem 2",
        "scenario": "standard",
    }
    mp.add_model_name(dantzig2["model"])
    expected = r"The meta category .* is already used at another level: "
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **dantzig2)


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_get_meta_strict(mp_, meta):
    """
    Set meta indicators on several model/scenario/version levels and test
    the 'strict' parameter of get_meta().
    """
    mp = mp_

    # set meta on various levels
    model_meta = {"model_int": 3, "model_string": "string_value", "model_bool": False}
    scenario_meta = {
        "scenario_int": 3,
        "scenario_string": "string_value",
        "scenario_bool": False,
    }
    meta2 = {"sample_int2": 3, "sample_string2": "string_value2", "sample_bool2": False}
    meta3 = {
        "sample_int3": 3,
        "sample_string3": "string_value3",
        "sample_bool3": False,
        "mixed3": ["string", 0.01, 2, True],
    }
    meta_scen = {
        "sample_int4": 3,
        "sample_string4": "string_value4",
        "sample_bool4": False,
        "mixed4": ["string", 0.01, 2, True],
    }
    scenario2 = "standard 2"
    model2 = "canning problem 2"
    mp.add_scenario_name(scenario2)
    mp.add_model_name(model2)
    dantzig2 = {
        "model": model2,
        "scenario": "standard",
    }
    dantzig3 = {
        "model": model2,
        "scenario": scenario2,
    }
    mp.set_meta(model_meta, model=DANTZIG["model"])
    mp.set_meta(scenario_meta, scenario=DANTZIG["scenario"])
    mp.set_meta(meta, **DANTZIG)
    mp.set_meta(meta2, **dantzig2)
    mp.set_meta(meta3, **dantzig3)
    scen = ixmp.Scenario(mp, **DANTZIG, version="new")
    scen.commit("save dummy scenario")
    scen.set_meta(meta_scen)

    # Retrieve and validate meta indicators
    # model
    obs1 = mp.get_meta(model=DANTZIG["model"])
    assert obs1 == model_meta
    # scenario
    obs2 = mp.get_meta(scenario=DANTZIG["scenario"], strict=True)
    assert obs2 == scenario_meta
    # model+scenario
    obs3 = mp.get_meta(**DANTZIG)
    exp3 = copy.copy(meta)
    exp3.update(model_meta)
    exp3.update(scenario_meta)
    assert obs3 == exp3
    # model+scenario, strict
    obs3_strict = mp.get_meta(**DANTZIG, strict=True)
    assert obs3_strict == meta
    assert obs3 != obs3_strict

    # second model+scenario combination
    obs4 = mp.get_meta(**dantzig2)
    exp4 = copy.copy(meta2)
    exp4.update(scenario_meta)
    assert obs4 == exp4
    # second model+scenario combination, strict
    obs4_strict = mp.get_meta(**dantzig2, strict=True)
    assert obs4_strict == meta2
    assert obs4 != obs4_strict

    # second model+scenario combination
    obs5 = mp.get_meta(**dantzig3)
    exp5 = copy.copy(meta3)
    assert obs5 == exp5

    # model+scenario+version
    obs6 = mp.get_meta(**DANTZIG, version=scen.version)
    exp6 = copy.copy(meta_scen)
    exp6.update(meta)
    exp6.update(model_meta)
    exp6.update(scenario_meta)
    assert obs6 == exp6
    obs6_strict = mp.get_meta(
        DANTZIG["model"], DANTZIG["scenario"], scen.version, strict=True
    )
    assert obs6_strict == meta_scen


@pytest.mark.parametrize("meta", META_ENTRIES)
def test_unique_meta_scenario(mp_, meta):
    """
    When setting a meta key on a specific Scenario run, setting the same key
    on an higher level (Model or Model+Scenario) should fail.
    """
    mp = mp_

    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta)
    # add a second scenario and verify that setting+getting Meta works
    scen2 = ixmp.Scenario(mp, **DANTZIG, version="new")
    scen2.commit("save dummy scenario")
    scen2.set_meta(meta)
    assert scen2.get_meta() == scen.get_meta()

    expected = (
        r"The meta category .* is already used at another level: "
        r"model canning problem, scenario standard, "
    )
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG)
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, model=DANTZIG["model"])


def test_meta_partial_overwrite(mp_):
    mp = mp_

    meta1 = {
        "sample_string": 3.0,
        "another_string": "string_value",
        "sample_bool": False,
    }
    meta2 = {"sample_string": 5.0, "yet_another_string": "hello", "sample_bool": True}
    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta1)
    scen.set_meta(meta2)
    expected = copy.copy(meta1)
    expected.update(meta2)
    obs = scen.get_meta()
    assert obs == expected


def test_remove_meta(mp_):
    mp = mp_

    meta = {"sample_int": 3.0, "another_string": "string_value"}
    remove_key = "another_string"
    mp.set_meta(meta, **DANTZIG)
    mp.remove_meta(remove_key, **DANTZIG)
    expected = copy.copy(meta)
    del expected[remove_key]
    obs = mp.get_meta(**DANTZIG)
    assert expected == obs


def test_remove_invalid_meta(mp_):
    """
    Removing nonexisting meta entries or None shouldn't result in any meta
    being removed. Providing None should give a ValueError.
    """
    mp = mp_

    mp.set_meta(SAMPLE_META, **DANTZIG)
    with pytest.raises(ValueError):
        mp.remove_meta(None, **DANTZIG)
    mp.remove_meta("nonexisting_category", **DANTZIG)
    mp.remove_meta([], **DANTZIG)
    obs = mp.get_meta(**DANTZIG)
    assert obs == SAMPLE_META


def test_set_and_remove_meta_scenario(mp_):
    """
    Test partial overwriting and meta deletion on scenario level.
    """
    mp = mp_

    meta1 = {"sample_string": 3.0, "another_string": "string_value"}
    meta2 = {"sample_string": 5.0, "yet_another_string": "hello"}
    remove_key = "another_string"

    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta1)
    scen.set_meta(meta2)
    expected = copy.copy(meta1)
    expected.update(meta2)
    obs = scen.get_meta()
    assert expected == obs

    scen.remove_meta(remove_key)
    del expected[remove_key]
    obs = scen.get_meta()
    assert obs == expected


def test_scenario_delete_meta_warning(mp_):
    """
    Scenario.delete_meta works but raises a deprecation warning.

    This test can be removed once Scenario.delete_meta is removed.
    """
    mp = mp_

    scen = ixmp.Scenario(mp, **DANTZIG)
    meta = {"sample_int": 3, "sample_string": "string_value"}
    remove_key = "sample_string"

    scen.set_meta(meta)
    with pytest.warns(DeprecationWarning):
        scen.delete_meta(remove_key)
    expected = copy.copy(meta)
    del expected[remove_key]
    obs = scen.get_meta()
    assert obs == expected


def test_meta_arguments(mp_):
    """Set scenario meta with key-value arguments"""
    mp = mp_

    meta = {"sample_int": 3}
    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta)
    # add a second scenario and verify that setting Meta for it works
    scen2 = ixmp.Scenario(mp, **DANTZIG, version="new")
    scen2.commit("save dummy scenario")
    scen2.set_meta(*meta.popitem())
    assert scen.get_meta() == scen2.get_meta()


def test_update_meta_lists(mp_):
    """Set metadata categories having list/array values."""
    mp = mp_

    SAMPLE_META = {"list_category": ["a", "b", "c"]}
    mp.set_meta(SAMPLE_META, model=DANTZIG["model"])
    obs = mp.get_meta(model=DANTZIG["model"])
    assert obs == SAMPLE_META
    # try updating meta
    SAMPLE_META = {"list_category": ["a", "e", "f"]}
    mp.set_meta(SAMPLE_META, model=DANTZIG["model"])
    obs = mp.get_meta(model=DANTZIG["model"])
    assert obs == SAMPLE_META


def test_meta_mixed_list(mp_):
    """Set metadata categories having list/array values."""
    mp = mp_

    meta = {"mixed_category": ["string", 0.01, True]}
    mp.set_meta(meta, model=DANTZIG["model"])
    obs = mp.get_meta(model=DANTZIG["model"])
    assert obs == meta
