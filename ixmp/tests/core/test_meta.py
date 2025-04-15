"""Test :ref:`data-meta` functionality of :class:`.Platform` and :class:`.Scenario`."""
# TODO move tests to .tests.backend or .tests.core, according to the class whose
#      behaviour they actually test.

import copy
from typing import Any, Generator

import pytest

import ixmp
from ixmp.testing import models

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


@pytest.fixture(scope="function")
def mp(test_mp_f: ixmp.Platform) -> Generator[ixmp.Platform, Any, None]:
    """A test Platform.

    The platform contains one time series with the "dantizg" model & scenario name from
    :data:`testing.models`.

    Unlike the other submodules of :mod:`ixmp.tests.core`, the tests in this file
    generally require a clean platform each time, so this is a function-scoped fixture.
    """
    # Don't call populate_test_platform(), since this is all that's needed
    ts = ixmp.TimeSeries(test_mp_f, **models["dantzig"], version="new")
    ts.commit("")

    yield test_mp_f


# TODO IXMP4Backend needs to handle meta data
@pytest.mark.jdbc
class TestMeta:
    @pytest.mark.parametrize("meta", META_ENTRIES)
    def test_set_meta_missing_argument(self, mp: ixmp.Platform, meta) -> None:
        with pytest.raises(ValueError):
            mp.set_meta(meta)
        with pytest.raises(ValueError):
            mp.set_meta(meta, model=DANTZIG["model"], version=0)
        with pytest.raises(ValueError):
            mp.set_meta(meta, scenario=DANTZIG["scenario"], version=0)

    @pytest.mark.parametrize("meta", META_ENTRIES)
    def test_set_get_meta(self, mp: ixmp.Platform, meta) -> None:
        """Assert that storing+retrieving meta yields expected values."""
        mp.set_meta(meta, model=DANTZIG["model"])
        obs = mp.get_meta(model=DANTZIG["model"])
        assert obs == meta

    @pytest.mark.parametrize("meta", META_ENTRIES)
    def test_unique_meta(self, mp: ixmp.Platform, meta) -> None:
        """
        When setting a meta category on two distinct levels, a uniqueness error is
        expected.
        """
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
    def test_set_get_meta_equals(self, mp: ixmp.Platform, meta) -> None:
        initial_meta = mp.get_meta(scenario=DANTZIG["scenario"])
        mp.set_meta(meta, model=DANTZIG["model"])
        obs_meta = mp.get_meta(scenario=DANTZIG["scenario"])
        assert obs_meta == initial_meta

    @pytest.mark.parametrize("meta", META_ENTRIES)
    def test_unique_meta_model_scenario(self, mp: ixmp.Platform, meta) -> None:
        """
        When setting a meta key for a Model, it shouldn't be possible to set it
        for a Model+Scenario then.
        """
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
    def test_get_meta_strict(self, mp: ixmp.Platform, meta) -> None:
        """
        Set meta indicators on several model/scenario/version levels and test
        the 'strict' parameter of get_meta().
        """
        # set meta on various levels
        model_meta = {
            "model_int": 3,
            "model_string": "string_value",
            "model_bool": False,
        }
        scenario_meta = {
            "scenario_int": 3,
            "scenario_string": "string_value",
            "scenario_bool": False,
        }
        meta2 = {
            "sample_int2": 3,
            "sample_string2": "string_value2",
            "sample_bool2": False,
        }
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
    def test_unique_meta_scenario(self, mp: ixmp.Platform, meta) -> None:
        """
        When setting a meta key on a specific Scenario run, setting the same key
        on an higher level (Model or Model+Scenario) should fail.
        """
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

    def test_meta_partial_overwrite(self, mp: ixmp.Platform) -> None:
        meta1 = {
            "sample_string": 3.0,
            "another_string": "string_value",
            "sample_bool": False,
        }
        meta2 = {
            "sample_string": 5.0,
            "yet_another_string": "hello",
            "sample_bool": True,
        }
        scen = ixmp.Scenario(mp, **DANTZIG)
        scen.set_meta(meta1)
        scen.set_meta(meta2)
        expected = copy.copy(meta1)
        expected.update(meta2)
        obs = scen.get_meta()
        assert obs == expected

    def test_remove_meta(self, mp: ixmp.Platform) -> None:
        meta = {"sample_int": 3.0, "another_string": "string_value"}
        remove_key = "another_string"
        mp.set_meta(meta, **DANTZIG)
        mp.remove_meta(remove_key, **DANTZIG)
        expected = copy.copy(meta)
        del expected[remove_key]
        obs = mp.get_meta(**DANTZIG)
        assert expected == obs

    def test_remove_invalid_meta(self, mp: ixmp.Platform) -> None:
        """
        Removing nonexisting meta entries or None shouldn't result in any meta
        being removed. Providing None should give a ValueError.
        """
        mp.set_meta(SAMPLE_META, **DANTZIG)
        with pytest.raises(ValueError):
            mp.remove_meta(None, **DANTZIG)
        mp.remove_meta("nonexisting_category", **DANTZIG)
        mp.remove_meta([], **DANTZIG)
        obs = mp.get_meta(**DANTZIG)
        assert obs == SAMPLE_META

    def test_set_and_remove_meta_scenario(self, mp: ixmp.Platform) -> None:
        """
        Test partial overwriting and meta deletion on scenario level.
        """
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

    def test_scenario_delete_meta_warning(self, mp: ixmp.Platform) -> None:
        """
        Scenario.delete_meta works but raises a deprecation warning.

        This test can be removed once Scenario.delete_meta is removed.
        """
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

    def test_meta_arguments(self, mp: ixmp.Platform) -> None:
        """Set scenario meta with key-value arguments"""
        meta = {"sample_int": 3}
        scen = ixmp.Scenario(mp, **DANTZIG)
        scen.set_meta(meta)
        # add a second scenario and verify that setting Meta for it works
        scen2 = ixmp.Scenario(mp, **DANTZIG, version="new")
        scen2.commit("save dummy scenario")
        scen2.set_meta(*meta.popitem())
        assert scen.get_meta() == scen2.get_meta()

    def test_update_meta_lists(self, mp: ixmp.Platform) -> None:
        """Set metadata categories having list/array values."""
        SAMPLE_META = {"list_category": ["a", "b", "c"]}
        mp.set_meta(SAMPLE_META, model=DANTZIG["model"])
        obs = mp.get_meta(model=DANTZIG["model"])
        assert obs == SAMPLE_META
        # try updating meta
        SAMPLE_META = {"list_category": ["a", "e", "f"]}
        mp.set_meta(SAMPLE_META, model=DANTZIG["model"])
        obs = mp.get_meta(model=DANTZIG["model"])
        assert obs == SAMPLE_META

    def test_meta_mixed_list(self, mp: ixmp.Platform) -> None:
        """Set metadata categories having list/array values."""
        meta = {"mixed_category": ["string", 0.01, True]}
        mp.set_meta(meta, model=DANTZIG["model"])
        obs = mp.get_meta(model=DANTZIG["model"])
        assert obs == meta
