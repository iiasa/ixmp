"""Test meta functionality of ixmp.Platform and ixmp.Scenario."""

import copy
import pytest

import ixmp
from ixmp.testing import models


SAMPLE_META = {'sample_int': 3, 'sample_string': 'string_value',
               'sample_bool': False}
META_ENTRIES = [
    {'sample_int': 3},
    {'sample_string': 'string_value'},
    {'sample_bool': False},
    {
        'sample_int': 3,
        'sample_string': 'string_value',
        'sample_bool': False,
    },
    {'mixed_category': ['string', 0.01, 2, True]},
]
DANTZIG = models['dantzig']


@pytest.mark.parametrize('meta', META_ENTRIES)
def test_set_meta_missing_argument(mp, meta):
    with pytest.raises(ValueError):
        mp.set_meta(meta)
    with pytest.raises(ValueError):
        mp.set_meta(meta, model=DANTZIG['model'], version=0)
    with pytest.raises(ValueError):
        mp.set_meta(meta, scenario=DANTZIG['scenario'], version=0)


@pytest.mark.parametrize('meta', META_ENTRIES)
def test_set_get_meta(mp, meta):
    """Assert that storing+retrieving meta yields expected values"""
    mp.set_meta(meta, model=DANTZIG['model'])
    obs = mp.get_meta(model=DANTZIG['model'])
    assert obs == meta
    assert obs == mp.get_meta()


@pytest.mark.parametrize('meta', META_ENTRIES)
def test_unique_meta(mp, meta):
    """
    When setting a meta key on two levels, a uniqueness error is expected.
    """
    scenario = ixmp.Scenario(mp, **DANTZIG, version='new')
    scenario.commit('save dummy scenario')
    mp.set_meta(meta, model=DANTZIG['model'])
    expected = ("Metadata already contains category")
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG, version=scenario.version)
    scen = ixmp.Scenario(mp, **DANTZIG)
    with pytest.raises(Exception, match=expected):
        scen.set_meta(meta)
    # changing the category value type still should raise an error
    meta = {'sample_entry': 3}
    mp.set_meta(meta, **DANTZIG)
    meta['sample_entry'] = 'test-string'
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG, version=scenario.version)


@pytest.mark.parametrize('meta', META_ENTRIES)
def test_unique_meta_differing_model(mp, meta):
    """
    When meta is set on a model-level, setting it on a new model and a scenario
    should fail.
    """
    mp.set_meta(meta, model=DANTZIG['model'])
    DANTZIG2 = {
        'model': 'canning problem 2',
        'scenario': 'standard',
    }
    mp.add_model(DANTZIG2['model'])
    expected = "Metadata already contains category"
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG2)


@pytest.mark.parametrize('meta', META_ENTRIES)
def test_unique_meta_model_scenario(mp, meta):
    """
    When setting a meta key for a Model, it shouldn't be possible to set it
    for a Model+Scenario then.
    """
    mp.set_meta(meta, model=DANTZIG['model'])
    expected = "Metadata already contains category"
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG)


@pytest.mark.parametrize('meta', META_ENTRIES)
def test_unique_meta_scenario(mp, meta):
    """
    When setting a meta key on a specific Scenario run, setting the same key
    on an higher level (Model or Model+Scenario) should fail.
    """
    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta)
    # add a second scenario and verify that setting Meta for it works
    scen2 = ixmp.Scenario(mp, **DANTZIG, version="new")
    scen2.commit('save dummy scenario')
    scen2.set_meta(meta)
    assert scen2.get_meta() == scen.get_meta()

    expected = ("Metadata already contains category")
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, **DANTZIG)
    with pytest.raises(Exception, match=expected):
        mp.set_meta(meta, model=DANTZIG['model'])


def test_meta_partial_overwrite(mp):
    meta1 = {'sample_string': 3.0, 'another_string': 'string_value',
             'sample_bool': False}
    meta2 = {'sample_string': 5.0, 'yet_another_string': 'hello',
             'sample_bool': True}
    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta1)
    scen.set_meta(meta2)
    expected = copy.copy(meta1)
    expected.update(meta2)
    obs = scen.get_meta()
    assert obs == expected


def test_remove_meta(mp):
    meta = {'sample_int': 3.0, 'another_string': 'string_value'}
    remove_key = 'another_string'
    mp.set_meta(meta, **DANTZIG)
    mp.remove_meta(remove_key, **DANTZIG)
    expected = copy.copy(meta)
    del expected[remove_key]
    obs = mp.get_meta(**DANTZIG)
    assert expected == obs


def test_remove_invalid_meta(mp):
    """
    Removing nonexisting meta entries or None shouldn't result in any meta
    being removed. Providing None should give a ValueError.
    """
    mp.set_meta(SAMPLE_META, **DANTZIG)
    with pytest.raises(ValueError):
        mp.remove_meta(None, **DANTZIG)
    mp.remove_meta('nonexisting_category', **DANTZIG)
    mp.remove_meta([], **DANTZIG)
    obs = mp.get_meta(**DANTZIG)
    assert obs == SAMPLE_META


def test_set_and_remove_meta_scenario(mp):
    """
    Test partial overwriting and meta deletion on scenario level
    """
    meta1 = {'sample_string': 3.0, 'another_string': 'string_value'}
    meta2 = {'sample_string': 5.0, 'yet_another_string': 'hello'}
    remove_key = 'another_string'

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


def test_scenario_delete_meta_warning(mp):
    """
    Scenario.delete_meta works but raises a deprecation warning.

    This test can be removed once Scenario.delete_meta is removed.
    """
    scen = ixmp.Scenario(mp, **DANTZIG)
    meta = {'sample_int': 3, 'sample_string': 'string_value'}
    remove_key = 'sample_string'

    scen.set_meta(meta)
    with pytest.warns(DeprecationWarning):
        scen.delete_meta(remove_key)
    expected = copy.copy(meta)
    del expected[remove_key]
    obs = scen.get_meta()
    assert obs == expected


def test_meta_arguments(mp):
    """Set scenario meta with key-value arguments"""
    meta = {'sample_int': 3}
    scen = ixmp.Scenario(mp, **DANTZIG)
    scen.set_meta(meta)
    # add a second scenario and verify that setting Meta for it works
    scen2 = ixmp.Scenario(mp, **DANTZIG, version="new")
    scen2.commit('save dummy scenario')
    scen2.set_meta(*meta.popitem())
    assert scen.get_meta() == scen2.get_meta()


def test_update_meta_lists(mp):
    """Set metadata categories having list/array values"""
    SAMPLE_META = {'list_category': ['a', 'b', 'c']}
    mp.set_meta(SAMPLE_META, model=DANTZIG['model'])
    obs = mp.get_meta(model=DANTZIG['model'])
    assert obs == SAMPLE_META
    # try updating meta
    SAMPLE_META = {'list_category': ['a', 'e', 'f']}
    mp.set_meta(SAMPLE_META, model=DANTZIG['model'])
    obs = mp.get_meta(model=DANTZIG['model'])
    assert obs == SAMPLE_META


def test_meta_mixed_list(mp):
    """Set metadata categories having list/array values"""
    meta = {'mixed_category': ['string', 0.01, True]}
    mp.set_meta(meta, model=DANTZIG['model'])
    obs = mp.get_meta(model=DANTZIG['model'])
    assert obs == meta
