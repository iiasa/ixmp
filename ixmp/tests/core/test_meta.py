"""Test meta functionality of ixmp.Platform."""

import copy
import pytest

import ixmp
from ixmp.testing import models


sample_meta = {'sample_int': 3, 'sample_string': 'string_value'}


def test_set_meta_missing_argument(mp):
    model = models['dantzig']

    with pytest.raises(ValueError):
        mp.set_meta(sample_meta)
    with pytest.raises(ValueError):
        mp.set_meta(sample_meta, model=model['model'], version=0)
    with pytest.raises(ValueError):
        mp.set_meta(sample_meta, scenario=model['scenario'], version=0)


def test_set_get_meta(mp):
    """ASsert that storing+retrieving meta yields expected values"""
    model = models['dantzig']['model']
    mp.set_meta(sample_meta, model=model)
    obs = mp.get_meta(model=model)
    assert obs == sample_meta


def test_unique_meta(mp):
    """
    When setting a meta key on two levels, a uniqueness error is expected.
    """
    model = models['dantzig']
    scenario = ixmp.Scenario(mp, **model, version='new')
    scenario.commit('save dummy scenario')
    mp.set_meta(sample_meta, model=model['model'])
    expected = ("Metadata already contains category")
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, **model, version=scenario.version)
    scen = ixmp.Scenario(mp, **model)
    with pytest.raises(Exception, match=expected):
        scen.set_meta(sample_meta)


def test_unique_meta_model_scenario(mp):
    """
    When setting a meta key for a Model, it shouldn't be possible to set it
    for a Model+Scenario then.
    """
    model = models['dantzig']
    mp.set_meta(sample_meta, model=model['model'])
    expected = "Metadata already contains category"
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, **model)


def test_unique_meta_scenario(mp):
    """
    When setting a meta key on a specific Scenario run, setting the same key
    on an higher level (Model or Model+Scenario) should fail.
    """
    model = models['dantzig']
    scen = ixmp.Scenario(mp, **model)
    scen.set_meta(sample_meta)
    # add a second scenario and verify that setting Meta for it works
    scen2 = ixmp.Scenario(mp, **model, version="new")
    scen2.commit('save dummy scenario')
    scen2.set_meta(sample_meta)
    assert scen2.get_meta() == scen.get_meta()

    expected = ("Metadata already contains category")
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, **model)
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, model=model['model'])


def test_meta_partial_overwrite(mp):
    meta1 = {'sample_string': 3.0, 'another_string': 'string_value'}
    meta2 = {'sample_string': 5.0, 'yet_another_string': 'hello'}
    model = models['dantzig']
    scen = ixmp.Scenario(mp, **model)
    scen.set_meta(meta1)
    scen.set_meta(meta2)
    expected = copy.copy(meta1)
    expected.update(meta2)
    obs = scen.get_meta()
    assert obs == expected


def test_remove_meta(mp):
    meta = {'sample_int': 3.0, 'another_string': 'string_value'}
    remove_key = 'another_string'
    model = models['dantzig']
    mp.set_meta(meta, **model)
    mp.remove_meta(remove_key, **model)
    expected = copy.copy(meta)
    del expected[remove_key]
    obs = mp.get_meta(**model)
    assert expected == obs


def test_remove_invalid_meta(mp):
    """
    Removing nonexisting meta entries or None shouldn't result in any meta
    being removed. Providing None should give a ValueError.
    """
    model = models['dantzig']
    mp.set_meta(sample_meta, **model)
    with pytest.raises(ValueError):
        mp.remove_meta(None, **model)
    mp.remove_meta('nonexisting_category', **model)
    mp.remove_meta([], **model)
    obs = mp.get_meta(**model)
    assert obs == sample_meta


def test_set_and_remove_meta_scenario(mp):
    """
    Test partial overwriting and meta deletion on scenario level
    """
    meta1 = {'sample_string': 3.0, 'another_string': 'string_value'}
    meta2 = {'sample_string': 5.0, 'yet_another_string': 'hello'}
    remove_key = 'another_string'
    model = models['dantzig']

    scen = ixmp.Scenario(mp, **model)
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
    """Scenario.delete_meta works but raises a deprecation warning"""
    model = models['dantzig']
    scen = ixmp.Scenario(mp, **model)
    meta = {'sample_int': 3, 'sample_string': 'string_value'}
    remove_key = 'sample_string'

    scen.set_meta(sample_meta)
    with pytest.warns(DeprecationWarning):
        scen.delete_meta(remove_key)
    expected = copy.copy(meta)
    del expected[remove_key]
    obs = scen.get_meta()
    assert obs == expected
