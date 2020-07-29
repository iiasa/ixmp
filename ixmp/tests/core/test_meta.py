"""Test meta functionality of ixmp.Platform."""

import pytest

import ixmp
from ixmp.testing import models


sample_meta = {'sample_string': 3, 'another_string': 'string_value'}


def test_set_meta_missing_argument(mp):
    meta = {'sample_string': 3}
    with pytest.raises(ValueError):
        mp.set_meta(meta)


def test_set_meta(mp):
    meta = {'sample_string': 3}
    model = models['dantzig']['model']
    mp.set_meta(meta, model=model)
    obs = mp.get_meta(model=model)
    assert obs == meta


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
    expected = ("Metadata already contains category")
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, **model)


def test_unique_meta_scenario(mp):
    """
    When setting a meta key on a specific Scenario run, setting the same key
    on an higher level should fail too.
    """
    model = models['dantzig']
    scen = ixmp.Scenario(mp, **model)
    scen.set_meta(sample_meta)

    expected = ("Metadata already contains category")
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, **model)
    with pytest.raises(Exception, match=expected):
        mp.set_meta(sample_meta, model=model['model'])
