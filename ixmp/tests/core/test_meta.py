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
    with pytest.raises(Exception) as err:
        # TODO: look for more exact exception -
        # jpype._jclass.at.ac.iiasa.ixmp.exceptions.IxException: at.ac.iiasa.ixmp.exceptions.IxException: Metadata already contains category another_string
        mp.set_meta(sample_meta, **model, version=scenario.version)
    scen = ixmp.Scenario(mp, **model)
    with pytest.raises(Exception):
        scen.set_meta(sample_meta)


def test_unique_meta_reversed(mp):
    """
    When setting a meta key on a lower level, setting the same key on an higher
    level should fail too.
    """
    model = models['dantzig']
    scen = ixmp.Scenario(mp, **model)
    scen.set_meta(sample_meta)

    with pytest.raises(Exception):
        # TODO: look for more exact exception
        mp.set_meta(sample_meta, **model)
    with pytest.raises(Exception):
        ixmp.set_meta(sample_meta, model=model['model'])
