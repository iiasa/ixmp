# coding=utf-8

import pandas as pd

def test_regions(test_mp):
    obs = test_mp.regions()
    assert all(obs.columns == ['region', 'mapped_to', 'parent', 'hierarchy'])
    assert all(obs[obs.region == 'World'] == ['World', None, None, 'common'])


def test_add_region(test_mp):
    test_mp.add_region('foo', 'bar', 'World')
    obs = test_mp.regions()
    assert all(obs[obs['region'] == 'foo'] == ['foo', None, 'World', 'bar'])


def test_add_region_synonym(test_mp):
    test_mp.add_region_synomym('foo2', 'foo')
    regions = test_mp.regions()
    obs = regions[regions.region.isin(['foo', 'foo2'])].reset_index(drop=True)
    print(obs)

    exp = pd.DataFrame([
        ['foo', None, 'World', 'bar'],
        ['foo2', 'foo', 'World', 'bar'],
    ],
        columns=['region', 'mapped_to', 'parent', 'hierarchy']
    )
    pd.testing.assert_frame_equal(obs, exp)
