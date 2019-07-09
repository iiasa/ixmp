# coding=utf-8

import pandas as pd


def test_regions(test_mp):
    regions = test_mp.regions()
    obs = regions[regions.region == 'World']
    assert all(obs.columns == ['region', 'mapped_to', 'parent', 'hierarchy'])
    assert all([list(obs.loc[0]) == ['World', None, 'World', 'common']])


def test_add_region(test_mp):
    test_mp.add_region('foo', 'bar', 'World')
    regions = test_mp.regions()
    obs = regions[regions['region'] == 'foo'].reset_index(drop=True)
    assert all([list(obs.loc[0]) == ['foo', None, 'World', 'bar']])


def test_add_region_synonym(test_mp):
    test_mp.add_region('foo', 'bar', 'World')
    test_mp.add_region_synomym('foo2', 'foo')
    regions = test_mp.regions()
    obs = regions[regions.region.isin(['foo', 'foo2'])].reset_index(drop=True)

    exp = pd.DataFrame([
        ['foo', None, 'World', 'bar'],
        ['foo2', 'foo', 'World', 'bar'],
    ],
        columns=['region', 'mapped_to', 'parent', 'hierarchy']
    )
    pd.testing.assert_frame_equal(obs, exp)
