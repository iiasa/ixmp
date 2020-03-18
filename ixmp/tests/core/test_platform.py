"""Test all functionality of ixmp.Platform."""
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import ixmp


def test_init():
    with pytest.raises(ValueError, match="backend class 'foo' not among "
                                         r"\['jdbc'\]"):
        ixmp.Platform(backend='foo')


def test_set_log_level(test_mp):
    test_mp.set_log_level('CRITICAL')
    test_mp.set_log_level('ERROR')
    test_mp.set_log_level('WARNING')
    test_mp.set_log_level('INFO')
    test_mp.set_log_level('DEBUG')
    test_mp.set_log_level('NOTSET')

    with pytest.raises(ValueError):
        test_mp.set_log_level(level='foo')


def test_scenario_list(mp):
    scenario = mp.scenario_list(model='Douglas Adams')['scenario']
    assert scenario[0] == 'Hitchhiker'


def test_export_timeseries_data(mp, tmp_path):
    path = tmp_path / 'export.csv'
    mp.export_timeseries_data(path, model='Douglas Adams')

    with open(path) as f:
        first_line = f.readline()
        assert first_line == ('MODEL,SCENARIO,VERSION,VARIABLE,UNIT,'
                              'REGION,META,SUBANNUAL,YEAR,VALUE\n')
        assert len(f.readlines()) == 2


def test_unit_list(test_mp):
    units = test_mp.units()
    assert ('cases' in units) is True


def test_add_unit(test_mp):
    test_mp.add_unit('test', 'just testing')


def test_regions(test_mp):
    regions = test_mp.regions()

    # Result has the expected columns
    columns = ['region', 'mapped_to', 'parent', 'hierarchy']
    assert all(regions.columns == columns)

    # One row is as expected
    obs = regions[regions.region == 'World']
    assert all([list(obs.loc[0]) == ['World', None, 'World', 'common']])


def test_add_region(test_mp):
    # Region can be added
    test_mp.add_region('foo', 'bar', 'World')

    # Region can be retrieved
    regions = test_mp.regions()
    obs = regions[regions['region'] == 'foo'].reset_index(drop=True)
    assert all([list(obs.loc[0]) == ['foo', None, 'World', 'bar']])


def test_add_region_synonym(test_mp):
    test_mp.add_region('foo', 'bar', 'World')
    test_mp.add_region_synonym('foo2', 'foo')
    regions = test_mp.regions()
    obs = regions[regions.region.isin(['foo', 'foo2'])] \
        .reset_index(drop=True)

    exp = pd.DataFrame([
        ['foo', None, 'World', 'bar'],
        ['foo2', 'foo', 'World', 'bar'],
    ],
        columns=['region', 'mapped_to', 'parent', 'hierarchy']
    )
    assert_frame_equal(obs, exp)
