import pandas as pd
import pytest
from numpy import testing as npt

import ixmp

test_args = ('Douglas Adams', 'Hitchhiker')


# string columns and dataframe for timeseries checks
iamc_idx_cols = ['model', 'scenario', 'region', 'variable', 'unit']
cols_str = ['region', 'variable', 'unit', 'year']

TS_DF = {'year': [2010, 2020], 'value': [23.7, 23.8]}
TS_DF = pd.DataFrame.from_dict(TS_DF)
TS_DF['region'] = 'World'
TS_DF['variable'] = 'Testing'
TS_DF['unit'] = '???'


def test_get_timeseries(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args)
    assert_timeseries(scen)


def test_get_timeseries_iamc(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args)
    obs = scen.timeseries(iamc=True, regions='World', variables='Testing')

    exp = TS_DF.pivot_table(index=['region', 'variable', 'unit'],
                            columns='year')['value'].reset_index()
    exp['model'] = 'Douglas Adams'
    exp['scenario'] = 'Hitchhiker'

    npt.assert_array_equal(exp[iamc_idx_cols], obs[iamc_idx_cols])
    npt.assert_array_almost_equal(exp[2010], obs[2010])


def test_new_timeseries_as_year_value(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    scen.add_timeseries(TS_DF)
    scen.commit('importing a testing timeseries')
    assert_timeseries(scen)


def test_new_timeseries_as_iamc(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    scen.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    scen.commit('importing a testing timeseries')
    assert_timeseries(scen)


def assert_timeseries(scen):
    obs = scen.timeseries(region='World', variable='Testing')
    npt.assert_array_equal(TS_DF[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(TS_DF['value'], obs['value'])


def test_new_timeseries_error(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    df = {'year': [2010, 2020], 'value': [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df['region'] = 'World'
    df['variable'] = 'Testing'
    # colum `unit` is missing
    pytest.raises(ValueError, scen.add_timeseries, df)


def test_timeseries_edit(test_mp_props):
    mp = ixmp.Platform(test_mp_props)
    scen = ixmp.TimeSeries(mp, *test_args)
    df = {'region': ['World', 'World'], 'variable': ['Testing', 'Testing'],
          'unit': ['???', '???'], 'year': [2010, 2020], 'value': [23.7, 23.8]}
    exp = pd.DataFrame.from_dict(df)
    obs = scen.timeseries()
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])

    scen.check_out(timeseries_only=True)
    df = {'region': ['World', 'World'],
          'variable': ['Testing', 'Testing'],
          'unit': ['???', '???'], 'year': [2010, 2020],
          'value': [23.7, 23.8]}
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit('testing of editing timeseries (same years)')

    scen.check_out(timeseries_only=True)
    df = {'region': ['World', 'World', 'World'],
          'variable': ['Testing', 'Testing', 'Testing2'],
          'unit': ['???', '???', '???'], 'year': [2020, 2030, 2030],
          'value': [24.8, 24.9, 25.1]}
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit('testing of editing timeseries (other years)')
    mp.close_db()

    mp = ixmp.Platform(test_mp_props)
    scen = ixmp.TimeSeries(mp, *test_args)
    obs = scen.timeseries().sort_values(by=['year'])
    df = df.append(exp.loc[0]).sort_values(by=['year'])
    npt.assert_array_equal(df[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(df['value'], obs['value'])
