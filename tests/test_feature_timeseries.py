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
    obs = scen.timeseries(region='World', variable='Testing', iamc=True)

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


def assert_timeseries(scen, exp=TS_DF):
    obs = scen.timeseries(region='World')
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])


def test_new_timeseries_error(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    df = {'year': [2010, 2020], 'value': [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df['region'] = 'World'
    df['variable'] = 'Testing'
    # column `unit` is missing
    pytest.raises(ValueError, scen.add_timeseries, df)


def test_timeseries_edit(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args)
    df = {'region': ['World'] * 2, 'variable': ['Testing'] * 2,
          'unit': ['???', '???'], 'year': [2010, 2020], 'value': [23.7, 23.8]}
    exp = pd.DataFrame.from_dict(df)
    obs = scen.timeseries()
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])

    scen.check_out(timeseries_only=True)
    df = {'region': ['World'] * 2,
          'variable': ['Testing'] * 2,
          'unit': ['???', '???'], 'year': [2010, 2020],
          'value': [23.7, 23.8]}
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit('testing of editing timeseries (same years)')

    scen.check_out(timeseries_only=True)
    df = {'region': ['World'] * 3,
          'variable': ['Testing', 'Testing', 'Testing2'],
          'unit': ['???', '???', '???'], 'year': [2020, 2030, 2030],
          'value': [24.8, 24.9, 25.1]}
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit('testing of editing timeseries (other years)')
    test_mp.close_db()

    test_mp.open_db()
    scen = ixmp.TimeSeries(test_mp, *test_args)
    obs = scen.timeseries().sort_values(by=['year'])
    df = df.append(exp.loc[0]).sort_values(by=['year'])
    npt.assert_array_equal(df[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(df['value'], obs['value'])


def test_timeseries_edit_iamc(test_mp):
    args_all = ('Douglas Adams 1', 'test_remove_all')
    scen = ixmp.TimeSeries(test_mp, *args_all, version='new', annotation='nk')

    df = pd.DataFrame.from_dict({'region': ['World'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2010': [23.7],
                                 '2020': [23.8]})
    scen.add_timeseries(df)
    scen.commit('updating timeseries in IAMC format')

    scen = ixmp.TimeSeries(test_mp, *args_all)
    obs = scen.timeseries()
    exp = pd.DataFrame.from_dict({'region': ['World', 'World'],
                                  'variable': ['Testing', 'Testing'],
                                  'unit': ['???', '???'],
                                  'year': [2010, 2020],
                                  'value': [23.7, 23.8]})
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])

    scen.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict({'region': ['World'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2000': [21.7],
                                 '2010': [22.7],
                                 '2020': [23.7],
                                 '2030': [24.7],
                                 '2040': [25.7],
                                 '2050': [25.8]})
    scen.add_timeseries(df)
    scen.commit('updating timeseries in IAMC format')

    exp = pd.DataFrame.from_dict(
        {'region': ['World'] * 6,
         'variable': ['Testing'] * 6,
         'unit': ['???'] * 6,
         'year': [2000, 2010, 2020, 2030, 2040, 2050],
         'value': [21.7, 22.7, 23.7, 24.7, 25.7, 25.8]})
    obs = scen.timeseries()
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])
    test_mp.close_db()


def test_timeseries_edit_with_region_synonyms(test_mp):
    args_all = ('Douglas Adams 1', 'test_remove_all')
    test_mp.set_log_level('DEBUG')
    test_mp.add_region_synomym('Hell', 'World')
    scen = ixmp.TimeSeries(test_mp, *args_all, version='new', annotation='nk')

    df = pd.DataFrame.from_dict({'region': ['World'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2010': [23.7],
                                 '2020': [23.8]})
    scen.add_timeseries(df)
    scen.commit('updating timeseries in IAMC format')

    scen = ixmp.TimeSeries(test_mp, *args_all)
    obs = scen.timeseries()
    exp = pd.DataFrame.from_dict({'region': ['World'] * 2,
                                  'variable': ['Testing'] * 2,
                                  'unit': ['???', '???'],
                                  'year': [2010, 2020],
                                  'value': [23.7, 23.8]})
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])

    scen.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict({'region': ['Hell'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2000': [21.7],
                                 '2010': [22.7],
                                 '2020': [23.7],
                                 '2030': [24.7],
                                 '2040': [25.7],
                                 '2050': [25.8]})
    scen.preload_timeseries()
    scen.add_timeseries(df)
    scen.commit('updating timeseries in IAMC format')

    exp = pd.DataFrame.from_dict(
        {'region': ['World'] * 6,
         'variable': ['Testing'] * 6,
         'unit': ['???'] * 6,
         'year': [2000, 2010, 2020, 2030, 2040, 2050],
         'value': [21.7, 22.7, 23.7, 24.7, 25.7, 25.8]})
    obs = scen.timeseries()
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])
    test_mp.close_db()


def test_timeseries_remove_single_entry(test_mp):
    args_single = ('Douglas Adams', 'test_remove_single')

    scen = ixmp.Scenario(test_mp, *args_single, version='new', annotation='fo')
    scen.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    scen.commit('importing a testing timeseries')

    scen = ixmp.Scenario(test_mp, *args_single)
    assert_timeseries(scen, TS_DF)

    scen.check_out()
    scen.remove_timeseries(TS_DF[TS_DF.year == 2010])
    scen.commit('testing for removing a single timeseries data point')

    exp = TS_DF[TS_DF.year == 2020]
    assert_timeseries(scen, exp)


def test_timeseries_remove_all_data(test_mp):
    args_all = ('Douglas Adams', 'test_remove_all')

    scen = ixmp.Scenario(test_mp, *args_all, version='new', annotation='fo')
    scen.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    scen.commit('importing a testing timeseries')

    scen = ixmp.Scenario(test_mp, *args_all)
    assert_timeseries(scen, TS_DF)

    exp = TS_DF.copy()
    exp['variable'] = 'Testing2'

    scen.check_out()
    scen.add_timeseries(exp)
    scen.remove_timeseries(TS_DF)
    scen.commit('testing for removing a full timeseries row')

    assert scen.timeseries(region='World', variable='Testing').empty
    assert_timeseries(scen, exp)
