import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import pytest

import ixmp
from ixmp.core import IAMC_IDX
from ixmp.testing import populate_test_platform


test_args = ('Douglas Adams', 'Hitchhiker')

# string columns and dataframe for timeseries checks
cols_str = ['region', 'variable', 'unit', 'year']

TS_DF = pd.DataFrame.from_dict(dict(
    region='World', variable='Testing', unit='???', year=[2010, 2020],
    value=[23.7, 23.8]))


# Fixtures
@pytest.fixture(scope='class')
def mp(test_mp):
    populate_test_platform(test_mp)
    yield test_mp


@pytest.fixture(scope='class')
def ts(mp):
    yield ixmp.TimeSeries(mp, model='Douglas Adams', scenario='Hitchhiker')


# Assertions
def assert_timeseries(ts, exp=TS_DF):
    obs = ts.timeseries(region='World')
    assert_frame_equal(exp[cols_str], obs[cols_str])
    assert_series_equal(exp['value'], obs['value'])


def test_get_timeseries(ts):
    assert_timeseries(ts)


def test_get_timeseries_iamc(ts):
    obs = ts.timeseries(region='World', variable='Testing', iamc=True)

    exp = TS_DF.assign(model='Douglas Adams', scenario='Hitchhiker') \
        .pivot_table(index=IAMC_IDX, columns='year')['value'] \
        .reset_index() \
        .rename_axis(columns=None)

    assert_frame_equal(exp, obs)


def test_new_timeseries_as_year_value(test_mp):
    ts = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    ts.add_timeseries(TS_DF)
    ts.commit('importing a testing timeseries')
    assert_timeseries(ts)


def test_new_timeseries_as_iamc(test_mp):
    ts = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    ts.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    ts.commit('importing a testing timeseries')
    assert_timeseries(ts)


def test_new_timeseries_error(test_mp):
    ts = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    df = {'year': [2010, 2020], 'value': [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df['region'] = 'World'
    df['variable'] = 'Testing'
    # column `unit` is missing
    pytest.raises(ValueError, ts.add_timeseries, df)


def test_timeseries_edit(mp, ts):
    df = {'region': ['World'] * 2, 'variable': ['Testing'] * 2,
          'unit': ['???', '???'], 'year': [2010, 2020], 'value': [23.7, 23.8]}
    exp = pd.DataFrame.from_dict(df)
    obs = ts.timeseries()
    assert_frame_equal(exp[cols_str], obs[cols_str])
    assert_series_equal(exp['value'], obs['value'])

    ts.check_out(timeseries_only=True)
    df = {'region': ['World'] * 2,
          'variable': ['Testing'] * 2,
          'unit': ['???', '???'], 'year': [2010, 2020],
          'value': [23.7, 23.8]}
    df = pd.DataFrame.from_dict(df)
    ts.add_timeseries(df)
    ts.commit('testing of editing timeseries (same years)')

    ts.check_out(timeseries_only=True)
    df = {'region': ['World'] * 3,
          'variable': ['Testing', 'Testing', 'Testing2'],
          'unit': ['???', '???', '???'], 'year': [2020, 2030, 2030],
          'value': [24.8, 24.9, 25.1]}
    df = pd.DataFrame.from_dict(df)
    ts.add_timeseries(df)
    ts.commit('testing of editing timeseries (other years)')
    mp.close_db()

    mp.open_db()
    ts = ixmp.TimeSeries(mp, *test_args)
    obs = ts.timeseries().sort_values(by=['year'])
    df = df.append(exp.loc[0]) \
        .sort_values(by=['year']) \
        .reset_index()
    assert_frame_equal(df[cols_str], obs[cols_str])
    assert_series_equal(df['value'], obs['value'])


def test_timeseries_edit_iamc(test_mp):
    args_all = ('Douglas Adams 1', 'test_remove_all')
    ts = ixmp.TimeSeries(test_mp, *args_all, version='new', annotation='nk')

    df = pd.DataFrame.from_dict({'region': ['World'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2010': [23.7],
                                 '2020': [23.8]})
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    ts = ixmp.TimeSeries(test_mp, *args_all)
    obs = ts.timeseries()
    exp = pd.DataFrame.from_dict({'region': ['World', 'World'],
                                  'variable': ['Testing', 'Testing'],
                                  'unit': ['???', '???'],
                                  'year': [2010, 2020],
                                  'value': [23.7, 23.8]})
    assert_frame_equal(exp[cols_str], obs[cols_str])
    assert_series_equal(exp['value'], obs['value'])

    ts.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict({'region': ['World'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2000': [21.7],
                                 '2010': [22.7],
                                 '2020': [23.7],
                                 '2030': [24.7],
                                 '2040': [25.7],
                                 '2050': [25.8]})
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    exp = pd.DataFrame.from_dict(
        {'region': ['World'] * 6,
         'variable': ['Testing'] * 6,
         'unit': ['???'] * 6,
         'year': [2000, 2010, 2020, 2030, 2040, 2050],
         'value': [21.7, 22.7, 23.7, 24.7, 25.7, 25.8]})
    obs = ts.timeseries()
    assert_frame_equal(exp[cols_str], obs[cols_str])
    assert_series_equal(exp['value'], obs['value'])
    test_mp.close_db()


def test_timeseries_edit_with_region_synonyms(test_mp):
    args_all = ('Douglas Adams 1', 'test_remove_all')
    test_mp.set_log_level('DEBUG')
    test_mp.add_region_synonym('Hell', 'World')
    ts = ixmp.TimeSeries(test_mp, *args_all, version='new', annotation='nk')

    df = pd.DataFrame.from_dict({'region': ['World'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2010': [23.7],
                                 '2020': [23.8]})
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    ts = ixmp.TimeSeries(test_mp, *args_all)
    obs = ts.timeseries()
    exp = pd.DataFrame.from_dict({'region': ['World'] * 2,
                                  'variable': ['Testing'] * 2,
                                  'unit': ['???', '???'],
                                  'year': [2010, 2020],
                                  'value': [23.7, 23.8]})
    assert_frame_equal(exp[cols_str], obs[cols_str])
    assert_series_equal(exp['value'], obs['value'])

    ts.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict({'region': ['Hell'],
                                 'variable': ['Testing'],
                                 'unit': ['???'],
                                 '2000': [21.7],
                                 '2010': [22.7],
                                 '2020': [23.7],
                                 '2030': [24.7],
                                 '2040': [25.7],
                                 '2050': [25.8]})
    ts.preload_timeseries()
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    exp = pd.DataFrame.from_dict(
        {'region': ['World'] * 6,
         'variable': ['Testing'] * 6,
         'unit': ['???'] * 6,
         'year': [2000, 2010, 2020, 2030, 2040, 2050],
         'value': [21.7, 22.7, 23.7, 24.7, 25.7, 25.8]})
    obs = ts.timeseries()
    assert_frame_equal(exp[cols_str], obs[cols_str])
    assert_series_equal(exp['value'], obs['value'])
    test_mp.close_db()


def test_timeseries_remove_single_entry(test_mp):
    args_single = ('Douglas Adams', 'test_remove_single')

    ts = ixmp.TimeSeries(test_mp, *args_single, version='new', annotation='fo')
    ts.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    ts.commit('importing a testing timeseries')

    ts = ixmp.TimeSeries(test_mp, *args_single)
    assert_timeseries(ts, TS_DF)

    ts.check_out()
    ts.remove_timeseries(TS_DF[TS_DF.year == 2010])
    ts.commit('testing for removing a single timeseries data point')

    exp = TS_DF[TS_DF.year == 2020].reset_index()
    assert_timeseries(ts, exp)


def test_timeseries_remove_all_data(test_mp):
    args_all = ('Douglas Adams', 'test_remove_all')

    ts = ixmp.TimeSeries(test_mp, *args_all, version='new', annotation='fo')
    ts.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    ts.commit('importing a testing timeseries')

    ts = ixmp.TimeSeries(test_mp, *args_all)
    assert_timeseries(ts, TS_DF)

    exp = TS_DF.copy()
    exp['variable'] = 'Testing2'

    ts.check_out()
    ts.add_timeseries(exp)
    ts.remove_timeseries(TS_DF)
    ts.commit('testing for removing a full timeseries row')

    assert ts.timeseries(region='World', variable='Testing').empty
    assert_timeseries(ts, exp)
