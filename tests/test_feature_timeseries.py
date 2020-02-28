import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import ixmp
from ixmp.core import IAMC_IDX
from ixmp.testing import populate_test_platform


test_args = dict(model='Douglas Adams', scenario='Hitchhiker')

# string columns and dataframe for timeseries checks
cols_str = ['region', 'variable', 'unit', 'year']


# Test data.
# NB the columns are in a specific order; model and scenario come last in the
#    data returned by ixmp.
# TODO fix this; model and scenario should come first, matching the IAMC order.
TS_DF = pd.DataFrame.from_dict(dict(
    region='World',
    variable='Testing',
    unit='???',
    year=[2010, 2020],
    value=[23.7, 23.8],
    **test_args
))

TS_2050 = pd.DataFrame.from_dict(dict(
    region='World',
    variable='Testing',
    unit='???',
    year=[2000, 2010, 2020, 2030, 2040, 2050],
    value=[21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
    **test_args
))


# Fixtures
@pytest.fixture(scope='class')
def mp(test_mp):
    populate_test_platform(test_mp)
    yield test_mp


@pytest.fixture(scope='class')
def ts(mp):
    yield ixmp.TimeSeries(mp, **test_args)


@pytest.fixture(scope='function')
def ts_empty(request, mp):
    """An empty TimeSeries with a temporary name on the test_mp."""
    yield ixmp.TimeSeries(mp, model=request.node.nodeid.replace('/', ' '),
                          scenario='test', version='new')


def test_get_timeseries(ts):
    assert_frame_equal(TS_DF, ts.timeseries())


def test_get_timeseries_iamc(ts):
    obs = ts.timeseries(region='World', variable='Testing', iamc=True)

    exp = TS_DF.pivot_table(index=IAMC_IDX, columns='year', values='value') \
        .reset_index() \
        .rename_axis(columns=None)

    assert_frame_equal(exp, obs)


def test_new_timeseries_as_year_value(test_mp):
    ts = ixmp.TimeSeries(test_mp, **test_args, version='new')
    ts.add_timeseries(TS_DF)
    ts.commit('importing a testing timeseries')
    assert_frame_equal(TS_DF, ts.timeseries())


def test_new_timeseries_as_iamc(test_mp):
    ts = ixmp.TimeSeries(test_mp, **test_args, version='new')
    ts.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    ts.commit('importing a testing timeseries')
    assert_frame_equal(TS_DF, ts.timeseries())


def test_new_timeseries_error(ts_empty):
    # Error: column 'unit' is missing
    with pytest.raises(ValueError):
        ts_empty.add_timeseries(TS_DF.drop('unit', axis=1))


def test_timeseries_edit(mp, ts):
    # Overwrite existing data
    ts.check_out(timeseries_only=True)
    ts.add_timeseries(TS_DF)
    ts.commit('testing of editing timeseries (same years)')

    # Overwrite and add new values at once
    ts.check_out(timeseries_only=True)
    df = pd.DataFrame.from_dict(dict(
        **test_args,
        region='World',
        variable=['Testing', 'Testing', 'Testing2'],
        unit='???',
        year=[2020, 2030, 2030],
        value=[24.8, 24.9, 25.1]))
    ts.add_timeseries(df)
    ts.commit('testing of editing timeseries (other years)')

    # Close and re-open database
    mp.close_db()
    mp.open_db()

    # All four rows are retrieved
    exp = pd.concat([TS_DF.loc[0:0, :], df]).reset_index(drop=True)
    print(exp)
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_edit_iamc(test_mp):
    args_all = ('Douglas Adams 1', 'test_remove_all')
    ts = ixmp.TimeSeries(test_mp, *args_all, version='new')

    ts.add_timeseries(TS_DF)
    ts.commit('updating timeseries in IAMC format')

    ts = ixmp.TimeSeries(test_mp, *args_all)
    exp = TS_DF.replace({'model': {'Douglas Adams': 'Douglas Adams 1'},
                         'scenario': {'Hitchhiker': 'test_remove_all'}})
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out(timeseries_only=True)
    df = TS_2050.pivot_table(index=IAMC_IDX, columns='year', values='value') \
                .reset_index()
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    exp = TS_2050.replace({'model': {'Douglas Adams': 'Douglas Adams 1'},
                           'scenario': {'Hitchhiker': 'test_remove_all'}})
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_edit_with_region_synonyms(test_mp):
    args_all = ('Douglas Adams 1', 'test_remove_all')
    test_mp.set_log_level('DEBUG')
    test_mp.add_region_synonym('Hell', 'World')
    ts = ixmp.TimeSeries(test_mp, *args_all, version='new')

    ts.add_timeseries(TS_DF)
    ts.commit('updating timeseries in IAMC format')

    ts = ixmp.TimeSeries(test_mp, *args_all)
    exp = TS_DF.replace({'model': {'Douglas Adams': 'Douglas Adams 1'},
                         'scenario': {'Hitchhiker': 'test_remove_all'}})
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out(timeseries_only=True)
    df = TS_2050.pivot_table(index=IAMC_IDX, columns='year', values='value') \
                .reset_index() \
                .replace('World', 'Hell')
    ts.preload_timeseries()
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    exp = TS_2050.replace({'model': {'Douglas Adams': 'Douglas Adams 1'},
                           'scenario': {'Hitchhiker': 'test_remove_all'}})
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_remove_single_entry(test_mp):
    args_single = ('Douglas Adams', 'test_remove_single')

    exp = TS_DF.replace('Hitchhiker', 'test_remove_single')

    ts = ixmp.TimeSeries(test_mp, *args_single, version='new')
    ts.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    ts.commit('importing a testing timeseries')

    ts = ixmp.TimeSeries(test_mp, *args_single)
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out()
    ts.remove_timeseries(TS_DF[TS_DF.year == 2010])
    ts.commit('testing for removing a single timeseries data point')

    exp = exp[exp.year == 2020].reset_index(drop=True)
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_remove_all_data(test_mp):
    args_all = ('Douglas Adams', 'test_remove_all')

    exp = TS_DF.replace('Hitchhiker', 'test_remove_all')

    ts = ixmp.TimeSeries(test_mp, *args_all, version='new')
    ts.add_timeseries(TS_DF.pivot_table(values='value', index=cols_str))
    ts.commit('importing a testing timeseries')

    ts = ixmp.TimeSeries(test_mp, *args_all)
    assert_frame_equal(exp, ts.timeseries())

    exp = exp.assign(variable='Testing2')

    ts.check_out()
    ts.add_timeseries(exp)
    ts.remove_timeseries(TS_DF)
    ts.commit('testing for removing a full timeseries row')

    assert ts.timeseries(region='World', variable='Testing').empty
    assert_frame_equal(exp, ts.timeseries())
