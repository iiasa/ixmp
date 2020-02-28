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
DATA = {
    0: pd.DataFrame.from_dict(dict(
        region='World',
        variable='Testing',
        unit='???',
        year=[2010, 2020],
        value=[23.7, 23.8],
        **test_args
    )),
    2050: pd.DataFrame.from_dict(dict(
        region='World',
        variable='Testing',
        unit='???',
        year=[2000, 2010, 2020, 2030, 2040, 2050],
        value=[21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
        **test_args
    )),
}


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
    # Use a hash of the pytest node ID to avoid exceeding the maximum length
    # for a scenario name
    node = hash(request.node.nodeid.replace('/', ' '))
    yield ixmp.TimeSeries(mp, model=f'test-{node}', scenario='test',
                          version='new')


def expected(df, ts):
    """Modify *df* with the 'model' and 'scenario' name from *ts."""
    return df.assign(model=ts.model, scenario=ts.scenario)


def wide(df):
    return df.pivot_table(index=IAMC_IDX, columns='year', values='value') \
             .reset_index()


def test_get_timeseries(ts):
    assert_frame_equal(DATA[0], ts.timeseries())


def test_get_timeseries_iamc(ts):
    exp = wide(DATA[0]).rename_axis(columns=None)
    assert_frame_equal(exp, ts.timeseries(iamc=True))


def test_new_timeseries_as_year_value(test_mp):
    ts = ixmp.TimeSeries(test_mp, **test_args, version='new')
    ts.add_timeseries(DATA[0])
    ts.commit('importing a testing timeseries')
    assert_frame_equal(DATA[0], ts.timeseries())


def test_new_timeseries_as_iamc(test_mp):
    ts = ixmp.TimeSeries(test_mp, **test_args, version='new')
    ts.add_timeseries(wide(DATA[0]))
    ts.commit('importing a testing timeseries')
    assert_frame_equal(DATA[0], ts.timeseries())


def test_new_timeseries_error(ts_empty):
    # Error: column 'unit' is missing
    with pytest.raises(ValueError):
        ts_empty.add_timeseries(DATA[0].drop('unit', axis=1))


def test_timeseries_edit(mp, ts):
    # Overwrite existing data
    ts.check_out(timeseries_only=True)
    ts.add_timeseries(DATA[0])
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
    exp = pd.concat([DATA[0].loc[0:0, :], df]).reset_index(drop=True)
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_edit_iamc(mp, ts_empty):
    ts = ts_empty
    info = dict(model=ts.model, scenario=ts.scenario)
    exp = expected(DATA[0], ts)

    ts.add_timeseries(DATA[0])
    ts.commit('updating timeseries in IAMC format')

    ts = ixmp.TimeSeries(mp, **info)
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out(timeseries_only=True)
    df = wide(DATA[2050])
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    exp = expected(DATA[2050], ts)
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_edit_with_region_synonyms(mp, ts_empty):
    ts = ts_empty
    info = dict(model=ts.model, scenario=ts.scenario)
    exp = expected(DATA[0], ts)

    mp.set_log_level('DEBUG')
    mp.add_region_synonym('Hell', 'World')

    ts.add_timeseries(DATA[0])
    ts.commit('updating timeseries in IAMC format')

    ts = ixmp.TimeSeries(mp, **info)
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out(timeseries_only=True)
    df = wide(DATA[2050]).replace('World', 'Hell')
    ts.preload_timeseries()
    ts.add_timeseries(df)
    ts.commit('updating timeseries in IAMC format')

    exp = expected(DATA[2050], ts)
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_remove_single_entry(mp, ts_empty):
    ts = ts_empty
    info = dict(model=ts.model, scenario=ts.scenario)
    exp = expected(DATA[0], ts)

    ts.add_timeseries(wide(DATA[0]))
    ts.commit('importing a testing timeseries')

    ts = ixmp.TimeSeries(mp, **info)
    assert_frame_equal(exp, ts.timeseries())

    ts.check_out()
    ts.remove_timeseries(exp[exp.year == 2010])
    ts.commit('testing for removing a single timeseries data point')

    exp = exp[exp.year == 2020].reset_index(drop=True)
    assert_frame_equal(exp, ts.timeseries())


def test_timeseries_remove_all_data(mp, ts_empty):
    ts = ts_empty
    info = dict(model=ts.model, scenario=ts.scenario)
    exp = expected(DATA[0], ts)

    ts.add_timeseries(wide(DATA[0]))
    ts.commit('importing a testing timeseries')

    ts = ixmp.TimeSeries(mp, **info)
    assert_frame_equal(exp, ts.timeseries())

    exp = exp.assign(variable='Testing2')

    ts.check_out()
    ts.add_timeseries(exp)
    ts.remove_timeseries(DATA[0])
    ts.commit('testing for removing a full timeseries row')

    assert ts.timeseries(region='World', variable='Testing').empty
    assert_frame_equal(exp, ts.timeseries())
