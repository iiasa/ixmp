from contextlib import contextmanager

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import ixmp
from ixmp.core import IAMC_IDX
from ixmp.testing import populate_test_platform


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
        model='model name',
        scenario='scenario name',
    )),
    2030: pd.DataFrame.from_dict(dict(
        region='World',
        variable=['Testing', 'Testing', 'Testing2'],
        unit='???',
        year=[2020, 2030, 2030],
        value=[24.8, 24.9, 25.1],
        model='model name',
        scenario='scenario name',
    )),
    2050: pd.DataFrame.from_dict(dict(
        region='World',
        variable='Testing',
        unit='???',
        year=[2000, 2010, 2020, 2030, 2040, 2050],
        value=[21.7, 22.7, 23.7, 24.7, 25.7, 25.8],
        model='model name',
        scenario='scenario name',
    )),
}


# Fixtures
@pytest.fixture(scope='class')
def mp(test_mp):
    populate_test_platform(test_mp)
    yield test_mp


def expected(df, ts):
    """Modify *df* with the 'model' and 'scenario' name from *ts."""
    return df.assign(model=ts.model, scenario=ts.scenario)


def wide(df):
    """Transform *df* from long to wide format."""
    return df.pivot_table(index=IAMC_IDX, columns='year', values='value') \
             .reset_index() \
             .rename_axis(columns=None)


@contextmanager
def transact(ts, condition=True, commit_message=''):
    """Context manager to wrap in a 'transaction'.

    If *condition* is True, the :class:`.TimeSeries`/:class:`.Scenario` *ts* is
    checked out *before* the block begins, and afterwards a commit is made with
    the *commit_message*. If *condition* is False, nothing occurs.
    """
    if condition:
        ts.check_out()
    try:
        yield ts
    finally:
        if condition:
            ts.commit(commit_message)


class TestTimeSeries:
    @pytest.fixture(scope='function')
    def ts(self, request, mp):
        """An empty TimeSeries with a temporary name on the test_mp."""
        # Use a hash of the pytest node ID to avoid exceeding the maximum
        # length for a scenario name
        node = hash(request.node.nodeid.replace('/', ' '))
        yield ixmp.TimeSeries(mp, model=f'test-{node}', scenario='test',
                              version='new')

    @pytest.mark.parametrize('format', ['long', 'wide'])
    def test_add_timeseries(self, ts, format):
        data = DATA[0] if format == 'long' else wide(DATA[0])

        # Data added
        ts.add_timeseries(data)
        ts.commit('')

        # Error: column 'unit' is missing
        with pytest.raises(ValueError):
            ts.add_timeseries(DATA[0].drop('unit', axis=1))

    @pytest.mark.parametrize('format', ['long', 'wide'])
    def test_get(self, ts, format):
        data = DATA[0] if format == 'long' else wide(DATA[0])

        ts.add_timeseries(data)
        ts.commit('')

        exp = expected(data, ts)
        args = {}

        if format == 'wide':
            args['iamc'] = True

        # Data can be retrieved and has the expected value
        assert_frame_equal(exp, ts.timeseries(**args))

    @pytest.mark.parametrize('format', ['long', 'wide'])
    def test_edit(self, mp, ts, format):
        """Tests that data can be overwritten."""
        data = expected(DATA[0], ts)
        all_data = [data.loc[0:0, :]]
        args = {}

        if format == 'wide':
            data = wide(data)
            args['iamc'] = True

        ts.add_timeseries(data)
        ts.commit('initial data')

        # Overwrite existing data
        with transact(ts, commit_message='overwrite existing data'):
            ts.add_timeseries(data)

        df = expected(DATA[2030], ts)
        all_data.append(df)
        if format == 'wide':
            df = wide(df)

        # Overwrite and add new values at once
        with transact(ts, commit_message='overwrite and add data'):
            ts.add_timeseries(df)

        # Close and re-open database
        mp.close_db()
        mp.open_db()

        # All four rows are retrieved
        exp = pd.concat(all_data).reset_index(drop=True)
        if format == 'wide':
            exp = wide(exp)
        assert_frame_equal(exp, ts.timeseries(**args))

    def test_edit_with_region_synonyms(self, mp, ts):
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

    @pytest.mark.parametrize('commit', [
        pytest.param(True),
        pytest.param(
            False,
            marks=pytest.mark.xfail(reason='TimeSeries must be checked in to '
                                           'retrieve data.')),
    ])
    def test_remove(self, mp, ts, commit):
        df = expected(DATA[2050], ts)

        ts.add_timeseries(DATA[2050])
        ts.commit('')

        if not commit:
            ts.check_out()

        assert_frame_equal(df, ts.timeseries())

        # Remove a single data point
        with transact(ts, commit):
            ts.remove_timeseries(df[df.year == 2010])

        # Expected data remains
        exp = df[df.year != 2010].reset_index(drop=True)
        assert_frame_equal(exp, ts.timeseries())

        # Remove two data points
        with transact(ts, commit):
            ts.remove_timeseries(df[df.year.isin([2030, 2050])])

        # Expected data remains
        exp = df[~df.year.isin([2010, 2030, 2050])].reset_index(drop=True)
        assert_frame_equal(exp, ts.timeseries())

        # Remove all remaining data
        with transact(ts, commit):
            ts.remove_timeseries(df)

        # Result is empty
        assert ts.timeseries().empty
