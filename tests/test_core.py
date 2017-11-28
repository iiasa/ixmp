import os

import pandas as pd

from numpy import testing as npt
import pandas.util.testing as pdt

import ixmp

import pytest

from testing_utils import test_mp, test_mp_props

test_args = ('Douglas Adams', 'Hitchhiker')
can_args = ('canning problem', 'standard')
msg_args = ('canning problem (MESSAGE scheme)', 'standard')
aut_args = ('Austrian energy model', 'baseline')

# string columns for timeseries checks
cols_str = ['region', 'variable', 'unit', 'year']


def test_new_scen(test_mp):
    scen = test_mp.Scenario(*can_args, version='new')
    assert scen.version == 0


def test_default_version(test_mp):
    scen = test_mp.Scenario(*can_args)
    assert scen.version == 2


def test_get_scalar(test_mp):
    scen = test_mp.Scenario(*can_args)
    obs = scen.scalar('f')
    exp = {'unit': 'USD/km', 'value': 90}
    assert obs == exp


def test_init_scalar(test_mp):
    scen = test_mp.Scenario(*can_args)
    scen2 = scen.clone(keep_sol=False)
    scen2.check_out()
    scen2.init_scalar('g', 90.0, 'USD/km')
    scen2.commit("adding a scalar 'g'")


# make sure that changes to a scenario are copied over during clone
def test_add_clone(test_mp):
    scen = test_mp.Scenario(*can_args)
    scen.check_out()
    scen.init_set('h')
    scen.add_set('h', 'test')
    scen.commit("adding an index set 'h', wiht element 'test'")

    scen2 = scen.clone(keep_sol=False)
    obs = scen2.set('h')
    npt.assert_array_equal(obs, ['test'])


# make sure that (only) the correct scenario is touched after cloning
def test_clone_edit(test_mp):
    scen = test_mp.Scenario(*can_args)
    scen2 = scen.clone(keep_sol=False)
    scen2.check_out()
    scen2.change_scalar('f', 95.0, 'USD/km')
    scen2.commit('change transport cost')
    obs = scen.scalar('f')
    exp = {'unit': 'USD/km', 'value': 90}
    assert obs == exp
    obs = scen2.scalar('f')
    exp = {'unit': 'USD/km', 'value': 95}
    assert obs == exp


def test_idx_name(test_mp):
    scen = test_mp.Scenario(*can_args)
    df = scen.idx_names('d')
    npt.assert_array_equal(df, ['i', 'j'])


def test_remote_unit_export(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.par('input', filters={'technology': ['transport_from_seattle']})
    obs = df.loc[0, 'unit']
    exp = '%'
    assert obs == exp


def test_remote_unit_can(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.par('bound_activity_up',
                filters={'technology': ['canning_plant']})
    obs = df.loc[0, 'unit']
    exp = 'cases'
    assert obs == exp


def test_remote_marginal(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.var('ACT', filters={'technology': ['transport_from_seattle']})
    npt.assert_array_almost_equal(df['mrg'], [0, 0, 0.036])


def test_remote_level(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.var('ACT', filters={'technology': ['transport_from_seattle']})
    npt.assert_array_almost_equal(df['lvl'], [50, 300, 0])


def test_remote_general_int(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.var('ACT', filters={'technology': ['transport_from_seattle']})
    npt.assert_array_almost_equal(df['year_vtg'], [2010, 2010, 2010])


def test_remote_general_str(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.var('ACT', filters={'technology': ['transport_from_seattle']})
    npt.assert_array_equal(
        df['mode'], ['to_new-york', 'to_chicago', 'to_topeka'])


def test_cat_all(test_mp):
    scen = test_mp.Scenario(*msg_args)
    df = scen.cat('technology', 'all')
    npt.assert_array_equal(df, ['canning_plant', 'transport_from_seattle',
                                'transport_from_san-diego'])


def test_add_cat(test_mp):
    scen = test_mp.Scenario(*msg_args)
    scen2 = scen.clone(keep_sol=False)
    scen2.check_out()
    scen2.add_cat('technology', 'trade',
                ['transport_from_san-diego', 'transport_from_seattle'])
    df = scen2.cat('technology', 'trade')
    npt.assert_array_equal(
        df, ['transport_from_san-diego', 'transport_from_seattle', ])
    scen2.discard_changes()


def test_add_cat_unique(test_mp):
    scen = test_mp.Scenario(*aut_args)
    scen2 = scen.clone(keep_sol=False)
    scen2.check_out()
    scen2.add_cat('year', 'firstmodelyear', 2020, True)
    df = scen2.cat('year', 'firstmodelyear')
    npt.assert_array_equal(
        df, ['2020'])
    scen2.discard_changes()


def test_years_active(test_mp):
    scen = test_mp.Scenario(*aut_args)
    df = scen.years_active('Austria', 'gas_ppl', '2020')
    npt.assert_array_equal(df, [2020, 2030, 2040])


def test_years_active_extend(test_mp):
    scen = test_mp.Scenario(*aut_args)
    scen = scen.clone(keep_sol=False)
    scen.check_out()
    scen.add_set('year', '2070')
    scen.add_par('duration_period', '2070', 10, 'y')
    df = scen.years_active('Austria', 'gas_ppl', '2050')
    npt.assert_array_equal(df, [2050, 2060, 2070])
    scen.discard_changes()


def test_unit_list(test_mp):
    units = test_mp.units()
    assert ('cases' in units) is True


def test_add_unit(test_mp):
    test_mp.add_unit('test', 'just testing')


def test_new_timeseries(test_mp):
    scen = test_mp.TimeSeries(*test_args, version='new', annotation='testing')
    df = {'region': ['World'], 'variable': ['Testing'], 'unit': ['???'],
          'year': [2020], 'value': [23.6]}
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit('importing a testing timeseries')


def test_get_timeseries(test_mp):
    scen = test_mp.TimeSeries(*test_args, version=2)
    obs = scen.timeseries()
    df = {'region': ['World'], 'variable': ['Testing'], 'unit': ['???'],
          'year': [2020], 'value': [23.6]}
    exp = pd.DataFrame.from_dict(df)
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])


def test_timeseries_edit(test_mp_props):
    mp = ixmp.Platform(test_mp_props)
    scen = mp.TimeSeries(*test_args)
    df = {'region': ['World', 'World'], 'variable': ['Testing', 'Testing'],
          'unit': ['???', '???'], 'year': [2010, 2020], 'value': [23.7, 23.8]}
    exp = pd.DataFrame.from_dict(df)
    obs = scen.timeseries()
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])

    scen.check_out(timeseries_only=True)
    df = {'region': ['World', 'World', 'World'],
          'variable': ['Testing', 'Testing', 'Testing2'],
          'unit': ['???', '???', '???'], 'year': [2020, 2030, 2030],
          'value': [24.8, 24.9, 25.1]}
    df = pd.DataFrame.from_dict(df)
    scen.add_timeseries(df)
    scen.commit('testing of editing timeseries')
    mp.close_db()

    mp = ixmp.Platform(test_mp_props)
    scen = mp.TimeSeries(*test_args)
    obs = scen.timeseries().sort_values(by=['year'])
    df = df.append(exp.loc[0]).sort_values(by=['year'])
    npt.assert_array_equal(df[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(df['value'], obs['value'])


def test_clone_slice(test_mp):
    scen = test_mp.Scenario(*aut_args)
    scen2 = scen.clone(keep_sol=True, first_model_year=2030)

    # check that the solution was not dropped
    assert (scen2.var('ACT').empty) is False

    # other checks for cloning and slicing
    clone_slice_test(scen2)


def test_clone_slice_drop(test_mp):
    scen = test_mp.Scenario(*aut_args)
    scen2 = scen.clone(keep_sol=False, first_model_year=2030)

    # check that the solution was removed
    assert (scen2.var('ACT').empty) is True

    # other checks for cloning and slicing
    clone_slice_test(scen2)


def clone_slice_test(scen2):
        # check that historical activity is correctly assigned during slicing
    obs = scen2.par('historical_activity',
                    filters={'technology': 'coal_ppl', 'year_act': 2020}).value
    exp = 1.335842
    npt.assert_array_almost_equal(obs, exp)

    # check that the first model year identifier was correctly shifted
    fy = scen2.cat('year', 'firstmodelyear')
    assert fy == '2030'

    # check that the timeseries data was correctly copied
    # (entire horizon for meta-timeseries, up to new firstmodelyear for others)
    ts = scen2.timeseries()
    horizon = range(2010, 2070, 10)
    gdp = pd.Series([1., 1.2163, 1.4108, 1.63746, 1.89083, 2.1447],
                    index=horizon)
    data = {'variable': 'GDP', 'year': horizon, 'value': gdp,
            'unit': 'million USD', 'region': 'Austria'}
    df = pd.DataFrame.from_dict(data)
    npt.assert_array_almost_equal(ts[ts.variable == 'GDP'].value, df.value)

    history = [2010, 2020]
    beta = 0.7
    demand = gdp ** beta
    data = {'variable': 'Demand', 'year': history, 'value': demand[history],
            'unit': 'GWa/y', 'region': 'Austria'}
    df = pd.DataFrame.from_dict(data)
    npt.assert_array_almost_equal(ts[ts.variable == 'Demand'].value, df.value)
