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
msg_multiyear_args = ('canning problem (MESSAGE scheme)', 'multi-year')

# string columns for timeseries checks
iamc_idx_cols = ['model', 'scenario', 'region', 'variable', 'unit']
cols_str = ['region', 'variable', 'unit', 'year']


def test_scen_list(test_mp):
    scenario = test_mp.scenario_list(model='Douglas Adams')['scenario']
    assert scenario[0] == 'Hitchhiker'


def test_new_scen(test_mp):
    scen = test_mp.Scenario(*can_args, version='new')
    assert scen.version == 0


def test_default_version(test_mp):
    scen = test_mp.Scenario(*can_args)
    assert scen.version == 2


def test_init_par_35(test_mp):
    scen = test_mp.Scenario(*can_args, version='new')
    scen.init_set('ii')
    scen.init_par('new_par', idx_sets='ii')

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
    scen = test_mp.Scenario(*can_args, version=1)
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


def test_var_marginal(test_mp):
    scen = test_mp.Scenario(*can_args)
    df = scen.var('x', filters={'i': ['seattle']})
    npt.assert_array_almost_equal(df['mrg'], [0, 0, 0.036])


def test_var_level(test_mp):
    scen = test_mp.Scenario(*can_args)
    df = scen.var('x', filters={'i': ['seattle']})
    npt.assert_array_almost_equal(df['lvl'], [50, 300, 0])


def test_var_general_str(test_mp):
    scen = test_mp.Scenario(*can_args)
    df = scen.var('x', filters={'i': ['seattle']})
    npt.assert_array_equal(
        df['j'], ['new-york', 'chicago', 'topeka'])


def test_unit_list(test_mp):
    units = test_mp.units()
    assert ('cases' in units) is True


def test_add_unit(test_mp):
    test_mp.add_unit('test', 'just testing')


def test_par_filters_unit(test_mp):
    scen = test_mp.Scenario(*can_args)
    df = scen.par('d', filters={'i': ['seattle']})
    obs = df.loc[0, 'unit']
    exp = 'km'
    assert obs == exp


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
        df, ['transport_from_san-diego', 'transport_from_seattle'])
    scen2.discard_changes()


def test_add_cat_unique(test_mp):
    scen = test_mp.Scenario(*msg_multiyear_args)
    scen2 = scen.clone(keep_sol=False)
    scen2.check_out()
    scen2.add_cat('year', 'firstmodelyear', 2020, True)
    df = scen2.cat('year', 'firstmodelyear')
    npt.assert_array_equal(
        df, ['2020'])
    scen2.discard_changes()


def test_years_active(test_mp):
    scen = test_mp.Scenario(*msg_multiyear_args)
    df = scen.years_active('seattle', 'canning_plant', '2020')
    npt.assert_array_equal(df, [2020, 2030])


def test_years_active_extend(test_mp):
    scen = test_mp.Scenario(*msg_multiyear_args)
    scen = scen.clone(keep_sol=False)
    scen.check_out()
    scen.add_set('year', ['2040', '2050'])
    scen.add_par('duration_period', '2040', 10, 'y')
    scen.add_par('duration_period', '2050', 10, 'y')
    df = scen.years_active('seattle', 'canning_plant', '2020')
    npt.assert_array_equal(df, [2020, 2030, 2040])
    scen.discard_changes()


def test_new_timeseries(test_mp):
    scen = test_mp.TimeSeries(*test_args, version='new', annotation='testing')
    df = {'year': [2010, 2020], 'value': [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df['region'] = 'World'
    df['variable'] = 'Testing'
    df['unit'] = '???'
    scen.add_timeseries(df)
    scen.commit('importing a testing timeseries')


def test_new_timeseries_error(test_mp):
    scen = test_mp.TimeSeries(*test_args, version='new', annotation='testing')
    df = {'year': [2010, 2020], 'value': [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df['region'] = 'World'
    df['variable'] = 'Testing'
    pytest.raises(ValueError, scen.add_timeseries, df)


def test_get_timeseries(test_mp):
    scen = test_mp.TimeSeries(*test_args, version=2)
    obs = scen.timeseries(regions='World', variables='Testing', units='???',
                          years=2020)
    df = {'region': ['World'], 'variable': ['Testing'], 'unit': ['???'],
          'year': [2020], 'value': [23.6]}
    exp = pd.DataFrame.from_dict(df)
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])


def test_get_timeseries_iamc(test_mp):
    scen = test_mp.TimeSeries(*test_args, version=2)
    obs = scen.timeseries(iamc=True, regions='World', variables='Testing')
    df = {'year': [2010, 2020], 'value': [23.5, 23.6]}
    df = pd.DataFrame.from_dict(df)
    df['model'] = 'Douglas Adams'
    df['scenario'] = 'Hitchhiker'
    df['region'] = 'World'
    df['variable'] = 'Testing'
    df['unit'] = '???'
    df = df.pivot_table(index=iamc_idx_cols, columns='year')['value']
    df.reset_index(inplace=True)
    
    exp = pd.DataFrame.from_dict(df)
    npt.assert_array_equal(exp[iamc_idx_cols], obs[iamc_idx_cols])
    npt.assert_array_almost_equal(exp[2010], obs[2010])


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
