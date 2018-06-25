import os

import pandas as pd
import pytest
from numpy import testing as npt

import ixmp
from ixmp.default_path_constants import CONFIG_PATH

test_args = ('Douglas Adams', 'Hitchhiker')
can_args = ('canning problem', 'standard')

# string columns for timeseries checks
iamc_idx_cols = ['model', 'scenario', 'region', 'variable', 'unit']
cols_str = ['region', 'variable', 'unit', 'year']


def local_config_exists():
    return os.path.exists(CONFIG_PATH)


@pytest.mark.skipif(local_config_exists(),
                    reason='will not overwrite local config files')
def test_default_dbprops_file(test_mp_use_default_dbprops_file):
    test_mp = test_mp_use_default_dbprops_file
    scenario = test_mp.scenario_list(model='Douglas Adams')['scenario']
    assert scenario[0] == 'Hitchhiker'


@pytest.mark.skipif(local_config_exists(),
                    reason='will not overwrite local config files')
def test_db_config_path(test_mp_use_db_config_path):
    test_mp = test_mp_use_db_config_path
    scenario = test_mp.scenario_list(model='Douglas Adams')['scenario']
    assert scenario[0] == 'Hitchhiker'


def test_platform_init_raises():
    pytest.raises(ValueError, ixmp.Platform, dbtype='foo')


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


def test_meta(test_mp):
    test_dict = {
        "test_string": 'test12345',
        "test_number": 123.456,
        "test_number_negative": -123.456,
        'test_int': 12345,
        'test_bool': True,
        'test_bool_false': False,
    }

    scen = test_mp.Scenario(*can_args, version=1)
    for k, v in test_dict.items():
        scen.set_meta(k, v)

    # test all
    obs_dict = scen.get_meta()
    for k, exp in test_dict.items():
        obs = obs_dict[k]
        assert obs == exp

    # test name
    obs = scen.get_meta('test_string')
    exp = test_dict['test_string']
    assert obs == exp


def test_load_scenario_data(test_mp):
    scen = test_mp.Scenario(*can_args, cache=True)
    scen.load_scenario_data()
    assert ('par', 'd') in scen._pycache  # key exists
    df = scen.par('d', filters={'i': ['seattle']})
    obs = df.loc[0, 'unit']
    exp = 'km'
    assert obs == exp


def test_load_scenario_data_clear_cache(test_mp):
    # this fails on commit: 4376f54
    scen = test_mp.Scenario(*can_args, cache=True)
    scen.load_scenario_data()
    scen.clear_cache(name='d')


def test_load_scenario_data_raises(test_mp):
    scen = test_mp.Scenario(*can_args, cache=False)
    pytest.raises(ValueError, scen.load_scenario_data)
