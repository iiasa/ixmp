import pytest
import numpy as np
import pandas.util.testing as pdt

import ixmp
from ixmp.testing import dantzig_transport, TS_DF, HIST_DF

TS_DF_CLEARED = TS_DF.copy()
TS_DF_CLEARED.loc[0, 2005] = np.nan


def test_run_clone(tmpdir, test_data_path):
    # this test is designed to cover the full functionality of the GAMS API
    # - creates a new scenario and exports a gdx file
    # - runs the tutorial transport model
    # - reads back the solution from the output
    # - performs the test on the objective value
    mp = ixmp.Platform(tmpdir, dbtype='HSQLDB')
    scen = dantzig_transport(mp, solve=test_data_path)
    assert np.isclose(scen.var('z')['lvl'], 153.675)
    pdt.assert_frame_equal(scen.timeseries(iamc=True), TS_DF)

    # cloning with `keep_solution=True` keeps all timeseries and the solution
    scen2 = scen.clone(keep_solution=True)
    assert np.isclose(scen2.var('z')['lvl'], 153.675)
    pdt.assert_frame_equal(scen2.timeseries(iamc=True), TS_DF)

    pdt.assert_frame_equal(scen2.timeseries(iamc=True), TS_DF)

    # cloning with `keep_solution=True` and `first_model_year` raises an error
    pytest.raises(ValueError, scen.clone, first_model_year=2005)

    # cloning with `keep_solution=False` drops the solution and only keeps
    # timeseries set as `meta=True`
    scen3 = scen.clone(keep_solution=False)
    assert np.isnan(scen3.var('z')['lvl'])
    pdt.assert_frame_equal(scen3.timeseries(iamc=True), HIST_DF)

    # cloning with `keep_solution=False` and `first_model_year`
    # drops the solution and removes all timeseries not marked `meta=True`
    # in the model horizon (i.e, `year >= first_model_year`)
    scen4 = scen.clone(keep_solution=False, first_model_year=2005)
    assert np.isnan(scen4.var('z')['lvl'])
    pdt.assert_frame_equal(scen4.timeseries(iamc=True), TS_DF_CLEARED)


def test_run_remove_solution(tmpdir, test_data_path):
    # create a new instance of the transport problem and solve it
    mp = ixmp.Platform(tmpdir, dbtype='HSQLDB')
    scen = dantzig_transport(mp, solve=test_data_path)
    assert np.isclose(scen.var('z')['lvl'], 153.675)

    # check that re-solving the model will raise an error if a solution exists
    pytest.raises(ValueError, scen.solve,
                  model=str(test_data_path / 'transport_ixmp'), case='fail')

    # remove the solution, check that variables are empty
    # and timeseries not marked `meta=True` are removed
    scen2 = scen.clone()
    scen2.remove_solution()
    assert not scen2.has_solution()
    assert np.isnan(scen2.var('z')['lvl'])
    pdt.assert_frame_equal(scen2.timeseries(iamc=True), HIST_DF)

    # remove the solution with a specific year as first model year, check that
    # variables are empty and timeseries not marked `meta=True` are removed
    scen3 = scen.clone()
    scen3.remove_solution(first_model_year=2005)
    assert not scen3.has_solution()
    assert np.isnan(scen3.var('z')['lvl'])
    pdt.assert_frame_equal(scen3.timeseries(iamc=True), TS_DF_CLEARED)


def scenario_list(mp):
    return mp.scenario_list(default=False)[['model', 'scenario']]


def assert_multi_db(mp1, mp2):
    pdt.assert_frame_equal(scenario_list(mp1), scenario_list(mp2))


def test_multi_db_run(tmpdir, test_data_path):
    # create a new instance of the transport problem and solve it
    mp1 = ixmp.Platform(tmpdir / 'mp1', dbtype='HSQLDB')
    scen1 = dantzig_transport(mp1)

    mp2 = ixmp.Platform(tmpdir / 'mp2', dbtype='HSQLDB')
    # add other unit to make sure that the mapping is correct during clone
    mp2.add_unit('wrong_unit')
    mp2.add_region('wrong_region', 'country')

    # check that cloning across platforms must copy the full solution
    pytest.raises(ValueError, scen1.clone, platform=mp2, keep_solution=False)

    # clone un-solved model across platforms (with default settings)
    scen2 = scen1.clone(platform=mp2)
    assert np.isnan(scen2.var('z')['lvl'])

    # check that custom unit, region and timeseries are migrated correctly
    assert scen2.par('f')['value'] == 90.0
    assert scen2.par('f')['unit'] == 'USD_per_km'
    pdt.assert_frame_equal(scen2.timeseries(iamc=True), TS_DF)


def test_multi_db_edit_source(tmpdir):
    # create a new instance of the transport problem
    mp1 = ixmp.Platform(tmpdir / 'mp1', dbtype='HSQLDB')
    scen1 = make_dantzig(mp1)

    mp2 = ixmp.Platform(tmpdir / 'mp2', dbtype='HSQLDB')
    scen2 = scen1.clone(platform=mp2)

    pdt.assert_frame_equal(scen1.par('d'), scen2.par('d'))

    scen1.check_out()
    scen1.add_par('d', ['san-diego', 'topeka'], 1.5, 'km')
    scen1.commit('foo')

    obs = (scen1
           .par('d')
           .set_index(['i', 'j'])
           .loc['san-diego', 'topeka']
           ['value']
           )
    exp = 1.5
    assert np.isclose(obs, exp)

    obs = (scen2
           .par('d')
           .set_index(['i', 'j'])
           .loc['san-diego', 'topeka']
           ['value']
           )
    exp = 1.4
    assert np.isclose(obs, exp)

    assert_multi_db(mp1, mp2)


def test_multi_db_edit_target(tmpdir):
    # create a new instance of the transport problem
    mp1 = ixmp.Platform(tmpdir / 'mp1', dbtype='HSQLDB')
    scen1 = make_dantzig(mp1)

    mp2 = ixmp.Platform(tmpdir / 'mp2', dbtype='HSQLDB')
    scen2 = scen1.clone(platform=mp2)

    pdt.assert_frame_equal(scen1.par('d'), scen2.par('d'))

    scen2.check_out()
    scen2.add_par('d', ['san-diego', 'topeka'], 1.5, 'km')
    scen2.commit('foo')

    obs = (scen2
           .par('d')
           .set_index(['i', 'j'])
           .loc['san-diego', 'topeka']
           ['value']
           )
    exp = 1.5
    assert np.isclose(obs, exp)

    obs = (scen1
           .par('d')
           .set_index(['i', 'j'])
           .loc['san-diego', 'topeka']
           ['value']
           )
    exp = 1.4
    assert np.isclose(obs, exp)

    assert_multi_db(mp1, mp2)
