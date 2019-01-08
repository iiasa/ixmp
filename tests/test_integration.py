import numpy as np
import pandas.util.testing as pdt

import ixmp
from ixmp.testing import dantzig_transport


def test_run_gams_api(tmpdir, test_data_path):
    # this test is designed to cover the full functionality of the GAMS API
    # - creates a new scenario and exports a gdx file
    # - runs the tutorial transport model
    # - reads back the solution from the output
    # - performs the test on the objective value
    mp = ixmp.Platform(tmpdir, dbtype='HSQLDB')
    scen = dantzig_transport(mp, solve=test_data_path)

    # test it
    obs = scen.var('z')['lvl']
    exp = 153.675
    assert np.isclose(obs, exp)


def scenario_list(mp):
    return mp.scenario_list(default=False)[['model', 'scenario']]


def assert_multi_db(mp1, mp2):
    pd.testing.assert_frame_equal(scenario_list(mp1), scenario_list(mp2))


def test_multi_db_run(tmpdir, test_data_path):
    mp1 = ixmp.Platform(tmpdir / 'mp1', dbtype='HSQLDB')
    scen1 = dantzig_transport(mp1, solve=test_data_path)

    mp2 = ixmp.Platform(tmpdir / 'mp2', dbtype='HSQLDB')
    scen2 = scen1.clone(platform=mp2, keep_solution=False)
    scen2.solve(model=str(test_data_path / 'transport_ixmp'))

    assert scen1.var('z') == scen2.var('z')
    assert_multi_db(mp1, mp2)


def test_multi_db_edit_source(tmpdir):
    mp1 = ixmp.Platform(tmpdir / 'mp1', dbtype='HSQLDB')
    scen1 = dantzig_transport(mp1)

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
    mp1 = ixmp.Platform(tmpdir / 'mp1', dbtype='HSQLDB')
    scen1 = dantzig_transport(mp1)

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
