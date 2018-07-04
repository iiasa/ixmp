import os

import numpy as np
import pandas.util.testing as pdt

import ixmp

from testing_utils import tempdir, make_scenario, solve_scenario


def test_run_gams_api():
    # this test is designed to cover the full functionality of the GAMS API
    # - creates a new scenario and exports a gdx file
    # - runs the tutorial transport model
    # - reads back the solution from the output
    # - performs the test on the objective value
    mp = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    scen = make_scenario(mp)
    solve_scenario(scen)

    # test it
    obs = scen.var('z')['lvl']
    exp = 153.675
    assert np.isclose(obs, exp)


def test_multi_db_run():
    mp1 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    scen1 = make_scenario(mp1)
    solve_scenario(scen1)

    mp2 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    scen2 = scen1.clone(platform=mp2, keep_sol=False)
    solve_scenario(scen2)

    assert scen1.var('z') == scen2.var('z')


def test_multi_db_edit_source():
    mp1 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    scen1 = make_scenario(mp1)

    mp2 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
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


def test_multi_db_edit_target():
    mp1 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    scen1 = make_scenario(mp1)

    mp2 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
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
