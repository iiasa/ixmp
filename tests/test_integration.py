import os

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

import ixmp

from testing_utils import tempdir


def make_scenario(platform):
    # details for creating a new scenario in the IX modeling platform
    model = "canning problem"
    scenario = "standard"
    annot = "Dantzig's transportation problem for illustration and testing"

    # initialize a new scenario instance
    scen = ixmp.Scenario(platform, model, scenario,
                         version='new', annotation=annot)

    # define the sets of locations of canning plants and markets
    scen.init_set("i")
    scen.add_set("i", ["seattle", "san-diego"])
    scen.init_set("j")
    scen.add_set("j", ["new-york", "chicago", "topeka"])

    # capacity of plant i in case
    # add parameter elements one-by-one (string and value)
    scen.init_par("a", idx_sets="i")
    scen.add_par("a", "seattle", 350, "cases")
    scen.add_par("a", "san-diego", 600, "cases")

    # demand at market j in cases
    # add parameter elements as dataframe (with index names)
    scen.init_par("b", idx_sets="j")
    b_data = [
        {'j': "new-york", 'value': 325, 'unit': "cases"},
        {'j': "chicago", 'value': 300, 'unit': "cases"},
        {'j': "topeka", 'value': 275, 'unit': "cases"}
    ]
    b = pd.DataFrame(b_data)
    scen.add_par("b", b)

    # distance in thousands of miles
    scen.init_par("d", idx_sets=["i", "j"])
    d_data = [
        {'i': "seattle", 'j': "new-york", 'value': 2.5, 'unit': "km"},
        {'i': "seattle", 'j': "chicago", 'value': 1.7, 'unit': "km"},
    ]
    d = pd.DataFrame(d_data)
    scen.add_par("d", d)

    # add more parameter elements as dataframe by index names
    d_data = [
        {'i': "seattle", 'j': "topeka", 'value': 1.8, 'unit': "km"},
        {'i': "san-diego", 'j': "new-york", 'value': 2.5, 'unit': "km"},
    ]
    d = pd.DataFrame(d_data)
    scen.add_par("d", d)

    # add other parameter elements as key list, value, unit
    scen.add_par("d", ["san-diego", "chicago"], 1.8, "km")
    scen.add_par("d", ["san-diego", "topeka"], 1.4, "km")

    # cost per case per 1000 miles
    # initialize scalar with a value and a unit (and optionally a comment)
    scen.init_scalar("f", 90.0, "USD/km")

    # initialize the decision variables and equations
    scen.init_var("z", None, None)
    scen.init_var("x", idx_sets=["i", "j"])
    scen.init_equ("demand", idx_sets=["j"])

    # save changes to database
    comment = "creating Dantzig's transport problem for unit test"
    scen.commit(comment)

    return scen


def solve_scenario(scen):
    here = os.path.dirname(os.path.abspath(__file__))
    fname = os.path.join(here, 'transport_ixmp')
    scen.solve(model=fname)


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
