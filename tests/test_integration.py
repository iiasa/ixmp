import numpy as np
import pandas.util.testing as pdt

import ixmp
from ixmp.testing import dantzig_transport



MODEL = "canning problem"
SCENARIO = "standard"
TS_DF = pd.DataFrame(
    [[MODEL, SCENARIO, 'DantzigLand', 'Demand', 'cases', 900], ],
    columns=['model', 'scenario', 'region', 'variable', 'unit', 2005],
)


def make_scenario(mp):
    # create custom unit in the platform
    mp.add_unit('USD_per_km')
    mp.add_region('DantzigLand', 'country')

    # details for creating a new scenario in the IX modeling platform
    annot = "Dantzig's transportation problem for illustration and testing"

    # initialize a new scenario instance
    scen = ixmp.Scenario(mp, MODEL, SCENARIO,
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
    scen.init_scalar("f", 90.0, "USD_per_km")

    # initialize the decision variables and equations
    scen.init_var("z", None, None)
    scen.init_var("x", idx_sets=["i", "j"])
    scen.init_equ("demand", idx_sets=["j"])

    # add timeseries data for testing the clone across platforms
    scen.add_timeseries(TS_DF)

    # save changes to database
    comment = "creating Dantzig's transport problem for unit test"
    scen.commit(comment)

    return scen


def solve_scenario(scen):
    here = os.path.dirname(os.path.abspath(__file__))
    fname = os.path.join(here, 'transport_ixmp')
    scen.solve(model=fname)


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

    mp2 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    # add other unit to make sure that the mapping is correct during clone
    mp2.add_unit('wrong_unit')
    mp2.add_region('wrong_region', 'country')

    scen2 = scen1.clone(platform=mp2, keep_solution=False)
    scen2.solve(model=str(test_data_path / 'transport_ixmp'))

    assert scen1.var('z') == scen2.var('z')
    assert_multi_db(mp1, mp2)

    # check that custom unit and region are migrated correctly
    assert scen2.par('f')['unit'] == 'USD_per_km'
    obs = scen2.timeseries(iamc=True)
    pd.testing.assert_frame_equal(obs, TS_DF, check_dtype=False)


def test_multi_db_edit_source():
    mp1 = ixmp.Platform(tempdir(), dbtype='HSQLDB')
    scen1 = make_scenario(mp1)

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
