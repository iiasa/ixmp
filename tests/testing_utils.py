import os
import shutil
import tempfile

import pandas as pd

here = os.path.dirname(os.path.realpath(__file__))


def tempdir():
    return os.path.join(tempfile._get_default_tempdir(),
                        next(tempfile._get_candidate_names()))


def create_local_testdb():
    # copy testdb
    dst = tempdir()
    test_props = os.path.join(dst, 'test.properties')
    src = os.path.join(here, 'testdb')
    shutil.copytree(src, dst)

    # create properties file
    fname = os.path.join(here, 'testdb', 'test.properties_template')
    with open(fname, 'r') as f:
        lines = f.read()
        lines = lines.format(here=dst.replace("\\", "/"))
    with open(test_props, 'w') as f:
        f.write(lines)

    return test_props


def make_scenario(platform):
    # details for creating a new scenario in the IX modeling platform
    model = "canning problem"
    scenario = "standard"
    annot = "Dantzig's transportation problem for illustration and testing"

    # initialize a new scenario instance
    scen = platform.Scenario(model, scenario, version='new', annotation=annot)

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
