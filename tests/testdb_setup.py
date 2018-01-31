# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:58:52 2017

@author: huppmann
"""

import os
import pandas as pd
import ixmp as ix

db_dir = os.path.join(ix.default_paths.TEST_DIR, 'testdb')
test_db = os.path.join(db_dir, 'ixmptest')

# %% remove existing database files

lobs = os.path.join(db_dir, 'ixmptest.lobs')

if os.path.isfile(lobs):
    os.remove(lobs)
os.remove(os.path.join(db_dir, 'ixmptest.properties'))
os.remove(os.path.join(db_dir, 'ixmptest.script'))

# %% launch the modeling platform instance, creating a new test database file

mp = ix.Platform(dbprops=test_db, dbtype='HSQLDB')

# %% initialize a new timeseries instance

ts = mp.TimeSeries('Douglas Adams', 'Hitchhiker',
                   version='new', annotation='testing')
df = {'region': ['World', 'World'], 'variable': ['Testing', 'Testing'],
      'unit': ['???', '???'], 'year': [2010, 2020], 'value': [23.7, 23.8]}
df = pd.DataFrame.from_dict(df)
ts.add_timeseries(df)
ts.commit('importing a testing timeseries')
ts.set_as_default()

# %% initialize the standard canning problem

model = "canning problem"
scenario = "standard"
annot = "Dantzig's transportation problem for illustration and testing"

# initialize an empty version (for timeseries import testing - version 1)
scen = mp.Scenario(model, scenario, version='new', annotation=annot)
scen.commit('this is an empty scenario')

# %% initialize another version (for more testing - version 2 - default)

scen = mp.Scenario(model, scenario, version='new', annotation=annot)

# define the sets of locations of canning plants and markets
scen.init_set("i")
scen.add_set("i", ["seattle", "san-diego"])
scen.init_set("j")
scen.add_set("j", ["new-york", "chicago", "topeka"])

# capacity of plant i in cases
# add parameter elements one-by-one (string and value)
scen.init_par("a", idx_sets="i")
scen.add_par("a", "seattle", 350, "cases")
scen.add_par("a", "san-diego", 600, "cases")

# demand at market j in cases
# add parameter elements as dataframe (with index names)
scen.init_par("b", idx_sets="j")
b_data = [
    {'j': "new-york", 'value': 325, 'unit': "cases"},
    {'j': "chicago",  'value': 300, 'unit': "cases"},
    {'j': "topeka",   'value': 275, 'unit': "cases"}
]
b = pd.DataFrame(b_data)
scen.add_par("b", b)

# distance in thousands of miles
# add parameter elements as dataframe (with index names)
scen.init_par("d", idx_sets=["i", "j"])
d_data = [
    {'i': "seattle", 'j': "new-york", 'value': 2.5, 'unit': "km"},
    {'i': "seattle", 'j': "chicago", 'value': 1.7, 'unit': "km"},
    {'i': "seattle", 'j': "topeka", 'value': 1.8, 'unit': "km"},
    {'i': "san-diego", 'j': "new-york", 'value': 2.5, 'unit': "km"},
    {'i': "san-diego", 'j': "chicago", 'value': 1.8, 'unit': "km"},
    {'i': "san-diego", 'j': "topeka", 'value': 1.4, 'unit': "km"}
]
d = pd.DataFrame(d_data)
scen.add_par("d", d)

# cost per case per 1000 miles
# initialize scalar with a value and a unit (and optionally a comment)
scen.init_scalar("f", 90.0, "USD/km")

# add some timeseries for testing purposes
df = {'region': ['World'], 'variable': ['Testing'], 'unit': ['???'],
      'year': [2010], 'value': [23.7]}
df = pd.DataFrame.from_dict(df)
scen.add_timeseries(df)

# initialize the decision variables and equations
scen.init_var("z", None, None)
scen.init_var("x", idx_sets=["i", "j"])
scen.init_equ("demand", idx_sets=["j"])

comment = "importing Dantzig's transport problem for illustration"
comment += " and testing of the Python interface using a generic ixmp.Scenario"
scen.commit(comment)

# set this new scenario as the default version for the model/scenario name
scen.set_as_default()

# solve the model using the GAMS code provided in the `tests` folder
fname = os.path.join(ix.default_paths.TEST_DIR, 'transport_ixmp')
scen.solve(model=fname, case='transport_standard')


# %% initialize a new scenario using the canning problem
# based on the scheme "MESSAGE"
# this allows to test that all MESSAGE-specific functions in the Java core
# work as expected

model = "canning problem (MESSAGE scheme)"
scenario = "standard"
annot = "Dantzig's canning problem as a MESSAGE-scheme ixmp.Scenario"

scen = mp.Scenario(model, scenario, version='new',
                   scheme="MESSAGE", annotation=annot)

# year set and (sub-annual) time set
year = [2010]
scen.add_set("year", year)
scen.add_set("cat_year", ["firstmodelyear", 2010])

city = ["seattle", "san-diego", "new-york", "chicago", "topeka"]
scen.add_set("node", city)
scen.add_set("lvl_spatial", "location")

for item in city:
    scen.add_set("map_spatial_hierarchy", ["location", item, "World"])

scen.add_set("commodity", "cases")
scen.add_set("level", ["supply", "consumption"])

scen.add_set("technology", "canning_plant")

scen.add_set(
    "technology", ["transport_from_seattle", "transport_from_san-diego"])

scen.add_set(
    "mode", ["production", "to_new-york", "to_chicago", "to_topeka"])

scen.add_par("demand", ['new-york', 'cases', 'consumption', '2010', 'year'],
             325.0, "cases")
scen.add_par("demand", ['chicago', 'cases', 'consumption', '2010', 'year'],
             300.0, "cases")
scen.add_par("demand", ['topeka', 'cases', 'consumption', '2010', 'year'],
             275.0, "cases")
bda_data = [
    {'node_loc': "seattle",   'value': 350.0},
    {'node_loc': "san-diego", 'value': 600}
]
bda = pd.DataFrame(bda_data)

bda['technology'] = 'canning_plant'
bda['year_act'] = '2010'
bda['mode'] = 'production'
bda['time'] = 'year'
bda['unit'] = 'cases'

scen.add_par("bound_activity_up", bda)

outp_data = [
    {'node_loc': "seattle"},
    {'node_loc': "san-diego"}
]
outp = pd.DataFrame(outp_data)

outp['technology'] = 'canning_plant'
outp['year_vtg'] = '2010'
outp['year_act'] = '2010'
outp['mode'] = 'production'
outp['node_dest'] = outp['node_loc']
outp['commodity'] = 'cases'
outp['level'] = 'supply'
outp['time'] = 'year'
outp['time_dest'] = 'year'
outp['value'] = 1
outp['unit'] = '%'

scen.add_par("output", outp)

inp_data = [
    {'mode': "to_new-york"},
    {'mode': "to_chicago"},
    {'mode': "to_topeka"},
]
inp = pd.DataFrame(inp_data)

inp['node_loc'] = 'seattle'
inp['technology'] = 'transport_from_seattle'
inp['year_vtg'] = '2010'
inp['year_act'] = '2010'
inp['node_origin'] = 'seattle'
inp['commodity'] = 'cases'
inp['level'] = 'supply'
inp['time'] = 'year'
inp['time_origin'] = 'year'
inp['value'] = 1
inp['unit'] = '%'

scen.add_par("input", inp)

inp['node_loc'] = 'san-diego'
inp['technology'] = 'transport_from_san-diego'
inp['node_origin'] = 'san-diego'

scen.add_par("input", inp)

outp_data = [
    {'mode': "to_new-york", 'node_dest': "new-york"},
    {'mode': "to_chicago", 'node_dest': "chicago"},
    {'mode': "to_topeka", 'node_dest': "topeka"},
]
outp = pd.DataFrame(outp_data)

outp['node_loc'] = 'seattle'
outp['technology'] = 'transport_from_seattle'
outp['year_vtg'] = '2010'
outp['year_act'] = '2010'
outp['commodity'] = 'cases'
outp['level'] = 'consumption'
outp['time'] = 'year'
outp['time_dest'] = 'year'
outp['value'] = 1
outp['unit'] = '%'

scen.add_par("output", outp)

outp['node_loc'] = 'san-diego'
outp['technology'] = 'transport_from_san-diego'

scen.add_par("output", outp)

var_cost_data = [
    {'node_loc': "seattle", 'technology': "transport_from_seattle",
     'mode': "to_new-york", 'value': 0.225},
    {'node_loc': "seattle", 'technology': "transport_from_seattle",
     'mode': "to_chicago", 'value': 0.153},
    {'node_loc': "seattle", 'technology': "transport_from_seattle",
     'mode': "to_topeka", 'value': 0.162},
    {'node_loc': "san-diego", 'technology': "transport_from_san-diego",
     'mode': "to_new-york", 'value': 0.225},
    {'node_loc': "san-diego", 'technology': "transport_from_san-diego",
     'mode': "to_chicago", 'value': 0.162},
    {'node_loc': "san-diego", 'technology': "transport_from_san-diego",
     'mode': "to_topeka", 'value': 0.126},
]
var_cost = pd.DataFrame(var_cost_data)

var_cost['year_vtg'] = '2010'
var_cost['year_act'] = '2010'
var_cost['time'] = 'year'
var_cost['unit'] = 'USD'

scen.add_par("var_cost", var_cost)

scen.add_par("ref_activity",
             "seattle.canning_plant.2010.production.year", 350, "cases")
scen.add_par("ref_activity",
             "san-diego.canning_plant.2010.production.year", 600, "cases")

comment = "importing a MESSAGE-scheme version of the transport problem"
scen.commit(comment)
scen.set_as_default()


# %%  duplicate the MESSAGE-scheme transport scenario for additional unit tests

scen = mp.Scenario(model, scenario)
scen = scen.clone(scen='multi-year',
                  annotation='adding additional years for unit-testing')

scen.check_out()
scen.add_set('year', [2020, 2030])
scen.add_par('technical_lifetime', ['seattle', 'canning_plant', '2020'],
             30, 'y')
scen.commit('adding years and technical lifetime to one technology')


# %% close the test database, remove the test database properties file

mp.close_db()
