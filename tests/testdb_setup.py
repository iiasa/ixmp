import os
import pandas as pd
import ixmp

from testing_utils import here

db_dir = os.path.join(here, 'testdb')
test_db = os.path.join(db_dir, 'ixmptest')

# %% remove existing database files

for fname in [
        os.path.join(db_dir, 'ixmptest.lobs'),
        os.path.join(db_dir, 'ixmptest.properties'),
        os.path.join(db_dir, 'ixmptest.script')
]:
    if os.path.isfile(fname):
        os.remove(fname)

# %% launch the modeling platform instance, creating a new test database file

mp = ixmp.Platform(dbprops=test_db, dbtype='HSQLDB')

# %% initialize a new timeseries instance

ts = ixmp.TimeSeries(mp, 'Douglas Adams', 'Hitchhiker',
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
scen = ixmp.Scenario(mp, model, scenario, version='new', annotation=annot)
scen.commit('this is an empty scenario')

# %% initialize another version (for more testing - version 2 - default)

scen = ixmp.Scenario(mp, model, scenario, version='new', annotation=annot)

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
    {'j': "chicago", 'value': 300, 'unit': "cases"},
    {'j': "topeka", 'value': 275, 'unit': "cases"}
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
fname = os.path.join(here, 'transport_ixmp')
scen.solve(model=fname, case='transport_standard')


# %% close the test database, remove the test database properties file

mp.close_db()
