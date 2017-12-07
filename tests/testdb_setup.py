# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:58:52 2017

@author: huppmann
"""

import os
import pandas as pd
import itertools
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
fname = os.path.join(ix.default_paths.TEST_DIR, 'transport_ixmp.gms')
scen.solve(model=fname, case='transport_standard')


# %% initialize the Austria example

model = "Austrian energy model"
scenario = "baseline"
annot = "developing a stylized energy system model for Austria"

scen = mp.Scenario(model, scenario, version='new', annotation=annot,
                   scheme="MESSAGE")

horizon = range(2010, 2070, 10)
firstyear = horizon[0]

scen.add_set("year", horizon)
scen.add_set("cat_year", ["firstmodelyear", firstyear])

country = "Austria"
scen.add_set("node", country)
scen.add_set("lvl_spatial", "country")
scen.add_set("map_spatial_hierarchy", ["country", country, "World"])
scen.add_set("mode", "standard")

scen.add_set("commodity", ["electricity", "light", "other_electricity"])
scen.add_set("level", ["secondary", "final", "useful"])

rate = [0.05] * len(horizon)
unit = ['%'] * len(horizon)
scen.add_par("interestrate", key=horizon, val=rate, unit=unit)

beta = 0.7
gdp = pd.Series([1., 1.2163, 1.4108, 1.63746, 1.89083, 2.1447], index=horizon)
demand = gdp ** beta

plants = [
    "coal_ppl",
    "gas_ppl",
    "oil_ppl",
    "bio_ppl",
    "hydro_ppl",
    "wind_ppl",
    "solar_pv_ppl"  # actually primary -> final
]
secondary_energy_tecs = plants + ['import']

final_energy_tecs = ['electricity_grid']

lights = [
    "bulb",
    "cfl"
]
useful_energy_tecs = lights + ['appliances']

technologies = secondary_energy_tecs + final_energy_tecs + useful_energy_tecs
scen.add_set("technology", technologies)

demand_per_year = 55209. / 8760  # from IEA statistics
elec_demand = pd.DataFrame({
        'node': country,
        'commodity': 'other_electricity',
        'level': 'useful',
        'year': horizon,
        'time': 'year',
        'value': demand_per_year * demand,
        'unit': 'GWa',
    })
scen.add_par("demand", elec_demand)

demand_per_year = 6134. / 8760  # from IEA statistics
light_demand = pd.DataFrame({
        'node': country,
        'commodity': 'light',
        'level': 'useful',
        'year': horizon,
        'time': 'year',
        'value': demand_per_year * demand,
        'unit': 'GWa',
    })
scen.add_par("demand", light_demand)

year_pairs = [(y_v, y_a) for y_v, y_a in
              itertools.product(horizon, horizon) if y_v <= y_a]
vintage_years, act_years = zip(*year_pairs)

base_input = {
    'node_loc': country,
    'year_vtg': vintage_years,
    'year_act': act_years,
    'mode': 'standard',
    'node_origin': country,
    'commodity': 'electricity',
    'time': 'year',
    'time_origin': 'year',
}

grid = pd.DataFrame(dict(
        technology='electricity_grid',
        level='secondary',
        value=1.0,
        unit='%',
        **base_input
        ))
scen.add_par("input", grid)


bulb = pd.DataFrame(dict(
        technology='bulb',
        level='final',
        value=1.0,
        unit='%',
        **base_input
        ))
scen.add_par("input", bulb)

cfl = pd.DataFrame(dict(
        technology='cfl',
        level='final',
        value=0.3,
        unit='%',
        **base_input
        ))
scen.add_par("input", cfl)

app = pd.DataFrame(dict(
        technology='appliances',
        level='final',
        value=1.0,
        unit='%',
        **base_input
        ))
scen.add_par("input", app)


def make_df(base, **kwargs):
    base.update(kwargs)
    return pd.DataFrame(base)


base_output = {
    'node_loc': country,
    'year_vtg': vintage_years,
    'year_act': act_years,
    'mode': 'standard',
    'node_dest': country,
    'time': 'year',
    'time_dest': 'year',
    'unit': '%',
}

imports = make_df(base_output, technology='import', commodity='electricity',
                  level='secondary', value=1.)
scen.add_par('output', imports)

grid = make_df(base_output, technology='electricity_grid',
               commodity='electricity', level='final', value=0.873)
scen.add_par('output', grid)

bulb = make_df(base_output, technology='bulb', commodity='light',
               level='useful', value=1.)
scen.add_par('output', bulb)

cfl = make_df(base_output, technology='cfl', commodity='light',
              level='useful', value=1.)
scen.add_par('output', cfl)

app = make_df(base_output, technology='appliances',
              commodity='other_electricity', level='useful', value=1.)
scen.add_par('output', app)

coal = make_df(base_output, technology='coal_ppl', commodity='electricity',
               level='secondary', value=1.)
scen.add_par('output', coal)

gas = make_df(base_output, technology='gas_ppl', commodity='electricity',
              level='secondary', value=1.)
scen.add_par('output', gas)

oil = make_df(base_output, technology='oil_ppl', commodity='electricity',
              level='secondary', value=1.)
scen.add_par('output', oil)

bio = make_df(base_output, technology='bio_ppl', commodity='electricity',
              level='secondary', value=1.)
scen.add_par('output', bio)

hydro = make_df(base_output, technology='hydro_ppl', commodity='electricity',
                level='secondary', value=1.)
scen.add_par('output', hydro)

wind = make_df(base_output, technology='wind_ppl',
               commodity='electricity', level='secondary', value=1.)
scen.add_par('output', wind)

solar_pv = make_df(base_output, technology='solar_pv_ppl',
                   commodity='electricity', level='final', value=1.)
scen.add_par('output', solar_pv)

base_technical_lifetime = {
    'node_loc': country,
    'year_vtg': horizon,
    'unit': 'y',
}

lifetimes = {
    'coal_ppl': 40,
    'gas_ppl': 30,
    'oil_ppl': 30,
    'bio_ppl': 30,
    'hydro_ppl': 60,
    'wind_ppl': 20,
    'solar_pv_ppl': 20,
    'bulb': 1,
    'cfl': 10,
}

for tec, val in lifetimes.items():
    df = make_df(base_technical_lifetime, technology=tec, value=val)
    scen.add_par('technical_lifetime', df)

base_capacity_factor = {
    'node_loc': country,
    'year_vtg': vintage_years,
    'year_act': act_years,
    'time': 'year',
    'unit': '%',
}

capacity_factor = {
    'coal_ppl': 0.85,
    'gas_ppl': 0.75,
    'oil_ppl': 0.75,
    'bio_ppl': 0.75,
    'hydro_ppl': 0.5,
    'wind_ppl': 0.2,
    'solar_pv_ppl': 0.15,
    'bulb': 0.1,
    'cfl':  0.1,
}

for tec, val in capacity_factor.items():
    df = make_df(base_capacity_factor, technology=tec, value=val)
    scen.add_par('capacity_factor', df)

base_inv_cost = {
    'node_loc': country,
    'year_vtg': horizon,
    'unit': 'USD/GWa',
}

# in $ / kW
costs = {
    'coal_ppl': 1500,
    'gas_ppl':  870,
    'oil_ppl':  950,
    'hydro_ppl': 3000,
    'bio_ppl':  1600,
    'wind_ppl': 1100,
    'solar_pv_ppl': 4000,
    'bulb': 5,
    'cfl':  900,
}

for tec, val in costs.items():
    df = make_df(base_inv_cost, technology=tec, value=val * 1e6)
    scen.add_par('inv_cost', df)

base_fix_cost = {
    'node_loc': country,
    'year_vtg': vintage_years,
    'year_act': act_years,
    'unit': 'USD/GWa',
}

# in $ / kW
costs = {
    'coal_ppl': 40,
    'gas_ppl':  25,
    'oil_ppl':  25,
    'hydro_ppl': 60,
    'bio_ppl':  30,
    'wind_ppl': 40,
    'solar_pv_ppl': 25,
}

for tec, val in costs.items():
    df = make_df(base_fix_cost, technology=tec, value=val * 1e6)
    scen.add_par('fix_cost', df)

base_var_cost = {
    'node_loc': country,
    'year_vtg': vintage_years,
    'year_act': act_years,
    'mode': 'standard',
    'time': 'year',
    'unit': 'USD/GWa',
}

# in $ / MWh
costs = {
    'coal_ppl': 24.4,
    'gas_ppl':  42.4,
    'oil_ppl':  77.8,
    'bio_ppl':  48.2,
    'electricity_grid': 47.8,
}

for tec, val in costs.items():
    df = make_df(base_var_cost, technology=tec, value=val * 8760. * 1e3)
    scen.add_par('var_cost', df)

base_growth = {
    'node_loc': country,
    'year_act': horizon[1:],
    'value': 0.05,
    'time': 'year',
    'unit': '%',
}

growth_technologies = [
    "coal_ppl",
    "gas_ppl",
    "oil_ppl",
    "bio_ppl",
    "hydro_ppl",
    "wind_ppl",
    "solar_pv_ppl",
    "cfl",
    "bulb",
]

for tec in growth_technologies:
    df = make_df(base_growth, technology=tec)
    scen.add_par('growth_activity_up', df)

base_initial = {
    'node_loc': country,
    'year_act': horizon[1:],
    'time': 'year',
    'unit': '%',
}

for tec in lights:
    df = make_df(base_initial, technology=tec,
                 value=0.01 * light_demand['value'].loc[horizon[1:]])
    scen.add_par('initial_activity_up', df)

base_activity = {
    'node_loc': country,
    'year_act': [2010],
    'mode': 'standard',
    'time': 'year',
    'unit': 'GWa',
}

# in GWh - from IEA Electricity Output
activity = {
    'coal_ppl': 7184,
    'gas_ppl':  14346,
    'oil_ppl':  1275,
    'hydro_ppl': 38406,
    'bio_ppl':  4554,
    'wind_ppl': 2064,
    'solar_pv_ppl': 89,
    'import': 2340,
    'cfl': 0,
}

for tec, val in activity.items():
    df = make_df(base_activity, technology=tec, value=val / 8760.)
    scen.add_par('bound_activity_up', df)
    scen.add_par('bound_activity_lo', df)

base_activity = {
    'node_loc': country,
    'year_act': horizon[1:],
    'mode': 'standard',
    'time': 'year',
    'unit': 'GWa',
}

# in GWh - base value from IEA Electricity Output
keep_activity = {
    'hydro_ppl': 38406,
    'bio_ppl':  4554,
    'import': 2340,
}

for tec, val in keep_activity.items():
    df = make_df(base_activity, technology=tec, value=val / 8760.)
    scen.add_par('bound_activity_up', df)

base_capacity = {
    'node_loc': country,
    'year_vtg': [2010],
    'unit': 'GWa',
}

cf = pd.Series(capacity_factor)
act = pd.Series(activity)
capacity = (act / 8760 / cf).dropna().to_dict()

for tec, val in capacity.items():
    df = make_df(base_capacity, technology=tec, value=val)
    scen.add_par('bound_new_capacity_up', df)

scen.add_set("emission", "CO2")
scen.add_cat('emission', 'GHGs', 'CO2')

base_emissions = {
    'node_loc': country,
    'year_vtg': vintage_years,
    'year_act': act_years,
    'mode': 'standard',
    'unit': 'kg/kWa'  # actually is tCO2/GWa
}

# units: tCO2/MWh
emissions = {
    'coal_ppl': ('CO2', 0.854),
    'gas_ppl':  ('CO2', 0.339),
    'oil_ppl':  ('CO2', 0.57),
}

for tec, (species, val) in emissions.items():
    df = make_df(base_emissions, technology=tec, emission=species,
                 value=val * 8760. * 1000)
    scen.add_par('emission_factor', df)

comment = 'initial commit for Austria model'
scen.commit(comment)
scen.set_as_default()

scen.check_out()
# add timeseries data for GDP as meta-data (will not be dropped during cloning)
data = {'variable': 'GDP', 'year': horizon, 'value': gdp,
        'unit': 'million USD', 'region': 'Austria'}
df = pd.DataFrame.from_dict(data)
scen.add_timeseries(df, meta=True)

# add timeseries data for GDP as meta-data (will not be dropped during cloning)
data = {'variable': 'Demand', 'year': horizon, 'value': demand,
        'unit': 'GWa/y', 'region': 'Austria'}
df = pd.DataFrame.from_dict(data)
scen.add_timeseries(df)
scen.commit('add timeseries meta data (gdp) and demand')


# %% initialize a new scenario using the canning problem
# based on the scheme "MESSAGE"

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


# %% close the test database, remove the test database properties file

mp.close_db()
