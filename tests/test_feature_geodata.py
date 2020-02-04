import ixmp
from numpy import testing as npt
import pandas as pd

test_args = ('Douglas Adams', 'Hitchhiker')

GEODATA = {
    'region': 'World',
    'variable': 'var1',
    'subannual': 'Year',
    'year': [2000, 2010, 2020],
    'value': ['test', 'more-test', '2020-test'],
    'unit': 'score',
    'meta': 0
}
GEODATA = pd.DataFrame.from_dict(GEODATA)


def test_fetch_empty_geodata(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    empty = scen.get_geodata()
    assert_geodata(empty, GEODATA.loc[[False, False, False]])


def test_add_geodata(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    scen.add_geodata(GEODATA)
    scen.commit('adding geodata (references to map layers)')
    assert_geodata(scen.get_geodata(), GEODATA)


def test_remove_geodata(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    scen.add_geodata(GEODATA)
    row = GEODATA.loc[[False, True, False]]
    scen.remove_geodata(row)
    scen.commit('adding geodata (references to map layers)')
    assert_geodata(scen.get_geodata(), GEODATA.loc[[True, False, True]])


def test_remove_multiple_geodata(test_mp):
    scen = ixmp.TimeSeries(test_mp, *test_args, version='new', annotation='fo')
    scen.add_geodata(GEODATA)
    row = GEODATA.loc[[False, True, True]]
    scen.remove_geodata(row)
    scen.commit('adding geodata (references to map layers)')
    assert_geodata(scen.get_geodata(), GEODATA.loc[[True, False, False]])


def assert_geodata(obs, exp):
    obs = obs.sort_values('year')
    exp = exp.sort_values('year')
    for column in obs.columns:
        npt.assert_array_equal(exp.get(column), obs.get(column))
