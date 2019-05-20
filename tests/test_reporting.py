"""Tests for ixmp.reporting."""
import subprocess

import ixmp
import ixmp.reporting
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from xarray.testing import (
    assert_allclose as assert_xr_allclose,
    assert_equal as assert_xr_equal,
)

from ixmp.testing import dantzig_transport
from ixmp.reporting import Key, Reporter
from ixmp.reporting.computations import aggregate, product, ratio
from ixmp.reporting.utils import ureg


test_args = ('Douglas Adams', 'Hitchhiker')

TS_DF = {'year': [2010, 2020], 'value': [23.7, 23.8]}
TS_DF = pd.DataFrame.from_dict(TS_DF)
TS_DF['region'] = 'World'
TS_DF['variable'] = 'Testing'
TS_DF['unit'] = '???'


@pytest.fixture
def scenario(test_mp):
    # from test_feature_timeseries.test_new_timeseries_as_year_value
    scen = ixmp.Scenario(test_mp, *test_args, version='new', annotation='foo')
    scen.add_timeseries(TS_DF)
    scen.commit('importing a testing timeseries')
    return scen


def test_reporting_key():
    k1 = Key('foo', ['a', 'b', 'c'])

    # Representation
    assert repr(k1) == 'foo:a-b-c'

    # Key hashes the same as its string representation
    assert hash(k1) == hash('foo:a-b-c')

    # Key compares equal to its string representation
    assert k1 == 'foo:a-b-c'

    # Number of aggregates for a 3-dimensional quantity
    assert sum(1 for a in k1.aggregates()) == 7


def test_reporter(scenario):
    r = Reporter.from_scenario(scenario)

    r.finalize(scenario)

    assert 'scenario' in r.graph


def test_reporter_from_dantzig(test_mp, test_data_path):
    scen = dantzig_transport(test_mp, solve=test_data_path)

    # Reporter.from_scenario can handle the Dantzig problem
    rep = Reporter.from_scenario(scen)

    # Aggregates are available automatically (d is defined over i and j)
    d_i = rep.get('d:i')

    # Units pass through summation
    assert d_i.attrs['_unit'] == ureg.parse_units('km')

    # Aggregation with weights
    weights = xr.DataArray([1, 2, 3],
                           coords=['chicago new-york topeka'.split()],
                           dims=['j'])
    new_key = rep.aggregate('d:i-j', 'j', 'weighted', weights)

    # …produces the expected new key with the summed dimension removed and
    # tag added
    assert new_key == 'd:i:weighted'

    # …produces the expected new value
    assert_xr_equal(
        rep.get(new_key),
        (rep.get('d:i-j') * weights).sum(dim=['j']) / weights.sum(dim=['j'])
        )

    # Disaggregation with explicit data
    # (cases of canned food 'p'acked in oil or water)
    shares = xr.DataArray([0.8, 0.2], coords=[['oil', 'water']], dims=['p'])
    new_key = rep.disaggregate('b:j', 'p', args=[shares])

    # …produces the expected key with new dimension added
    assert new_key == 'b:j-p'

    b_jp = rep.get('b:j-p')

    # Units pass through disaggregation
    assert b_jp.attrs['_unit'] == 'cases'

    # Set elements are available
    assert rep.get('j') == ['new-york', 'chicago', 'topeka']

    # 'all' key retrieves all quantities
    names = set('a b d f demand demand-margin z x'.split())
    assert names == {da.name for da in rep.get('all')}


def test_reporter_read_config(test_mp, test_data_path):
    scen = dantzig_transport(test_mp)

    rep = Reporter.from_scenario(scen)
    with pytest.warns(UserWarning,
                      match=r"Unrecognized sections {'notarealsection'}"):
        rep.read_config(test_data_path / 'report-config-0.yaml')

    # Data from configured file is available
    assert rep.get('d_check').loc['seattle', 'chicago'] == 1.7


def test_reporter_apply():
    # Reporter with two scalar values
    r = Reporter()
    r.add('foo', 42)
    r.add('bar', 11)

    # A computation
    def product(a, b):
        return a * b

    # A generator method that yields keys and computations
    def baz_qux(key):
        yield key + ':baz', (product, key, 0.5)
        yield key + ':qux', (product, key, 1.1)

    # Apply the generator to two targets
    r.apply(baz_qux, 'foo')
    r.apply(baz_qux, 'bar')

    # Four computations were added to the reporter
    assert len(r.keys()) == 6
    assert r.get('foo:baz') == 42 * 0.5
    assert r.get('foo:qux') == 42 * 1.1
    assert r.get('bar:baz') == 11 * 0.5
    assert r.get('bar:qux') == 11 * 1.1

    # A generator that takes two arguments
    def twoarg(key1, key2):
        yield key1 + '__' + key2, (product, key1, key2)

    r.apply(twoarg, 'foo:baz', 'bar:qux')

    # One computation added to the reporter
    assert len(r.keys()) == 7
    assert r.get('foo:baz__bar:qux') == 42 * 0.5 * 11 * 1.1

    # A useless generator that does nothing
    def useless(key):
        return
    r.apply(useless, 'foo:baz__bar:qux')
    assert len(r.keys()) == 7


def test_reporter_disaggregate():
    r = Reporter()
    foo = Key('foo', ['a', 'b', 'c'])
    r.add(foo, '<foo data>')
    r.add('d_shares', '<share data>')

    # Disaggregation works
    r.disaggregate(foo, 'd', args=['d_shares'])

    assert 'foo:a-b-c-d' in r.graph
    assert r.graph['foo:a-b-c-d'] == (ixmp.reporting.disaggregate_shares,
                                      'foo:a-b-c', 'd_shares')

    # Invalid method
    with pytest.raises(ValueError):
        r.disaggregate(foo, 'd', method='baz')


def test_reporter_file(tmp_path):
    r = Reporter()

    # Path to a temporary file
    p = tmp_path / 'foo.txt'

    # File can be added to the Reporter before it is created, because the file
    # is not read until/unless required
    k1 = r.add_file(p)

    # File has the expected key
    assert k1 == 'file:foo.txt'

    # Add some contents to the file
    p.write_text('Hello, world!')

    # The file's contents can be read through the Reporter
    assert r.get('file:foo.txt') == 'Hello, world!'

    # Write the report to file
    p2 = tmp_path / 'bar.txt'
    r.write('file:foo.txt', p2)

    # The Reporter produces the expected output file
    assert p2.read_text() == 'Hello, world!'


def test_reporting_file_formats(test_data_path, tmp_path):
    r = Reporter()

    expected = xr.DataArray.from_series(
        pd.read_csv(test_data_path / 'report-input.csv',
                    index_col=['i', 'j'])['value'])

    # CSV file is automatically parsed to xr.DataArray
    p1 = test_data_path / 'report-input.csv'
    k = r.add_file(p1)
    assert_xr_equal(r.get(k), expected)

    # Write to CSV
    p2 = tmp_path / 'report-output.csv'
    r.write(k, p2)

    # Output is identical to input file, except for order
    assert (sorted(p1.read_text().split('\n')) ==
            sorted(p2.read_text().split('\n')))

    # Write to Excel
    p3 = tmp_path / 'report-output.xlsx'
    r.write(k, p3)
    # TODO check the contents of the Excel file


def test_reporting_units():
    r = Reporter()

    # Create some dummy data
    dims = dict(coords=['a b c'.split()], dims=['x'])
    r.add('energy:x', xr.DataArray([1., 3, 8], **dims, attrs={'_unit': 'MJ'}))
    r.add('time', xr.DataArray([5., 6, 8], **dims, attrs={'_unit': 'hour'}))
    r.add('efficiency', xr.DataArray([0.9, 0.8, 0.95], **dims))

    # Aggregation preserves units
    r.add('energy', (aggregate, 'energy:x', None, ['x']))
    assert r.get('energy').attrs['_unit'] == ureg.parse_units('MJ')

    # Units are derived for a ratio of two quantities
    r.add('power', (ratio, 'energy:x', 'time'))
    assert r.get('power').attrs['_unit'] == ureg.parse_units('MJ/hour')

    # Product of dimensioned and dimensionless quantities keeps the former
    r.add('energy2', (product, 'energy:x', 'efficiency'))
    assert r.get('energy2').attrs['_unit'] == ureg.parse_units('MJ')


def test_reporter_describe(test_mp, test_data_path):
    scen = dantzig_transport(test_mp)
    r = Reporter.from_scenario(scen)

    # hexadecimal ID of *scen*
    id_ = hex(id(scen))

    # Describe one key
    expected = """'d:i':
- aggregate(dimensions=['j'], ...)
- 'd:i-j':
  - data_for_quantity('d', 'par', 'value', ...)
  - 'scenario':
    - <ixmp.core.Scenario object at {id}>
""".format(id=id_)
    assert r.describe('d:i') == expected

    # Describe all keys
    expected = (test_data_path / 'report-describe.txt').read_text() \
                                                       .format(id=id_)
    assert r.describe() == expected


def test_reporter_visualize(test_mp):
    scen = dantzig_transport(test_mp)
    r = Reporter.from_scenario(scen)

    r.visualize('visualize.png')

    # TODO compare to a specimen; place in a temporary directory


def test_reporting_cli(test_mp_props, test_data_path):
    # Put something in the database
    mp = ixmp.Platform(dbprops=test_mp_props)
    dantzig_transport(mp)
    mp.close_db()
    del mp

    cmd = ['ixmp',
           '--dbprops', str(test_mp_props),
           '--model', 'canning problem',
           '--scenario', 'standard',
           'report',
           '--config', str(test_data_path / 'report-config-0.yaml'),
           '--default', 'd_check',
           ]
    out = subprocess.check_output(cmd, encoding='utf-8')

    # Reporting produces the expected command-line output
    assert out.endswith("""
<xarray.DataArray 'value' (i: 2, j: 3)>
array([[1.8, 2.5, 1.4],
       [1.7, 2.5, 1.8]])
Coordinates:
  * i        (i) object 'san-diego' 'seattle'
  * j        (j) object 'chicago' 'new-york' 'topeka'
""")


@pytest.mark.skip('Slow.')
def test_reporting_size():
    # Attempt to trigger MemoryError
    from itertools import zip_longest
    from functools import reduce
    from operator import add

    import numpy as np

    # 12,500,000 elements
    dims = 'abcdef'
    sizes = [1, 4, 10, 500, 25, 25]
    coords = []
    for d, N in zip(dims, sizes):
        coords.append([f'{d}_{i:03d}' for i in range(N)])

    def _make_values():
        values = list(zip_longest(*coords, np.random.rand(500)))
        return pd.DataFrame(values, columns=list(dims) + ['value']) \
                 .ffill() \
                 .set_index(list(dims))

    N = 6
    rep = Reporter()
    for i in range(N):
        base_data = xr.Dataset.from_dataframe(_make_values())
        base_key = Key(f'ds{i}', base_data.coords.keys())
        rep.add(base_key, base_data)
        rep.graph.update(base_key.aggregates())

    def add_op(*args):
        return reduce(add, args)

    rep.add('test', (add_op, *[f'ds{i}:b-c-d-e-f' for i in range(N)]))
    print(rep.get('test'))


def test_reporting_aggregate2(test_mp):
    scen = ixmp.Scenario(test_mp, 'Group reporting', 'group reporting', 'new')

    # New sets
    t_foo = ['foo{}'.format(i) for i in (1, 2, 3)]
    t_bar = ['bar{}'.format(i) for i in (4, 5, 6)]
    t = t_foo + t_bar
    y = list(map(str, range(2000, 2051, 10)))

    # Add to scenario
    scen.init_set('t')
    scen.add_set('t', t)
    scen.init_set('y')
    scen.add_set('y', y)

    # Data
    x = xr.DataArray(np.random.rand(len(t), len(y)),
                     coords=[t, y], dims=['t', 'y'])

    # As a pd.DataFrame with units
    x_df = x.to_series().rename('value').reset_index()
    x_df['unit'] = 'kg'

    scen.init_par('x', ['t', 'y'])
    scen.add_par('x', x_df)

    # Reporter
    rep = Reporter.from_scenario(scen)

    # Define some groups
    t_groups = {'foo': t_foo, 'bar': t_bar, 'baz': ['foo1', 'bar5', 'bar6']}

    # Add aggregates
    key1 = rep.aggregate2('x:t-y', 'agg1', groups={'t': t_groups}, keep=True)

    # Group has expected key
    assert key1 == 'x:t-y:agg1'

    # Aggregate is computed without error
    agg1 = rep.get(key1)

    # Expected set of keys along the aggregated dimension
    assert set(agg1.coords['t'].values) == set(t) | set(t_groups.keys())

    # Sums are as expected
    assert_xr_allclose(agg1.sel(t='foo', drop=True), x.sel(t=t_foo).sum('t'))
    assert_xr_allclose(agg1.sel(t='bar', drop=True), x.sel(t=t_bar).sum('t'))
    assert_xr_allclose(agg1.sel(t='baz', drop=True),
                       x.sel(t=['foo1', 'bar5', 'bar6']).sum('t'))

    # Add aggregates, without keeping originals
    key2 = rep.aggregate2('x:t-y', 'agg2', groups={'t': t_groups}, keep=False)

    # Distinct keys
    assert key2 != key1

    # Only the aggregated and no original keys along the aggregated dimension
    agg2 = rep.get(key2)
    assert set(agg2.coords['t'].values) == set(t_groups.keys())

    with pytest.raises(NotImplementedError):
        # Not yet supported; requires two separate operations
        key3 = rep.aggregate2('x:t-y', 'agg3',
                              groups={'t': t_groups, 'y': [2000, 2010]})
