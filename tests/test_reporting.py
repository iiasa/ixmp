"""Tests for ixmp.reporting."""
import os
import subprocess

import ixmp
import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
import pytest
import xarray as xr

import ixmp.reporting
from ixmp.reporting import Key, Reporter, computations
from ixmp.reporting.utils import ureg, Quantity
from ixmp.testing import make_dantzig, assert_qty_allclose, assert_qty_equal


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


def test_reporting_configure():
    # TODO test: All supported configuration keys can be handled
    # TODO test: Unsupported keys raise warnings or errors
    pass


def test_missing_keys():
    """Adding computations that refer to missing keys raises KeyError."""
    r = Reporter()
    r.add('a', 3)
    r.add('d', 4)

    def gen(other):
        """A generator for apply()."""
        return (lambda a, b: a * b, 'a', other)

    # One missing key
    with pytest.raises(KeyError, match=r"\['b'\]"):
        r.add_product('ab', 'a', 'b')

    # Two missing keys
    with pytest.raises(KeyError, match=r"\['c', 'b'\]"):
        r.add_product('abc', 'c', 'a', 'b')

    # Using apply() targeted at non-existent keys also raises an Exception
    with pytest.raises(KeyError, match=r"\['e', 'f'\]"):
        r.apply(gen, 'd', 'e', 'f')

    # add(..., strict=True) checks str or Key arguments
    with pytest.raises(KeyError, match=r"\['b', g:h-i\]"):
        r.add('foo', (computations.product, 'a', 'b', Key('g', 'hi')),
              strict=True)


def test_reporter_from_scenario(scenario):
    r = Reporter.from_scenario(scenario)

    r.finalize(scenario)

    assert 'scenario' in r.graph


def test_reporter_from_dantzig(test_mp, test_data_path):
    scen = make_dantzig(test_mp, solve=test_data_path)

    # Reporter.from_scenario can handle the Dantzig problem
    rep = Reporter.from_scenario(scen)

    # Partial sums are available automatically (d is defined over i and j)
    d_i = rep.get('d:i')

    # Units pass through summation
    assert d_i.attrs['_unit'] == ureg.parse_units('km')

    # Summation across all dimensions results a 1-element Quantity
    d = rep.get('d:')
    assert len(d) == 1
    assert np.isclose(d.iloc[0], 11.7)

    # Weighted sum
    weights = Quantity(xr.DataArray(
        [1, 2, 3],
        coords=['chicago new-york topeka'.split()],
        dims=['j']))
    new_key = rep.aggregate('d:i-j', 'weighted', 'j', weights)

    # ...produces the expected new key with the summed dimension removed and
    # tag added
    assert new_key == 'd:i:weighted'

    # ...produces the expected new value
    obs = rep.get(new_key)
    exp = (rep.get('d:i-j') * weights).sum(dim=['j']) / weights.sum(dim=['j'])
    # TODO: attrs has to be explicitly copied here because math is done which
    # returns a pd.Series
    exp = Quantity(exp, attrs=rep.get('d:i-j').attrs)

    assert_series_equal(obs.sort_index(), exp.sort_index())

    # Disaggregation with explicit data
    # (cases of canned food 'p'acked in oil or water)
    shares = xr.DataArray([0.8, 0.2], coords=[['oil', 'water']], dims=['p'])
    new_key = rep.disaggregate('b:j', 'p', args=[Quantity(shares)])

    # ...produces the expected key with new dimension added
    assert new_key == 'b:j-p'

    b_jp = rep.get('b:j-p')

    # Units pass through disaggregation
    assert b_jp.attrs['_unit'] == 'cases'

    # Set elements are available
    assert rep.get('j') == ['new-york', 'chicago', 'topeka']

    # 'all' key retrieves all quantities
    obs = {da.name for da in rep.get('all')}
    exp = set('a b d f demand demand-margin z x'.split())
    assert obs == exp

    # Shorthand for retrieving a full key name
    assert rep.full_key('d') == 'd:i-j' and isinstance(rep.full_key('d'), Key)


def test_reporter_read_config(test_mp, test_data_path):
    scen = make_dantzig(test_mp)

    rep = Reporter.from_scenario(scen)
    with pytest.warns(UserWarning,
                      match=r"Unrecognized sections {'notarealsection'}"):
        rep.read_config(test_data_path / 'report-config-0.yaml')

    # Data from configured file is available
    assert rep.get('d_check').loc['seattle', 'chicago'] == 1.7


def test_reporter_apply():
    # Reporter with two scalar values
    r = Reporter()
    r.add('foo', (lambda x: x, 42))
    r.add('bar', (lambda x: x, 11))

    N = len(r.keys())

    # A computation
    def _product(a, b):
        return a * b

    # A generator method that yields keys and computations
    def baz_qux(key):
        yield key + ':baz', (_product, key, 0.5)
        yield key + ':qux', (_product, key, 1.1)

    # Apply the generator to two targets
    r.apply(baz_qux, 'foo')
    r.apply(baz_qux, 'bar')

    # Four computations were added to the reporter
    N += 4
    assert len(r.keys()) == N
    assert r.get('foo:baz') == 42 * 0.5
    assert r.get('foo:qux') == 42 * 1.1
    assert r.get('bar:baz') == 11 * 0.5
    assert r.get('bar:qux') == 11 * 1.1

    # A generator that takes two arguments
    def twoarg(key1, key2):
        yield key1 + '__' + key2, (_product, key1, key2)

    r.apply(twoarg, 'foo:baz', 'bar:qux')

    # One computation added to the reporter
    N += 1
    assert len(r.keys()) == N
    assert r.get('foo:baz__bar:qux') == 42 * 0.5 * 11 * 1.1

    # A useless generator that does nothing
    def useless(key):
        return
    r.apply(useless, 'foo:baz__bar:qux')

    # Nothing added to the reporter
    assert len(r.keys()) == N


def test_reporter_disaggregate():
    r = Reporter()
    foo = Key('foo', ['a', 'b', 'c'])
    r.add(foo, '<foo data>')
    r.add('d_shares', '<share data>')

    # Disaggregation works
    r.disaggregate(foo, 'd', args=['d_shares'])

    assert 'foo:a-b-c-d' in r.graph
    assert r.graph['foo:a-b-c-d'] == (computations.disaggregate_shares,
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
    assert_qty_equal(r.get(k), expected)

    # Write to CSV
    p2 = tmp_path / 'report-output.csv'
    r.write(k, p2)

    # Output is identical to input file, except for order
    assert (sorted(p1.read_text().split('\n'))
            == sorted(p2.read_text().split('\n')))

    # Write to Excel
    p3 = tmp_path / 'report-output.xlsx'
    r.write(k, p3)
    # TODO check the contents of the Excel file


def test_reporting_units():
    """Test handling of units within Reporter computations."""
    r = Reporter()

    # Create some dummy data
    dims = dict(coords=['a b c'.split()], dims=['x'])
    r.add('energy:x',
          Quantity(xr.DataArray([1., 3, 8], attrs={'_unit': 'MJ'}, **dims)))
    r.add('time',
          Quantity(xr.DataArray([5., 6, 8], attrs={'_unit': 'hour'}, **dims)))
    r.add('efficiency',
          Quantity(xr.DataArray([0.9, 0.8, 0.95], **dims)))

    # Aggregation preserves units
    r.add('energy', (computations.sum, 'energy:x', None, ['x']))
    assert r.get('energy').attrs['_unit'] == ureg.parse_units('MJ')

    # Units are derived for a ratio of two quantities
    r.add('power', (computations.ratio, 'energy:x', 'time'))
    assert r.get('power').attrs['_unit'] == ureg.parse_units('MJ/hour')

    # Product of dimensioned and dimensionless quantities keeps the former
    r.add('energy2', (computations.product, 'energy:x', 'efficiency'))
    assert r.get('energy2').attrs['_unit'] == ureg.parse_units('MJ')


def test_reporting_platform_units(test_mp, caplog):
    """Test handling of units from ixmp.Platform.

    test_mp is loaded with some units includig '-', '???', 'G$', etc. which are
    not parseable with pint; and others which are not defined in a default
    pint.UnitRegistry. These tests check the handling of those units.
    """

    # Prepare a Scenario with test data
    scen = ixmp.Scenario(test_mp, 'reporting_platform_units',
                         'reporting_platform_units', 'new')
    t, t_foo, t_bar, x = add_test_data(scen)
    rep = Reporter.from_scenario(scen)
    x_key = rep.full_key('x')

    # Convert 'x' to dataframe
    x = x.to_series().rename('value').reset_index()

    # Exception message, formatted as a regular expression
    msg = r"unit '{}' cannot be parsed; contains invalid character\(s\) '{}'"

    # Unit and components for the regex
    bad_units = [
        ('-', '-', '-'),
        ('???', r'\?\?\?', r'\?'),
        ('G$', r'G\$', r'\$')
    ]
    for unit, expr, chars in bad_units:
        # Overwrite the parameter
        x['unit'] = unit
        scen.add_par('x', x)

        # Parsing units with invalid chars raises an intelligible exception
        with pytest.raises(ValueError, match=msg.format(expr, chars)):
            rep.get(x_key)

    # Now using parseable but unrecognized units
    x['unit'] = 'USD/kWa'
    scen.add_par('x', x)

    # Unrecognized units are added automatically, with log messages emitted
    caplog.clear()
    rep.get(x_key)
    expected = [
        'Add unit definition: USD = [USD]',
        'Add unit definition: kWa = [kWa]',
    ]
    assert all(e in [rec.message for rec in caplog.records] for e in expected)

    # Mix of recognized/unrecognized units can be added: USD is already in the
    # unit registry, so is not re-added
    x['unit'] = 'USD/pkm'
    test_mp.add_unit('USD/pkm')
    scen.add_par('x', x)

    caplog.clear()
    rep.get(x_key)
    assert not any('Add unit definition: USD = [USD]' in
                   rec.message for rec in caplog.records)
    assert any('Add unit definition: pkm = [pkm]' in
               rec.message for rec in caplog.records)


def test_reporter_describe(test_mp, test_data_path):
    scen = make_dantzig(test_mp)
    r = Reporter.from_scenario(scen)

    # hexadecimal ID of *scen*
    id_ = hex(id(scen)) if os.name != 'nt' else \
        '{:#018X}'.format(id(scen)).replace('X', 'x')

    # Describe one key
    expected = """'d:i':
- sum(dimensions=['j'], weights=None, ...)
- 'd:i-j':
  - data_for_quantity('par', 'd', 'value', ...)
  - 'scenario':
    - <ixmp.core.Scenario object at {id}>
  - 'filters':
    - {{}}
""".format(id=id_)
    assert r.describe('d:i') == expected

    # Describe all keys
    expected = (test_data_path / 'report-describe.txt').read_text() \
                                                       .format(id=id_)
    assert r.describe() == expected


def test_reporter_visualize(test_mp, tmp_path):
    scen = make_dantzig(test_mp)
    r = Reporter.from_scenario(scen)

    r.visualize(str(tmp_path / 'visualize.png'))

    # TODO compare to a specimen


def test_reporting_cli(test_mp_props, test_data_path):
    # Put something in the database
    mp = ixmp.Platform(dbprops=test_mp_props)
    make_dantzig(mp)
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


def test_report_size(test_mp):
    """Stress-test reporting of large, sparse quantities."""
    from itertools import zip_longest

    import numpy as np

    # test_mp.add_unit('kg')
    scen = ixmp.Scenario(test_mp, 'size test', 'base', version='new')

    # Dimensions and their lengths
    dims = 'abcdef'
    sizes = [1, 5, 21, 21, 89, 377]  # Fibonacci #s; next 1597, 6765

    # commented: "377 / 73984365 elements = 0.00051% full"
    # from functools import reduce
    # from operator import mul
    # size = reduce(mul, sizes)
    # print('{} / {} elements = {:.5f}% full'
    #       .format(max(sizes), size, 100 * max(sizes) / size))

    # Names like f_0000 ... f_1596 along each dimension
    coords = []
    for d, N in zip(dims, sizes):
        # py3.5 compat: could use an f-string here
        coords.append(['{}_{:04d}'.format(d, i) for i in range(N)])
        # Add to Scenario
        scen.init_set(d)
        scen.add_set(d, coords[-1])

    def _make_values():
        """Make a DataFrame containing each label in *coords* at least once."""
        values = list(zip_longest(*coords, np.random.rand(max(sizes))))
        result = pd.DataFrame(values, columns=list(dims) + ['value']) \
                   .ffill()
        result['unit'] = 'kg'
        return result

    # Fill the Scenario with quantities named q_01 ... q_09
    N = 10
    names = []
    for i in range(10):
        # py3.5 compat: could use an f-string here
        name = 'q_{:02d}'.format(i)
        scen.init_par(name, list(dims))
        scen.add_par(name, _make_values())
        names.append(name)

    # Create the reporter
    rep = Reporter.from_scenario(scen)

    # Add an operation that takes the product, i.e. requires all the q_*
    keys = [rep.full_key(name) for name in names]
    rep.add('bigmem', tuple([computations.product] + keys))

    # One quantity fits in memory
    rep.get(keys[0])
    # assert False

    # All quantities together trigger MemoryError
    rep.get('bigmem')


def add_test_data(scen):
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

    return t, t_foo, t_bar, x


def test_reporting_aggregate(test_mp):
    scen = ixmp.Scenario(test_mp, 'Group reporting', 'group reporting', 'new')
    t, t_foo, t_bar, x = add_test_data(scen)

    # Reporter
    rep = Reporter.from_scenario(scen)

    # Define some groups
    t_groups = {'foo': t_foo, 'bar': t_bar, 'baz': ['foo1', 'bar5', 'bar6']}

    # Add aggregates
    key1 = rep.aggregate('x:t-y', 'agg1', {'t': t_groups}, keep=True)

    # Group has expected key and contents
    assert key1 == 'x:t-y:agg1'

    # Aggregate is computed without error
    agg1 = rep.get(key1)

    # Expected set of keys along the aggregated dimension
    assert set(agg1.coords['t'].values) == set(t) | set(t_groups.keys())

    # Sums are as expected
    # TODO: the check_dtype arg assumes Quantity backend is a AttrSeries,
    # should that be made default in assert_qty_allclose?
    assert_qty_allclose(agg1.sel(t='foo', drop=True), x.sel(t=t_foo).sum('t'),
                        check_dtype=False)
    assert_qty_allclose(agg1.sel(t='bar', drop=True), x.sel(t=t_bar).sum('t'),
                        check_dtype=False)
    assert_qty_allclose(agg1.sel(t='baz', drop=True),
                        x.sel(t=['foo1', 'bar5', 'bar6']).sum('t'),
                        check_dtype=False)

    # Add aggregates, without keeping originals
    key2 = rep.aggregate('x:t-y', 'agg2', {'t': t_groups}, keep=False)

    # Distinct keys
    assert key2 != key1

    # Only the aggregated and no original keys along the aggregated dimension
    agg2 = rep.get(key2)
    assert set(agg2.coords['t'].values) == set(t_groups.keys())

    with pytest.raises(NotImplementedError):
        # Not yet supported; requires two separate operations
        rep.aggregate('x:t-y', 'agg3', {'t': t_groups, 'y': [2000, 2010]})


def test_reporting_filters(test_mp, tmp_path):
    """Reporting can be filtered ex ante."""
    scen = ixmp.Scenario(test_mp, 'Reporting filters', 'Reporting filters',
                         'new')
    t, t_foo, t_bar, x = add_test_data(scen)

    rep = Reporter.from_scenario(scen)
    x_key = rep.full_key('x')

    def assert_t_indices(labels):
        assert set(rep.get(x_key).index.levels[0]) == set(labels)

    # 1. Set filters directly
    rep.graph['filters'] = {'t': t_foo}
    assert_t_indices(t_foo)

    # Reporter can be re-used by changing filters
    rep.graph['filters'] = {'t': t_bar}
    assert_t_indices(t_bar)

    rep.graph['filters'] = {}
    assert_t_indices(t)

    # 2. Set filters using a convenience method
    rep = Reporter.from_scenario(scen)
    rep.set_filters(t=t_foo)
    assert_t_indices(t_foo)

    # Clear filters using the convenience method
    rep.set_filters(t=None)
    assert_t_indices(t)

    # 3. Set filters via configuration keys
    # NB passes through from_scenario() -> __init__() -> configure()
    rep = Reporter.from_scenario(scen, filters={'t': t_foo})
    assert_t_indices(t_foo)

    # Configuration key can also be read from file
    rep = Reporter.from_scenario(scen)

    # Write a temporary file containing the desired labels
    config_file = tmp_path / 'config.yaml'
    config_file.write_text('\n'.join([
        'filters:',
        '  t: {!r}'.format(t_bar),
    ]))

    rep.configure(config_file)
    assert_t_indices(t_bar)
