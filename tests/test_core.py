import re

import numpy.testing as npt
import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp
from ixmp.testing import make_dantzig

test_args = ('Douglas Adams', 'Hitchhiker')
can_args = ('canning problem', 'standard')
launch_log_msg = "launching ixmp.Platform using config file at '{}'"
csv_header = ('MODEL,SCENARIO,VERSION,VARIABLE,'
              'UNIT,REGION,META,TIME,YEAR,VALUE\n')


def test_platform_init():
    with pytest.raises(ValueError, match="backend class 'foo' not among "
                       r"\['jdbc'\]"):
        ixmp.Platform(backend='foo')


def test_scen_list(test_mp):
    scenario = test_mp.scenario_list(model='Douglas Adams')['scenario']
    assert scenario[0] == 'Hitchhiker'


def test_ts_data_export(test_mp, tmp_path):
    path = tmp_path / 'export.csv'
    test_mp.export_timeseries_data(path, model='Douglas Adams')

    with open(path) as f:
        first_line = f.readline()
        assert first_line == csv_header
        assert len(f.readlines()) == 2


def test_new_scen(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args, version='new')
    assert scen.version == 0

    # A scenario with scheme='MESSAGE' can only be created with a subclass
    class Scenario(ixmp.Scenario):
        pass

    scen2 = Scenario(test_mp, model='foo', scenario='bar',
                     scheme='MESSAGE', version='new')

    # JDBCBackend complains unless these items are added
    scen2.add_set('technology', 't')
    scen2.add_set('year', '2000')
    scen2.commit('')
    del scen2

    # Loading this "MESSAGE-scheme" scenario with ixmp.Scenario raises an
    # exception
    with pytest.raises(RuntimeError):
        ixmp.Scenario(test_mp, model='foo', scenario='bar')

    # …but loading with a subclass of ixmp.Scenario is fine
    Scenario(test_mp, model='foo', scenario='bar')


def test_default_version(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    assert scen.version == 2


def test_scenario_from_url(test_mp, caplog):
    url = 'ixmp://{}/Douglas Adams/Hitchhiker'.format(test_mp.name)

    # Default version is loaded
    scen, mp = ixmp.Scenario.from_url(url)
    assert scen.version == 1

    # Giving an invalid version with errors='raise' raises an exception
    with pytest.raises(Exception, match='There was a problem getting the run '
                                        'id from the database!'):
        scen, mp = ixmp.Scenario.from_url(url + '#10000', errors='raise')

    # Giving an invalid scenario with errors='warn' raises an exception
    scen, mp = ixmp.Scenario.from_url(url + 'foo')
    assert scen is None and isinstance(mp, ixmp.Platform)
    assert re.match(
        "RuntimeError: There was a problem getting 'Hitchhikerfoo' in table "
        "'SCENARIO' from the database!\nwhen loading Scenario from url "
        "ixmp://[^/]*test_scenario_from_url/Douglas Adams/Hitchhikerfoo",
        caplog.records[-1].message)


def test_has_set(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    assert scen.has_set('i')
    assert not scen.has_set('k')


def test_range(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args, version='new')

    scen.init_set('ii')
    ii = range(1, 20, 2)

    # range instance is automatically converted to list of str in add_set
    scen.add_set('ii', ii)

    scen.init_par('new_par', idx_sets='ii')

    # range instance is a valid key argument to add_par
    scen.add_par('new_par', ii, [1.2] * len(ii))


def test_gh_210(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args, version='new')
    i = ['i0', 'i1', 'i2']

    scen.init_set('i')
    scen.add_set('i', i)
    scen.init_par('foo', idx_sets='i')

    columns = ['i', 'value']
    foo_data = pd.DataFrame(zip(i, [10, 20, 30]), columns=columns)

    # foo_data is not modified by add_par()
    scen.add_par('foo', foo_data)
    assert all(foo_data.columns == columns)


def test_get_scalar(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    obs = scen.scalar('f')
    exp = {'unit': 'USD/km', 'value': 90}
    assert obs == exp


def test_has_par(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    assert scen.has_par('f')
    assert not scen.has_par('m')


def test_add_par(test_mp):
    # add_par() broadcasts scalar values/units across multiple keys
    scen = ixmp.Scenario(test_mp, *can_args)
    scen.remove_solution()
    scen.check_out()
    scen.add_par('b', ['new-york', 'chicago'], value=100, unit='cases')


def test_init_scalar(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    scen2 = scen.clone(keep_solution=False)
    scen2.check_out()
    scen2.init_scalar('g', 90.0, 'USD/km')
    scen2.commit("adding a scalar 'g'")


def test_init_set(test_mp):
    """Test ixmp.Scenario.init_set()."""
    scen = ixmp.Scenario(test_mp, *can_args)

    # Add set on a locked scenario
    with pytest.raises(RuntimeError,
                       match="This Scenario cannot be edited, do a checkout "
                             "first!"):
        scen.init_set('foo')

    scen = scen.clone(keep_solution=False)
    scen.check_out()
    scen.init_set('foo')

    # Initialize an already-existing set
    with pytest.raises(ValueError, match="'foo' already exists"):
        scen.init_set('foo')


def test_set(test_mp):
    """Test ixmp.Scenario.add_set(), .set(), and .remove_set()."""
    scen = ixmp.Scenario(test_mp, *can_args)

    # Add element to a non-existent set
    with pytest.raises(KeyError,
                       match="No Item 'foo' exists in this Scenario!"):
        scen.add_set('foo', 'bar')

    scen.remove_solution()
    scen.check_out()

    # Add elements to a 0-D set
    scen.add_set('i', 'i1')  # Name only
    scen.add_set('i', 'i2', 'i2 comment')  # Name and comment
    scen.add_set('i', ['i3'])  # List of names, length 1
    scen.add_set('i', ['i4', 'i5'])  # List of names, length >1
    scen.add_set('i', range(0, 3))  # Generator (range object)
    # Lists of names and comments, length 1
    scen.add_set('i', ['i6'], ['i6 comment'])
    # Lists of names and comments, length >1
    scen.add_set('i', ['i7', 'i8'], ['i7 comment', 'i8 comment'])

    # Incorrect usage

    # Lists of different length
    with pytest.raises(ValueError,
                       match="Comment 'extra' without matching key"):
        scen.add_set('i', ['i9'], ['i9 comment', 'extra'])
    with pytest.raises(ValueError,
                       match="Key 'extra' without matching comment"):
        scen.add_set('i', ['i9', 'extra'], ['i9 comment'])

    # Add elements to a 1D set
    scen.init_set('foo', 'i', 'dim_i')
    scen.add_set('foo', ['i1'])  # Single key
    scen.add_set('foo', ['i2'], 'i2 in foo')  # Single key and comment
    scen.add_set('foo', 'i3')  # Bare name automatically wrapped
    # Lists of names and comments, length 1
    scen.add_set('foo', ['i6'], ['i6 comment'])
    # Lists of names and comments, length >1
    scen.add_set('foo', [['i7'], ['i8']], ['i7 comment', 'i8 comment'])
    # Dict
    scen.add_set('foo', dict(dim_i=['i7', 'i8']))

    # Incorrect usage
    # Improperly wrapped keys
    with pytest.raises(ValueError, match=r"2-D key \['i4', 'i5'\] invalid for "
                                         r"1-D set foo\['dim_i'\]"):
        scen.add_set('foo', ['i4', 'i5'])
    with pytest.raises(ValueError):
        scen.add_set('foo', range(0, 3))
    # Lists of different length
    with pytest.raises(ValueError,
                       match="Comment 'extra' without matching key"):
        scen.add_set('i', ['i9'], ['i9 comment', 'extra'])
    with pytest.raises(ValueError,
                       match="Key 'extra' without matching comment"):
        scen.add_set('i', ['i9', 'extra'], ['i9 comment'])
    # Missing element in the index set
    with pytest.raises(ValueError, match="The index set 'i' does not have an "
                                         "element 'bar'!"):
        scen.add_set('foo', 'bar')

    # Retrieve set elements
    i = {'seattle', 'san-diego', 'i1', 'i2', 'i3', 'i4', 'i5', '0', '1', '2',
         'i6', 'i7', 'i8'}
    assert i == set(scen.set('i'))

    # Remove elements from an 0D set: bare name
    scen.remove_set('i', 'i2')
    i -= {'i2'}
    assert i == set(scen.set('i'))

    # Remove elements from an 0D set: Iterable of names, length >1
    scen.remove_set('i', ['i4', 'i5'])
    i -= {'i4', 'i5'}
    assert i == set(scen.set('i'))

    # Remove elements from a 1D set: Dict
    scen.remove_set('foo', dict(dim_i=['i7', 'i8']))
    # Added elements from above; minus directly removed elements; minus i2
    # because it was removed from the set i that indexes dim_i of foo
    foo = {'i1', 'i2', 'i3', 'i6', 'i7', 'i8'} - {'i2'} - {'i7', 'i8'}
    assert foo == set(scen.set('foo')['dim_i'])

    # Remove a set completely
    assert 'h' not in scen.set_list()

    scen.init_set('h')
    assert 'h' in scen.set_list()

    scen.remove_set('h')
    assert 'h' not in scen.set_list()


# make sure that changes to a scenario are copied over during clone
def test_add_clone(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args, version=1)
    scen.check_out()
    scen.init_set('h')
    scen.add_set('h', 'test')
    scen.commit("adding an index set 'h', with element 'test'")

    scen2 = scen.clone(keep_solution=False)
    obs = scen2.set('h')
    npt.assert_array_equal(obs, ['test'])


# make sure that (only) the correct scenario is touched after cloning
def test_clone_edit(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    scen2 = scen.clone(keep_solution=False)
    scen2.check_out()
    scen2.change_scalar('f', 95.0, 'USD/km')
    scen2.commit('change transport cost')
    obs = scen.scalar('f')
    exp = {'unit': 'USD/km', 'value': 90}
    assert obs == exp
    obs = scen2.scalar('f')
    exp = {'unit': 'USD/km', 'value': 95}
    assert obs == exp


def test_idx_name(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    df = scen.idx_names('d')
    npt.assert_array_equal(df, ['i', 'j'])


def test_has_var(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    assert scen.has_var('x')
    assert not scen.has_var('y')


def test_var_marginal(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    df = scen.var('x', filters={'i': ['seattle']})
    npt.assert_array_almost_equal(df['mrg'], [0, 0, 0.036])


def test_var_level(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    df = scen.var('x', filters={'i': ['seattle']})
    npt.assert_array_almost_equal(df['lvl'], [50, 300, 0])


def test_var_general_str(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    df = scen.var('x', filters={'i': ['seattle']})
    npt.assert_array_equal(
        df['j'], ['new-york', 'chicago', 'topeka'])


def test_unit_list(test_mp):
    units = test_mp.units()
    assert ('cases' in units) is True


def test_add_unit(test_mp):
    test_mp.add_unit('test', 'just testing')


def test_par_filters_unit(test_mp):
    scen = ixmp.Scenario(test_mp, *can_args)
    df = scen.par('d', filters={'i': ['seattle']})
    obs = df.loc[0, 'unit']
    exp = 'km'
    assert obs == exp


def test_filter_str(test_mp):
    scen = ixmp.Scenario(test_mp, 'model', 'scenario', version='new')

    elements = ['foo', 42, object()]
    expected = list(map(str, elements))

    scen.init_set('s')

    # Set elements can be added which are not str
    scen.add_set('s', elements)

    # Elements are stored and returned as str
    assert expected == scen.set('s').tolist()

    # Parameter defined over 's'
    p = pd.DataFrame.from_records(zip(elements, [1., 2., 3.]),
                                  columns=['s', 'value'])

    # Expected return dtypes of index and value columns
    dtypes = {'s': str, 'value': float}
    p_exp = p.astype(dtypes)

    scen.init_par('p', ['s'])
    scen.add_par('p', p)

    # Values can be retrieved using non-string filters
    exp = p_exp.loc[1:, :].reset_index(drop=True)
    obs = scen.par('p', filters={'s': elements[1:]})
    pdt.assert_frame_equal(exp[['s', 'value']], obs[['s', 'value']])


def test_meta(test_mp):
    test_dict = {
        "test_string": 'test12345',
        "test_number": 123.456,
        "test_number_negative": -123.456,
        'test_int': 12345,
        'test_bool': True,
        'test_bool_false': False,
    }

    scen = ixmp.Scenario(test_mp, *can_args, version=1)
    for k, v in test_dict.items():
        scen.set_meta(k, v)

    # test all
    obs_dict = scen.get_meta()
    for k, exp in test_dict.items():
        obs = obs_dict[k]
        assert obs == exp

    # test name
    obs = scen.get_meta('test_string')
    exp = test_dict['test_string']
    assert obs == exp

    # Setting with a type other than int, float, bool, str raises TypeError
    with pytest.raises(TypeError, match='Cannot store metadata of type'):
        scen.set_meta('test_string', complex(1, 1))


def test_load_scenario_data(test_mp):
    """load_scenario_data() caches all data."""
    scen = ixmp.Scenario(test_mp, *can_args)
    scen.load_scenario_data()

    cache_key = scen.platform._backend._cache_key(scen, 'par', 'd')

    # Item exists in cache
    assert cache_key in scen.platform._backend._cache

    # Cache has not been used
    hits_before = scen.platform._backend._cache_hit.get(cache_key, 0)
    assert hits_before == 0

    # Retrieving the expected value
    assert 'km' == scen.par('d', filters={'i': ['seattle']}).loc[0, 'unit']

    # Cache was used to return the value
    hits_after = scen.platform._backend._cache_hit[cache_key]
    assert hits_after == hits_before + 1


def test_load_scenario_data_clear_cache(test_mp):
    # this fails on commit: 4376f54
    scen = ixmp.Scenario(test_mp, *can_args, cache=True)
    scen.load_scenario_data()
    scen.platform._backend.cache_invalidate(scen, 'par', 'd')


def test_log_level(test_mp):
    test_mp.set_log_level('CRITICAL')
    test_mp.set_log_level('ERROR')
    test_mp.set_log_level('WARNING')
    test_mp.set_log_level('INFO')
    test_mp.set_log_level('DEBUG')
    test_mp.set_log_level('NOTSET')


def test_log_level_raises(test_mp):
    pytest.raises(ValueError, test_mp.set_log_level, level='foo')


def test_solve_callback(test_mp):
    """Test the callback argument to Scenario.solve().

    In real usage, callback() would compute some kind of convergence criterion.
    This test uses a sequence of different values for d(seattle, new-york) in
    Dantzig's transport problem. Once the correct value is set on the
    ixmp.Scenario, the solution equals an expected value, and the model has
    'converged'.
    """
    # Set up the Dantzig problem
    scen = make_dantzig(test_mp)

    # Solve the scenario as configured
    solve_args = dict(model='dantzig', gams_args=['LogOption=2'])
    scen.solve(**solve_args)

    # Store the expected value of the decision variable, x
    expected = scen.var('x')

    # The reference distance between Seattle and New York is 2.5 [10^3 miles]
    d = [3.5, 2.0, 2.7, 2.5, 1.0]

    def set_d(scenario, value):
        """Set the distance between Seattle and New York to *value*."""
        scenario.remove_solution()
        scenario.check_out()
        data = {'i': 'seattle', 'j': 'new-york', 'value': value, 'unit': 'km'}
        # TODO should not be necessary here to call pd.DataFrame
        scenario.add_par('d', pd.DataFrame(data, index=[0]))
        scenario.commit('iterative solution')

    # Changing the entry in the array 'd' results in an optimal 'x' that is
    # different from the one stored as *expected*.
    set_d(scen, d[0])

    def change_distance(scenario):
        """Callback for model solution."""
        # Check if the model has 'converged' on the correct solution
        if (scenario.var('x') == expected).all(axis=None):
            return True

        # Convergence not reached

        # Change the distance between Seattle and New York, using the
        # 'iteration' variable stored on the Scenario object
        set_d(scenario, d[scenario.iteration])

        # commented: see below
        # # Trigger another solution of the model
        # return False

    # Warning is raised because 'return False' is commented above, meaning
    # user may have forgotten any return statement in the callback
    message = (r'solve\(callback=...\) argument returned None; will loop '
               'indefinitely unless True is returned.')
    with pytest.warns(UserWarning, match=message):
        # Model iterates automatically
        scen.solve(callback=change_distance, **solve_args)

    # Solution reached after 4 iterations, i.e. for d[4 - 1] == 2.5
    assert scen.iteration == 4
