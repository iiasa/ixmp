from pathlib import Path
from pandas.testing import assert_frame_equal
from click.exceptions import UsageError

import ixmp
import pandas as pd


def test_main(ixmp_cli, test_mp, tmp_path):
    # Name of a temporary file that doesn't exist
    tmp_path /= 'temp.properties'

    # Giving --dbprops and a nonexistent file is an invalid argument
    cmd = [
        '--platform', 'pname',
        '--dbprops', str(tmp_path),
        'platform', 'list',  # Doesn't get executed; fails in cli.main()
    ]
    result = ixmp_cli.invoke(cmd)
    # Check against click's default exit code for the exception
    assert result.exit_code == UsageError.exit_code

    # Create the file
    tmp_path.write_text('')

    # Giving both --platform and --dbprops is bad option usage
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == UsageError.exit_code

    # --dbprops alone causes backend='jdbc' to be inferred (but an error
    # because temp.properties is empty)
    result = ixmp_cli.invoke(cmd[2:])
    assert 'Config file contains no database URL' in result.exception.args[0]

    # --url argument can be given
    cmd = ['--url', 'ixmp://{}/Douglas Adams/Hitchhiker'.format(test_mp.name),
           'platform', 'list']
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 0

    # --url and other Platform/Scenario specifiers are bad option usage
    result = ixmp_cli.invoke(['--platform', 'foo'] + cmd)
    assert result.exit_code == UsageError.exit_code


def test_config(ixmp_cli):
    # ixmp has no string keys by default, so we insert a fake one
    ixmp.config.register('test key', str)
    ixmp.config.values['test key'] = 'foo'

    # get() works
    assert ixmp_cli.invoke(['config', 'get', 'test key']).output == 'foo\n'

    # set() changes the value
    result = ixmp_cli.invoke(['config', 'set', 'test key', 'bar'])
    assert result.exit_code == 0
    assert ixmp_cli.invoke(['config', 'get', 'test key']).output == 'bar\n'

    # get() with a value is an invalid call
    result = ixmp_cli.invoke(['config', 'get', 'test key', 'BADVALUE'])
    assert result.exit_code != 0


def test_list(ixmp_cli, test_mp):
    cmd = ['list', '--match', 'foo']

    # 'list' without specifying a platform/scenario is a UsageError
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == UsageError.exit_code

    # CLI works; nothing returned with a --match option that matches nothing
    r = ixmp_cli.invoke(['--platform', test_mp.name] + cmd)
    assert r.output == """
0 model name(s)
0 scenario name(s)
0 (model, scenario) combination(s)
0 total scenarios
"""


def test_platform(ixmp_cli, tmp_path):
    """Test 'platform' command."""
    def call(*args, exit_0=True):
        result = ixmp_cli.invoke(['platform'] + list(map(str, args)))
        assert not exit_0 or result.exit_code == 0
        return result

    # The default platform is 'local'
    r = call('list')
    assert 'default local\n' in r.output

    # JBDC Oracle platform can be added
    r = call('add', 'p1', 'jdbc', 'oracle', 'HOSTNAME', 'USER', 'PASSWORD')

    # Default platform can be changed
    r = call('add', 'default', 'p1')
    r = call('list')
    assert 'default p1\n' in r.output
    # Reset to avoid disturbing other tests
    call('add', 'default', 'local')

    # Setting the default using a non-existent platform fails
    r = call('add', 'default', 'nonexistent', exit_0=False)
    assert r.exit_code == 1

    # JDBC HSQLDB platform can be added with absolute path
    r = call('add', 'p2', 'jdbc', 'hsqldb', tmp_path)
    assert ixmp.config.get_platform_info('p2')[1]['path'] == tmp_path

    # JDBC HSQLDB platform can be added with relative path
    rel = './foo'
    r = call('add', 'p3', 'jdbc', 'hsqldb', rel)
    assert Path(rel).resolve() == \
        ixmp.config.get_platform_info('p3')[1]['path']

    # Platform can be removed
    r = call('remove', 'p3')
    assert r.output == "Removed platform config for 'p3'\n"

    # Non-existent platform can't be removed
    r = call('remove', 'p3', exit_0=False)  # Already removed
    assert r.exit_code == 1

    # Extra args to 'remove' are invalid
    r = call('remove', 'p2', 'BADARG', exit_0=False)
    assert r.exit_code == 1


def test_import(ixmp_cli, test_mp, test_data_path):
    fname = test_data_path / 'timeseries_canning.csv'

    platform_name = test_mp.name
    del test_mp

    result = ixmp_cli.invoke([
        '--platform', platform_name,
        '--model', 'canning problem',
        '--scenario', 'standard',
        '--version', '1',
        'import',
        '--firstyear', '2020',
        str(fname),
    ])
    assert result.exit_code == 0

    # Check that the TimeSeries now contains the expected content
    mp = ixmp.Platform(name=platform_name)
    scen = ixmp.Scenario(mp, 'canning problem', 'standard', 1)
    exp = pd.DataFrame.from_dict({
        'region': ['World'],
        'variable': ['Testing'],
        'unit': ['???'],
        'time': ['Year'],
        'year': [2020],
        'value': [28.3],
        'model': ['canning problem'],
        'scenario': ['standard'],
    })
    assert_frame_equal(exp, scen.timeseries())


def test_report(ixmp_cli):
    # 'report' without specifying a platform/scenario is a UsageError
    result = ixmp_cli.invoke(['report', 'key'])
    assert result.exit_code == UsageError.exit_code
