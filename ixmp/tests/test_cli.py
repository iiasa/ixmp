from pathlib import Path
from pandas.testing import assert_frame_equal
from click.exceptions import BadParameter, UsageError

import ixmp
from ixmp.cli import VersionType
from ixmp.testing import models, populate_test_platform
import pandas as pd
import pytest


def test_versiontype():
    vt = VersionType()
    # str converts to int
    assert vt.convert('1', None, None) == 1
    assert vt.convert('-1', None, None) == -1

    # str 'new' is passes through
    assert vt.convert('new', None, None) == 'new'

    # int passes through
    assert vt.convert(1, None, None) == 1

    with pytest.raises(BadParameter, match="'xx' must be an integer or 'new'"):
        vt.convert('xx', None, None)


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
    result = ixmp_cli.invoke(['--platform', test_mp.name] + cmd)
    assert result.exit_code == 0, (result.exception, result.output)
    assert result.output == """
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


def test_import_ts(ixmp_cli, test_mp, test_data_path):
    # Ensure the 'canning problem'/'standard' TimeSeries exists
    populate_test_platform(test_mp)

    # Invoke the CLI to import data to version 1 of the TimeSeries
    result = ixmp_cli.invoke([
        '--platform', test_mp.name,
        '--model', models['dantzig']['model'],
        '--scenario', models['dantzig']['scenario'],
        '--version', '1',
        'import', 'timeseries',
        '--firstyear', '2020',
        '--lastyear', '2200',
        str(test_data_path / 'timeseries_canning.csv'),
    ])
    assert result.exit_code == 0, result.output

    # Expected data
    exp = pd.DataFrame.from_dict({
        'region': ['World'],
        'variable': ['Testing'],
        'unit': ['???'],
        'year': [2020],
        'value': [28.3],
        'model': ['canning problem'],
        'scenario': ['standard'],
    })

    # The specified TimeSeries version contains the expected data
    scen = ixmp.Scenario(test_mp, **models['dantzig'], version=1)
    assert_frame_equal(scen.timeseries(variable=['Testing']), exp)

    # The data is not present in other versions
    scen = ixmp.Scenario(test_mp, **models['dantzig'], version=2)
    assert len(scen.timeseries(variable=['Testing'])) == 0


def test_excel_io(ixmp_cli, test_mp, tmp_path):
    populate_test_platform(test_mp)
    tmp_path /= 'dantzig.xlsx'

    # Invoke the CLI to export data to Excel
    cmd = [
        '--platform', test_mp.name,
        '--model', models['dantzig']['model'],
        '--scenario', models['dantzig']['scenario'],
        'export', str(tmp_path),
    ]
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 0, result.output

    # Fails without platform/scenario info
    assert ixmp_cli.invoke(cmd[6:]).exit_code == UsageError.exit_code

    # Invoke the CLI to read data from Excel
    cmd = [
        '--platform', test_mp.name,
        '--model', models['dantzig']['model'],
        '--scenario', models['dantzig']['scenario'],
        'import', 'scenario', str(tmp_path),
    ]

    # Fails without platform/scenario info
    assert ixmp_cli.invoke(cmd[6:]).exit_code == UsageError.exit_code

    # Fails without --discard-solution
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 1
    assert 'This Scenario has a solution' in result.output

    # Succeeds with --discard-solution
    cmd.insert(-1, '--discard-solution')
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 0, result.output

    # Import into a new model name fails without --init-items
    cmd = [
        '--platform', test_mp.name,
        '--model', 'foo model',
        '--scenario', 'bar scenario',
        '--version', 'new',
        'import', 'scenario', str(tmp_path),
    ]
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 1

    # Succeeds
    cmd.insert(-1, '--init-items')
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 0, result.output


def test_report(ixmp_cli):
    # 'report' without specifying a platform/scenario is a UsageError
    result = ixmp_cli.invoke(['report', 'key'])
    assert result.exit_code == UsageError.exit_code


def test_solve(ixmp_cli, test_mp):
    populate_test_platform(test_mp)
    cmd = [
        '--platform', test_mp.name,
        '--model', models['dantzig']['model'],
        '--scenario', models['dantzig']['scenario'],
        'solve', '--remove-solution'
    ]
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 0, result.output

    # test failing solving without solution removal
    cmd = [
        '--platform', test_mp.name,
        '--model', models['dantzig']['model'],
        '--scenario', models['dantzig']['scenario'],
        'solve'
    ]
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 1, result.output

    # missing scenario
    cmd = [
        '--platform', test_mp.name,
        '--model', 'non-existing',
        '--scenario', models['dantzig']['scenario'],
        'solve'
    ]
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 1, result.output
    assert "Error: model='non-existing'" in result.output

    # no platform/scenario provided
    cmd = [
        'solve'
    ]
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code != 0, result.output
    assert "Error: give --url before command solve" in result.output
