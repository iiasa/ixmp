from pathlib import Path

import ixmp
import jpype
import pandas as pd
import pandas.testing as pdt


def test_jvm_warn(recwarn):
    """Test that no warnings are issued on JVM start-up.

    A warning message is emitted e.g. for JPype 0.7 if the 'convertStrings'
    kwarg is not provided to jpype.startJVM.

    NB this function should be in test_core.py, but because pytest executes
    tests in file, then code order, it must be before the call to ix.Platform()
    below.
    """

    # Start the JVM for the first time in the test session
    from ixmp.backend.jdbc import start_jvm
    start_jvm()

    if jpype.__version__ > '0.7':
        # Zero warnings were recorded
        assert len(recwarn) == 0, recwarn.pop().message


def test_platform(ixmp_cli, tmp_path):
    """Test 'platform' command."""
    from ixmp import config

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

    # Setting the default using a non-existent platform fails
    r = call('add', 'default', 'nonexistent', exit_0=False)
    assert r.exit_code == 1

    # JDBC HSQLDB platform can be added with absolute path
    r = call('add', 'p2', 'jdbc', 'hsqldb', tmp_path)
    assert config.get_platform_info('p2')[1]['path'] == tmp_path

    # JDBC HSQLDB platform can be added with relative path
    rel = './foo'
    r = call('add', 'p3', 'jdbc', 'hsqldb', rel)
    assert Path(rel).resolve() == config.get_platform_info('p3')[1]['path']


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
        'year': [2020],
        'value': [28.3],
        'model': ['canning problem'],
        'scenario': ['standard'],
    })
    pdt.assert_frame_equal(exp, scen.timeseries())
