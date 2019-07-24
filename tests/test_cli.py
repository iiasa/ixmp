# See also:
# - test_core.test_default_dbprops_file, testing 'ixmp-config'.
# - test_core.test_db_config_path, testing 'ixmp-config'.
import os
import subprocess

import ixmp as ix
import jpype
import numpy.testing as npt
import pandas as pd


def test_jvm_warn(recwarn):
    """Test for a JVM start-up warning.

    This will be emitted, e.g. for JPype 0.7 if the 'convertStrings' kwarg is
    not provided to jpype.startJVM.

    NB this should be in test_core.py, but because pytest executes tests in
    file, then code order, it must be before the call to ix.Platform() below.
    """
    ix.start_jvm()
    if jpype.__version__ > '0.7':
        assert len(recwarn) == 0, recwarn.pop().message


def test_import_timeseries(test_mp_props, test_data_path):
    fname = test_data_path / 'timeseries_canning.csv'

    cmd = ('import-timeseries --dbprops="{}" --data="{}" --model="{}" '
           '--scenario="{}" --version="{}" --firstyear="{}"').format(
        test_mp_props, fname, 'canning problem', 'standard', 1, 2020)

    win = os.name == 'nt'
    subprocess.check_call(cmd, shell=not win)

    mp = ix.Platform(test_mp_props)
    scen = ix.Scenario(mp, 'canning problem', 'standard', 1)
    obs = scen.timeseries()
    df = {'region': ['World'], 'variable': ['Testing'], 'unit': ['???'],
          'year': [2020], 'value': [28.3]}
    exp = pd.DataFrame.from_dict(df)
    cols_str = ['region', 'variable', 'unit', 'year']
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])
