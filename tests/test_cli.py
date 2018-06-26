import os
import subprocess
import pandas as pd
import ixmp as ix
from numpy import testing as npt

from testing_utils import here


def test_import_timeseries(test_mp_props):
    fname = os.path.join(here, 'timeseries_canning.csv')

    cmd = 'import-timeseries --dbprops="{}" --data="{}" --model="{}" --scenario="{}" --version="{}" --firstyear="{}"'
    cmd = cmd.format(test_mp_props, fname,
                     'canning problem', 'standard', 1, 2020)

    win = os.name == 'nt'
    subprocess.check_call(cmd, shell=not win)

    mp = ix.Platform(test_mp_props)
    scen = mp.Scenario('canning problem', 'standard', 1)
    obs = scen.timeseries()
    df = {'region': ['World'], 'variable': ['Testing'], 'unit': ['???'],
          'year': [2020], 'value': [28.3]}
    exp = pd.DataFrame.from_dict(df)
    cols_str = ['region', 'variable', 'unit', 'year']
    npt.assert_array_equal(exp[cols_str], obs[cols_str])
    npt.assert_array_almost_equal(exp['value'], obs['value'])
