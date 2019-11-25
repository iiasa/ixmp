"""Tests for ixmp.utils."""
import os

import pandas as pd
import pandas.util.testing as pdt

from ixmp import utils


def make_obs(fname, exp, **kwargs):
    utils.pd_write(exp, fname, index=False)
    obs = utils.pd_read(fname, **kwargs)
    os.remove(fname)
    return obs


def test_pd_io_csv(tmp_path):
    fname = 'test.csv'
    exp = pd.DataFrame({'a': [0, 1], 'b': [2, 3]})
    d = tmp_path / "sub"
    d.mkdir()
    obs = make_obs(fname, exp)
    pdt.assert_frame_equal(obs, exp)


def test_pd_io_xlsx():
    fname = 'test.xlsx'
    exp = pd.DataFrame({'a': [0, 1], 'b': [2, 3]})
    obs = make_obs(fname, exp)
    pdt.assert_frame_equal(obs, exp)


def test_pd_io_xlsx_multi():
    fname = 'test.xlsx'
    exp = {
        'sheet1': pd.DataFrame({'a': [0, 1], 'b': [2, 3]}),
        'sheet2': pd.DataFrame({'c': [4, 5], 'd': [6, 7]}),
    }
    obs = make_obs(fname, exp, sheet_name=None)
    for k, _exp in exp.items():
        _obs = obs[k]
        pdt.assert_frame_equal(_obs, _exp)
