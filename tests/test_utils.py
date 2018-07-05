import os

import pandas as pd
import pandas.util.testing as pdt

from ixmp import utils


def do_test(fname, exp):
    utils.pd_write(exp, fname)
    obs = utils.pd_read(fname)
    pdt.assert_frame_equal(obs, exp)
    os.remove(fname)


def test_pd_io_csv():
    fname = 'test.csv'
    exp = pd.DataFrame({'a': [0, 1], 'b': [2, 3]})
    do_test(fname, exp)


def test_pd_io_xlsx():
    fname = 'test.xlsx'
    exp = pd.DataFrame({'a': [0, 1], 'b': [2, 3]})
    do_test(fname, exp)


def test_pd_io_xlsx_multi():
    fname = 'test.xlsx'
    exp = {
        'sheet1': pd.DataFrame({'a': [0, 1], 'b': [2, 3]}),
        'sheet2': pd.DataFrame({'c': [4, 5], 'd': [6, 7]}),
        }
    do_test(fname, exp)
