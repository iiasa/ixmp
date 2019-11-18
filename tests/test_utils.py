"""Tests for ixmp.utils."""
import os

import pandas as pd
import pandas.util.testing as pdt
import pytest
from pytest import mark, param

from ixmp import utils


def make_obs(fname, exp, **kwargs):
    utils.pd_write(exp, fname, index=False)
    obs = utils.pd_read(fname, **kwargs)
    os.remove(fname)
    return obs


def test_pd_io_csv():
    fname = 'test.csv'
    exp = pd.DataFrame({'a': [0, 1], 'b': [2, 3]})
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


def test_pd_write(tmp_path):

    fname = 'test.csv'
    d = tmp_path / "sub"
    d.mkdir()

    data_frame = [1, 2, 3, 4]

    with pytest.raises(ValueError):
        assert utils.pd_write(data_frame, fname)


def test_check_year():

    # If y is a string value, raise a Value Error.

    y1 = "a"
    s1 = "a"
    with pytest.raises(ValueError):
        assert utils.check_year(y1, s1)

    # If y = None.

    y2 = None
    s2 = None

    assert utils.check_year(y2, s2) is None

    # If y is integer.

    y3 = 4
    s3 = 4

    assert utils.check_year(y3, s3) is True


m_s = dict(model='m', scenario='s')

URLS = [
    ('ixmp://example/m/s', dict(name='example'), m_s),
    ('ixmp://example/m/s#42', dict(name='example'),
     dict(model='m', scenario='s', version=42)),
    ('ixmp://example/m/s', dict(name='example'), m_s),
    ('ixmp://local/m/s', dict(name='local'), m_s),
    ('ixmp://local/m/s/foo/bar', dict(name='local'),
     dict(model='m', scenario='s/foo/bar')),
    ('m/s#42', dict(), dict(model='m', scenario='s', version=42)),

    # Invalid values
    # Wrong scheme
    param('foo://example/m/s', None, None,
          marks=mark.xfail(raises=ValueError)),
    # No Scenario name
    param('ixmp://example/m', None, None,
          marks=mark.xfail(raises=ValueError)),
    # Version not an integer
    param('ixmp://example/m#notaversion', None, None,
          marks=mark.xfail(raises=ValueError)),
    # Query string not supported
    param('ixmp://example/m/s?querystring', None, None,
          marks=mark.xfail(raises=ValueError)),
]


@pytest.mark.parametrize('url, p, s', URLS)
def test_parse_url(url, p, s):
    platform_info, scenario_info = utils.parse_url(url)

    # Expected platform and scenario information is returned
    assert platform_info == p
    assert scenario_info == s
