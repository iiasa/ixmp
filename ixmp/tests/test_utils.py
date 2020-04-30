"""Tests for ixmp.utils."""
import pytest
from pytest import mark, param

from ixmp import Scenario, utils
from ixmp.testing import populate_test_platform


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


def test_format_scenario_list(test_mp):
    populate_test_platform(test_mp)

    exp = [
        '',
        'Douglas Adams/',
        '  Hitchhiker#1',
        '',
        'canning problem/',
        '  standard#2  1–3',
        '',
        '2 model name(s)',
        '2 scenario name(s)',
        '2 (model, scenario) combination(s)',
        '4 total scenarios',
    ]

    # Expected results
    assert exp == utils.format_scenario_list(test_mp)

    # With as_url=True
    exp = list(map(lambda s: s.format(test_mp.name), [
        'ixmp://{}/Douglas Adams/Hitchhiker#1',
        'ixmp://{}/canning problem/standard#2',
    ]))
    assert exp == utils.format_scenario_list(test_mp, as_url=True)


def test_maybe_commit(caplog, test_mp):
    s = Scenario(test_mp, 'maybe_commit', 'maybe_commit', version='new')

    # A new Scenario is not committed, so this works
    assert utils.maybe_commit(s, True, message='foo') is True

    # *s* is already commited. No commit is performed, but the function call
    # succeeds and a message is logged
    assert utils.maybe_commit(s, True, message='foo') is False
    assert caplog.messages[-1].startswith("maybe_commit() didn't commit: ")
