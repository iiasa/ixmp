"""Tests for ixmp.util."""

import logging

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
from pytest import mark, param

from ixmp import Scenario, util
from ixmp.testing import make_dantzig, populate_test_platform


class TestDeprecatedPathFinder:
    def test_import(self):
        with pytest.warns(
            DeprecationWarning,
            match="Importing from 'ixmp.reporting.computations' is deprecated and will "
            "fail in a future version. Use 'ixmp.report.operator'.",
        ):
            import ixmp.reporting.computations  # type: ignore  # noqa: F401

    @pytest.mark.filterwarnings("ignore")
    def test_import1(self):
        """utils can be imported from ixmp, but raises DeprecationWarning."""
        from ixmp import utils

        assert "diff" in dir(utils)

    def test_importerror(self):
        with pytest.warns(DeprecationWarning), pytest.raises(ImportError):
            import ixmp.reporting.foo  # type: ignore  # noqa: F401


def test_check_year():
    # If y is a string value, raise a Value Error.

    y1 = "a"
    s1 = "a"
    with pytest.raises(ValueError):
        assert util.check_year(y1, s1)

    # If y = None.

    y2 = None
    s2 = None

    assert util.check_year(y2, s2) is None

    # If y is integer.

    y3 = 4
    s3 = 4

    assert util.check_year(y3, s3) is True


def test_diff_identical(test_mp, request):
    """diff() of identical Scenarios."""
    scen_a = make_dantzig(test_mp, request=request)
    scen_b = make_dantzig(test_mp, request=request)

    # Compare identical scenarios: produces data of same length
    for name, df in util.diff(scen_a, scen_b):
        data_a = util.maybe_convert_scalar(scen_a.par(name))
        assert len(data_a) == len(df)

    # Compare identical scenarios, with filters
    iterator = util.diff(scen_a, scen_b, filters=dict(i=["seattle"]))
    for (name, df), (exp_name, N) in zip(iterator, [("a", 1), ("d", 3)]):
        assert exp_name == name and len(df) == N


# FIXME I don't see why IXMP4Backend shouldn't support this, but it's failing.
@pytest.mark.jdbc
def test_diff_data(test_mp, request):
    """diff() when Scenarios contain the same items, but different data."""
    scen_a = make_dantzig(test_mp, request=request)
    scen_b = make_dantzig(test_mp, request=request)

    # Modify `scen_a` and `scen_b`
    scen_a.check_out()
    scen_b.check_out()

    # Remove elements from "b"
    drop_args = dict(labels=["value", "unit"], axis=1)
    scen_a.remove_par("b", scen_a.par("b").iloc[0:1, :].drop(**drop_args))
    scen_b.remove_par("b", scen_b.par("b").iloc[1:2, :].drop(**drop_args))
    # Remove elements from "d"
    scen_a.remove_par("d", scen_a.par("d").query("i == 'san-diego'").drop(**drop_args))
    # Modify values in "d"
    scen_b.add_par("d", scen_b.par("d").query("i == 'seattle'").assign(value=123.4))

    # Expected results
    exp_b = pd.DataFrame(
        [
            ["chicago", 300.0, "cases", np.nan, np.nan, "left_only"],
            ["new-york", np.nan, np.nan, 325.0, "cases", "right_only"],
            ["topeka", 275.0, "cases", 275.0, "cases", "both"],
        ],
        columns="j value_a unit_a value_b unit_b _merge".split(),
    )
    exp_d = pd.DataFrame(
        [
            ["san-diego", "chicago", np.nan, np.nan, 1.8, "km", "right_only"],
            ["san-diego", "new-york", np.nan, np.nan, 2.5, "km", "right_only"],
            ["san-diego", "topeka", np.nan, np.nan, 1.4, "km", "right_only"],
            ["seattle", "chicago", 1.7, "km", 123.4, "km", "both"],
            ["seattle", "new-york", 2.5, "km", 123.4, "km", "both"],
            ["seattle", "topeka", 1.8, "km", 123.4, "km", "both"],
        ],
        columns="i j value_a unit_a value_b unit_b _merge".split(),
    )

    # Use the specific categorical produced by pd.merge()
    merge_cat = pd.CategoricalDtype(["left_only", "right_only", "both"])
    exp_b = exp_b.astype(dict(_merge=merge_cat))
    exp_d = exp_d.astype(dict(_merge=merge_cat))

    # Compare different scenarios without filters
    for name, df in util.diff(scen_a, scen_b):
        if name == "b":
            pdt.assert_frame_equal(exp_b, df)
        elif name == "d":
            pdt.assert_frame_equal(exp_d, df)

    # Compare different scenarios with filters
    iterator = util.diff(scen_a, scen_b, filters=dict(j=["chicago"]))
    for name, df in iterator:
        # Same as above, except only the filtered rows should appear
        if name == "b":
            pdt.assert_frame_equal(exp_b.iloc[0:1, :], df)
        elif name == "d":
            pdt.assert_frame_equal(exp_d.iloc[[0, 3], :].reset_index(drop=True), df)


def test_diff_items(test_mp, request):
    """diff() when Scenarios contain the different items."""
    scen_a = make_dantzig(test_mp, request=request)
    scen_b = make_dantzig(test_mp, request=request)

    # Modify `scen_a` and `scen_b`
    scen_a.check_out()
    scen_b.check_out()

    # Remove items
    scen_a.remove_par("b")
    scen_b.remove_par("d")

    # Compare different scenarios without filters
    for name, df in util.diff(scen_a, scen_b):
        pass  # No check on the contents

    # Compare different scenarios with filters
    iterator = util.diff(scen_a, scen_b, filters=dict(j=["chicago"]))
    for name, df in iterator:
        pass  # No check of the contents


# TODO IXMP4Backend doesn't handle retrieval of scalars correctly yet;
# but look here for a test case!
@pytest.mark.jdbc
def test_discard_on_error(caplog, test_mp, request):
    caplog.set_level(logging.INFO, "ixmp.util")

    # Create a test scenario, checked-in state
    s = make_dantzig(test_mp, request=request)
    url = s.url

    # Some actions that don't trigger exceptions
    assert dict(value=90, unit="USD/km") == s.scalar("f")
    s.check_out()
    s.change_scalar("f", 100, "USD/km")

    # Catch the deliberately-raised exception so that the test passes
    with pytest.raises(KeyError):
        # Trigger KeyError and the discard_on_error() behaviour
        with util.discard_on_error(s):
            s.add_par("d", pd.DataFrame([["foo", "bar", 1.0, "kg"]]))

    # Exception was caught and logged
    assert caplog.messages[-3].startswith("Avoid locking ")
    assert [
        "Discard scenario changes",
        "Close database connection",
    ] == caplog.messages[-2:]

    # Re-load the mp and the scenario
    with pytest.raises(RuntimeError):
        # Fails because the connection to test_mp was closed by discard_on_error()
        s2 = Scenario(test_mp, **util.parse_url(url)[1])

    # Reopen the connection
    test_mp.open_db()

    # Now the scenario can be reloaded
    s2 = Scenario(test_mp, **util.parse_url(url)[1])
    assert s2 is not s  # Different object instance than above

    # Data modification above was discarded by discard_on_error()
    assert dict(value=90, unit="USD/km") == s.scalar("f")


def test_filtered():
    df = pd.DataFrame()
    assert df is util.filtered(df, filters=None)


def test_isscalar():
    with pytest.warns(DeprecationWarning):
        assert False is util.isscalar([3, 4])


def test_logger_deprecated():
    with pytest.warns(DeprecationWarning):
        util.logger()


m_s = dict(model="m", scenario="s")

URLS = [
    ("ixmp://example/m/s", dict(name="example"), m_s),
    (
        "ixmp://example/m/s#42",
        dict(name="example"),
        dict(model="m", scenario="s", version=42),
    ),
    ("ixmp://example/m/s", dict(name="example"), m_s),
    ("ixmp://local/m/s", dict(name="local"), m_s),
    (
        "ixmp://local/m/s/foo/bar",
        dict(name="local"),
        dict(model="m", scenario="s/foo/bar"),
    ),
    ("m/s#42", dict(), dict(model="m", scenario="s", version=42)),
    # Invalid values
    # Wrong scheme
    param("foo://example/m/s", None, None, marks=mark.xfail(raises=ValueError)),
    # No Scenario name
    param("ixmp://example/m", None, None, marks=mark.xfail(raises=ValueError)),
    # Version not an integer
    param(
        "ixmp://example/m/s#notaversion",
        None,
        None,
        marks=mark.xfail(raises=ValueError),
    ),
    # Query string not supported
    param(
        "ixmp://example/m/s?querystring",
        None,
        None,
        marks=mark.xfail(raises=ValueError),
    ),
]


@pytest.mark.parametrize("url, p, s", URLS)
def test_parse_url(url, p, s):
    platform_info, scenario_info = util.parse_url(url)

    # Expected platform and scenario information is returned
    assert platform_info == p
    assert scenario_info == s


def test_format_scenario_list(test_mp_f):
    # Use the function-scoped fixture for precise version numbers
    mp = test_mp_f
    populate_test_platform(mp)

    # Expected results

    assert [
        "",
        "Douglas Adams/",
        "  Hitchhiker#1  ",
        "",
        "canning problem/",
        "  standard#2    1–3",
        "",
        "2 model name(s)",
        "2 scenario name(s)",
        "2 (model, scenario) combination(s)",
        "4 total scenarios",
    ] == util.format_scenario_list(mp)

    # With as_url=True
    assert list(
        map(
            lambda s: s.format(mp.name),
            [
                "ixmp://{}/Douglas Adams/Hitchhiker#1",
                "ixmp://{}/canning problem/standard#2",
            ],
        )
    ) == util.format_scenario_list(mp, as_url=True)


# IXMP4Backend doesn't have proper commits yet, so these never raise RuntimeErrors
@pytest.mark.jdbc
def test_maybe_commit(caplog, test_mp):
    s = Scenario(test_mp, "maybe_commit", "maybe_commit", version="new")

    # A new Scenario is not committed, so this works
    assert util.maybe_commit(s, True, message="foo") is True

    # *s* is already commited. No commit is performed, but the function call
    # succeeds and a message is logged
    caplog.set_level(logging.INFO, logger="ixmp")
    assert util.maybe_commit(s, True, message="foo") is False
    assert caplog.messages[-1].startswith("maybe_commit() didn't commit: ")
