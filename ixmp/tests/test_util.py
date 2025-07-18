"""Tests for ixmp.util."""

import logging
from contextlib import nullcontext
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest
from pytest import mark, param

from ixmp import Scenario, util
from ixmp.testing import make_dantzig, populate_test_platform
from ixmp.util.ixmp4 import is_ixmp4backend

if TYPE_CHECKING:
    from ixmp.core.platform import Platform
    from ixmp.types import TimeSeriesIdentifiers


class TestDeprecatedPathFinder:
    def test_import(self) -> None:
        with pytest.warns(
            DeprecationWarning,
            match="Importing from 'ixmp.reporting.computations' is deprecated and will "
            "fail in a future version. Use 'ixmp.report.operator'.",
        ):
            import ixmp.reporting.computations  # noqa: F401

    @pytest.mark.filterwarnings("ignore")
    def test_import1(self) -> None:
        """utils can be imported from ixmp, but raises DeprecationWarning."""
        from ixmp import utils

        assert "diff" in dir(utils)

    def test_importerror(self) -> None:
        with pytest.warns(DeprecationWarning), pytest.raises(ImportError):
            import ixmp.reporting.foo  # noqa: F401


def test_check_year() -> None:
    # If y is a string value, raise a Value Error.

    y1 = "a"
    s1 = "a"
    with pytest.raises(ValueError):
        assert util.check_year(y1, s1)  # type: ignore[arg-type]

    # If y = None.

    y2 = None
    s2 = None

    assert util.check_year(y2, s2) is None

    # If y is integer.

    y3 = 4
    s3 = 4

    assert util.check_year(y3, s3) is True


def test_diff_identical(test_mp: "Platform", request: pytest.FixtureRequest) -> None:
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


def test_diff_data(test_mp: "Platform", request: pytest.FixtureRequest) -> None:
    """diff() when Scenarios contain the same items, but different data."""
    scen_a = make_dantzig(test_mp, request=request)
    scen_b = make_dantzig(test_mp, request=request)

    # Modify `scen_a` and `scen_b`
    for scen, idx in (scen_a, slice(0, 1)), (scen_b, slice(1, 2)):
        df_b = scen.par("b")
        df_d = scen.par("d")
        assert isinstance(df_b, pd.DataFrame) and isinstance(df_d, pd.DataFrame)

        with scen.transact():
            # Remove elements from parameter "b"
            scen.remove_par("b", df_b.sort_values(by="j").iloc[idx, :])

            # Either remove (`scen_a`) or modify (`scen_b`) elements in parameter "d"
            if scen is scen_a:
                scen.remove_par("d", df_d.query("i == 'san-diego'"))
            else:
                scen.add_par("d", df_d.query("i == 'seattle'").assign(value=123.4))

    # Use the specific categorical produced by pd.merge()
    merge_cat = pd.CategoricalDtype(["left_only", "right_only", "both"])

    # Expected results
    exp_b = pd.DataFrame(
        [
            ["chicago", np.nan, np.nan, 300.0, "cases", "right_only"],
            ["new-york", 325.0, "cases", np.nan, np.nan, "left_only"],
            ["topeka", 275.0, "cases", 275.0, "cases", "both"],
        ],
        columns="j value_a unit_a value_b unit_b _merge".split(),
    ).astype({"_merge": merge_cat})
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
    ).astype({"_merge": merge_cat})

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


def test_diff_items(test_mp: "Platform", request: pytest.FixtureRequest) -> None:
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
    names = set()
    for name, df in util.diff(scen_a, scen_b):
        names.add(name)

    # All items are included in the comparison, e.g. "b" from scen_b, "d" from scen_a.
    assert {"a", "b", "d", "f"} == names

    # Compare different scenarios with filters
    names = set()
    for name, df in util.diff(scen_a, scen_b, filters=dict(j=["chicago"])):
        names.add(name)

    # Only the parameters indexed by "j" are compared
    assert {"b", "d"} == names


def test_discard_on_error(
    caplog: pytest.LogCaptureFixture,
    test_mp: "Platform",
    request: pytest.FixtureRequest,
) -> None:
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
    with (
        nullcontext()
        if is_ixmp4backend(test_mp._backend)
        else pytest.raises(RuntimeError)
    ):
        # Fails because the connection to test_mp was closed by discard_on_error()
        s2 = Scenario(test_mp, **util.parse_url(url)[1])

    # Reopen the connection
    test_mp.open_db()

    # Now the scenario can be reloaded
    s2 = Scenario(test_mp, **util.parse_url(url)[1])
    assert s2 is not s  # Different object instance than above

    # Data modification above was discarded by discard_on_error()
    # NB Currently does *not* pass with IXMP4Backend
    assert dict(value=90, unit="USD/km") == s.scalar("f") or is_ixmp4backend(
        test_mp._backend
    )


def test_filtered() -> None:
    df = pd.DataFrame()
    assert df is util.filtered(df, filters=None)


def test_isscalar() -> None:
    with pytest.warns(DeprecationWarning):
        assert False is util.isscalar([3, 4])


def test_logger_deprecated() -> None:
    with pytest.warns(DeprecationWarning):
        util.logger()


m_s: "TimeSeriesIdentifiers" = dict(model="m", scenario="s")

URLS: list[tuple[str, Optional[dict[str, str]], Optional["TimeSeriesIdentifiers"]]] = [
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
    # NOTE Somehow, pytest doesn't export the ParameterSet for annotations, but all of
    # these are intentionally failing, anyway
    param("foo://example/m/s", None, None, marks=mark.xfail(raises=ValueError)),  # type: ignore[list-item]
    # No Scenario name
    param("ixmp://example/m", None, None, marks=mark.xfail(raises=ValueError)),  # type: ignore[list-item]
    # Version not an integer
    param(
        "ixmp://example/m/s#notaversion",
        None,
        None,
        marks=mark.xfail(raises=ValueError),
    ),  # type: ignore[list-item]
    # Query string not supported
    param(
        "ixmp://example/m/s?querystring",
        None,
        None,
        marks=mark.xfail(raises=ValueError),
    ),  # type: ignore[list-item]
]


@pytest.mark.parametrize("url, p, s", URLS)
def test_parse_url(
    url: str, p: Optional[dict[str, str]], s: Optional["TimeSeriesIdentifiers"]
) -> None:
    platform_info, scenario_info = util.parse_url(url)

    # Expected platform and scenario information is returned
    assert platform_info == p
    assert scenario_info == s


def test_format_scenario_list(test_mp_f: "Platform") -> None:
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
def test_maybe_commit(caplog: pytest.LogCaptureFixture, test_mp: "Platform") -> None:
    s = Scenario(test_mp, "maybe_commit", "maybe_commit", version="new")

    # A new Scenario is not committed, so this works
    assert util.maybe_commit(s, True, message="foo") is True

    # *s* is already commited. No commit is performed, but the function call
    # succeeds and a message is logged
    caplog.set_level(logging.INFO, logger="ixmp")
    assert util.maybe_commit(s, True, message="foo") is False
    assert caplog.messages[-1].startswith("maybe_commit() didn't commit: ")
