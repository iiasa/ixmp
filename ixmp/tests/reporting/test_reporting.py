"""Tests for ixmp.reporting."""
import logging
import os

import numpy as np
import pandas as pd
import pint
import pytest
import xarray as xr

import ixmp
import ixmp.reporting
from ixmp.reporting import (
    RENAME_DIMS,
    ComputationError,
    Key,
    KeyExistsError,
    MissingKeyError,
    Quantity,
    Reporter,
    computations,
    configure,
)
from ixmp.testing import (
    assert_logs,
    assert_qty_allclose,
    assert_qty_equal,
    make_dantzig,
)

from . import add_test_data

pytestmark = pytest.mark.usefixtures("parametrize_quantity_class")

test_args = ("Douglas Adams", "Hitchhiker")

TS_DF = {"year": [2010, 2020], "value": [23.7, 23.8]}
TS_DF = pd.DataFrame.from_dict(TS_DF)
TS_DF["region"] = "World"
TS_DF["variable"] = "Testing"
TS_DF["unit"] = "???"


@pytest.fixture
def scenario(test_mp):
    # from test_feature_timeseries.test_new_timeseries_as_year_value
    scen = ixmp.Scenario(test_mp, *test_args, version="new", annotation="foo")
    scen.add_timeseries(TS_DF)
    scen.commit("importing a testing timeseries")
    return scen


@pytest.fixture(scope="session")
def ureg():
    yield pint.get_application_registry()


def test_configure(test_mp, test_data_path):
    # TODO test: configuration keys 'units', 'replace_units'

    # Configure globally; reads 'rename_dims' section
    configure(rename_dims={"i": "i_renamed"})

    # Reporting uses the RENAME_DIMS mapping of 'i' to 'i_renamed'
    scen = make_dantzig(test_mp)
    rep = Reporter.from_scenario(scen)
    assert "d:i_renamed-j" in rep, rep.graph.keys()
    assert ["seattle", "san-diego"] == rep.get("i_renamed")

    # Original name 'i' are not found in the reporter
    assert "d:i-j" not in rep, rep.graph.keys()
    pytest.raises(KeyError, rep.get, "i")

    # Remove the configuration for renaming 'i', so that other tests work
    RENAME_DIMS.pop("i")


def test_reporter_add():
    """Adding computations that refer to missing keys raises KeyError."""
    r = Reporter()
    r.add("a", 3)
    r.add("d", 4)

    # Adding an existing key with strict=True
    with pytest.raises(KeyExistsError, match=r"key 'a' already exists"):
        r.add("a", 5, strict=True)

    def gen(other):  # pragma: no cover
        """A generator for apply()."""
        return (lambda a, b: a * b, "a", other)

    def msg(*keys):
        """Return a regex for str(MissingKeyError(*keys))."""
        return f"required keys {repr(tuple(keys))} not defined".replace(
            "(", "\\("
        ).replace(")", "\\)")

    # One missing key
    with pytest.raises(MissingKeyError, match=msg("b")):
        r.add_product("ab", "a", "b")

    # Two missing keys
    with pytest.raises(MissingKeyError, match=msg("c", "b")):
        r.add_product("abc", "c", "a", "b")

    # Using apply() targeted at non-existent keys also raises an Exception
    with pytest.raises(MissingKeyError, match=msg("e", "f")):
        r.apply(gen, "d", "e", "f")

    # add(..., strict=True) checks str or Key arguments
    g = Key("g", "hi")
    with pytest.raises(MissingKeyError, match=msg("b", g)):
        r.add("foo", (computations.product, "a", "b", g), strict=True)

    # aggregate() and disaggregate() call add(), which raises the exception
    with pytest.raises(MissingKeyError, match=msg(g)):
        r.aggregate(g, "tag", "i")
    with pytest.raises(MissingKeyError, match=msg(g)):
        r.disaggregate(g, "j")

    # add(..., sums=True) also adds partial sums
    r.add("foo:a-b-c", [], sums=True)
    assert "foo:b" in r

    # add(name, ...) where name is the name of a computation
    r.add("select", "bar", "a", indexers={"dim": ["d0", "d1", "d2"]})

    # add(name, ...) with keyword arguments not recognized by the computation
    # raises an exception
    msg = "unexpected keyword argument 'bad_kwarg'"
    with pytest.raises(TypeError, match=msg):
        r.add("select", "bar", "a", bad_kwarg="foo", index=True)


def test_reporter_add_queue():
    r = Reporter()
    r.add("foo-0", (lambda x: x, 42))

    # A computation
    def _product(a, b):
        return a * b

    # A queue of computations to add. Only foo-1 succeeds on the first pass;
    # only foo-2 on the second pass, etc.
    strict = dict(strict=True)
    queue = [
        (("foo-4", _product, "foo-3", 10), strict),
        (("foo-3", _product, "foo-2", 10), strict),
        (("foo-2", _product, "foo-1", 10), strict),
        (("foo-1", _product, "foo-0", 10), {}),
    ]

    # Maximum 3 attempts → foo-4 fails on the start of the 3rd pass
    with pytest.raises(MissingKeyError, match="foo-3"):
        r.add(queue, max_tries=3, fail="raise")

    # But foo-2 was successfully added on the second pass, and gives the
    # correct result
    assert r.get("foo-2") == 42 * 10 * 10


def test_reporter_add_product(test_mp, ureg):
    scen = ixmp.Scenario(test_mp, "reporter_add_product", "reporter_add_product", "new")
    *_, x = add_test_data(scen)
    rep = Reporter.from_scenario(scen)

    # add_product() works
    key = rep.add_product("x squared", "x", "x", sums=True)

    # Product has the expected dimensions
    assert key == "x squared:t-y"

    # Product has the expected value
    exp = Quantity(x * x, name="x")
    exp.attrs["_unit"] = ureg("kilogram ** 2").units
    assert_qty_equal(exp, rep.get(key))

    # add('product', ...) works
    key = rep.add("product", "x_squared", "x", "x", sums=True)


def test_reporter_from_scenario(scenario):
    r = Reporter.from_scenario(scenario)

    r.finalize(scenario)

    assert "scenario" in r.graph


def test_reporter_from_dantzig(test_mp, ureg):
    scen = make_dantzig(test_mp, solve=True)

    # Reporter.from_scenario can handle the Dantzig problem
    rep = Reporter.from_scenario(scen)

    # Partial sums are available automatically (d is defined over i and j)
    d_i = rep.get("d:i")

    # Units pass through summation
    assert d_i.attrs["_unit"] == ureg.parse_units("km")

    # Summation across all dimensions results a 1-element Quantity
    d = rep.get("d:")
    assert d.shape == ((1,) if Quantity.CLASS == "AttrSeries" else tuple())
    assert d.size == 1
    assert np.isclose(d.values, 11.7)

    # Weighted sum
    weights = Quantity(
        xr.DataArray([1, 2, 3], coords=["chicago new-york topeka".split()], dims=["j"])
    )
    new_key = rep.aggregate("d:i-j", "weighted", "j", weights)

    # ...produces the expected new key with the summed dimension removed and
    # tag added
    assert new_key == "d:i:weighted"

    # ...produces the expected new value
    obs = rep.get(new_key)
    d_ij = rep.get("d:i-j")
    exp = Quantity(
        (d_ij * weights).sum(dim=["j"]) / weights.sum(dim=["j"]),
        attrs=d_ij.attrs,
    )

    assert_qty_equal(exp, obs)

    # Disaggregation with explicit data
    # (cases of canned food 'p'acked in oil or water)
    shares = xr.DataArray([0.8, 0.2], coords=[["oil", "water"]], dims=["p"])
    new_key = rep.disaggregate("b:j", "p", args=[Quantity(shares)])

    # ...produces the expected key with new dimension added
    assert new_key == "b:j-p"

    b_jp = rep.get("b:j-p")

    # Units pass through disaggregation
    assert b_jp.attrs["_unit"] == "cases"

    # Set elements are available
    assert rep.get("j") == ["new-york", "chicago", "topeka"]

    # 'all' key retrieves all quantities
    obs = {da.name for da in rep.get("all")}
    exp = set(
        (
            "a b d f x z cost cost-margin demand demand-margin supply " "supply-margin"
        ).split()
    )
    assert obs == exp

    # Shorthand for retrieving a full key name
    assert rep.full_key("d") == "d:i-j" and isinstance(rep.full_key("d"), Key)


def test_reporter_read_config(test_mp, test_data_path):
    scen = make_dantzig(test_mp)
    rep = Reporter.from_scenario(scen)

    # Configuration can be read from file
    rep.configure(test_data_path / "report-config-0.yaml")

    # Data from configured file is available
    assert rep.get("d_check").loc["seattle", "chicago"] == 1.7


def test_reporter_apply():
    # Reporter with two scalar values
    r = Reporter()
    r.add("foo", (lambda x: x, 42))
    r.add("bar", (lambda x: x, 11))

    N = len(r.keys())

    # A computation
    def _product(a, b):
        return a * b

    # A generator function that yields keys and computations
    def baz_qux(key):
        yield key + ":baz", (_product, key, 0.5)
        yield key + ":qux", (_product, key, 1.1)

    # Apply the generator to two targets
    r.apply(baz_qux, "foo")
    r.apply(baz_qux, "bar")

    # Four computations were added to the reporter
    N += 4
    assert len(r.keys()) == N
    assert r.get("foo:baz") == 42 * 0.5
    assert r.get("foo:qux") == 42 * 1.1
    assert r.get("bar:baz") == 11 * 0.5
    assert r.get("bar:qux") == 11 * 1.1

    # A generator that takes two arguments
    def twoarg(key1, key2):
        yield key1 + "__" + key2, (_product, key1, key2)

    r.apply(twoarg, "foo:baz", "bar:qux")

    # One computation added to the reporter
    N += 1
    assert len(r.keys()) == N
    assert r.get("foo:baz__bar:qux") == 42 * 0.5 * 11 * 1.1

    # A useless generator that does nothing
    def useless():
        return

    r.apply(useless)

    # Nothing added to the reporter
    assert len(r.keys()) == N

    # Adding with a generator that takes Reporter as the first argument
    def add_many(rep: Reporter, max=5):
        [rep.add(f"foo{x}", _product, "foo", x) for x in range(max)]

    r.apply(add_many, max=10)

    # Function was called, adding keys
    assert len(r.keys()) == N + 10

    # Keys work
    assert r.get("foo9") == 42 * 9


def test_reporter_disaggregate():
    r = Reporter()
    foo = Key("foo", ["a", "b", "c"])
    r.add(foo, "<foo data>")
    r.add("d_shares", "<share data>")

    # Disaggregation works
    r.disaggregate(foo, "d", args=["d_shares"])

    assert "foo:a-b-c-d" in r.graph
    assert r.graph["foo:a-b-c-d"] == (
        computations.disaggregate_shares,
        "foo:a-b-c",
        "d_shares",
    )

    # Invalid method
    with pytest.raises(ValueError):
        r.disaggregate(foo, "d", method="baz")


def test_reporter_file(tmp_path):
    r = Reporter()

    # Path to a temporary file
    p = tmp_path / "foo.txt"

    # File can be added to the Reporter before it is created, because the file
    # is not read until/unless required
    k1 = r.add_file(p)

    # File has the expected key
    assert k1 == "file:foo.txt"

    # Add some contents to the file
    p.write_text("Hello, world!")

    # The file's contents can be read through the Reporter
    assert r.get("file:foo.txt") == "Hello, world!"

    # Write the report to file
    p2 = tmp_path / "bar.txt"
    r.write("file:foo.txt", p2)

    # Write using a string path
    r.write("file:foo.txt", str(p2))

    # The Reporter produces the expected output file
    assert p2.read_text() == "Hello, world!"


def test_file_formats(test_data_path, tmp_path):
    r = Reporter()

    expected = Quantity(
        pd.read_csv(test_data_path / "report-input0.csv", index_col=["i", "j"])[
            "value"
        ],
        units="km",
    )

    # CSV file is automatically parsed to xr.DataArray
    p1 = test_data_path / "report-input0.csv"
    k = r.add_file(p1, units=pint.Unit("km"))
    assert_qty_equal(r.get(k), expected)

    # Dimensions can be specified
    p2 = test_data_path / "report-input1.csv"
    k2 = r.add_file(p2, dims=dict(i="i", j_dim="j"))
    assert_qty_equal(r.get(k), r.get(k2))

    # Units are loaded from a column
    assert r.get(k2).attrs["_unit"] == pint.Unit("km")

    # Specifying units that do not match file contents → ComputationError
    r.add_file(p2, key="bad", dims=dict(i="i", j_dim="j"), units="kg")
    with pytest.raises(ComputationError):
        r.get("bad")

    # Write to CSV
    p3 = tmp_path / "report-output.csv"
    r.write(k, p3)

    # Output is identical to input file, except for order
    assert sorted(p1.read_text().split("\n")) == sorted(p3.read_text().split("\n"))

    # Write to Excel
    p4 = tmp_path / "report-output.xlsx"
    r.write(k, p4)
    # TODO check the contents of the Excel file


def test_reporter_full_key():
    r = Reporter()

    # Without index, the full key cannot be retrieved
    r.add("a:i-j-k", [])
    with pytest.raises(KeyError, match="a"):
        r.full_key("a")

    # Using index=True adds the full key to the index
    r.add("a:i-j-k", [], index=True)
    assert r.full_key("a") == "a:i-j-k"

    # The full key can be retrieved by giving only some of the indices
    assert r.full_key("a:j") == "a:i-j-k"

    # Same with a tag
    r.add("a:i-j-k:foo", [], index=True)
    # Original and tagged key can both be retrieved
    assert r.full_key("a") == "a:i-j-k"
    assert r.full_key("a::foo") == "a:i-j-k:foo"


def test_units(ureg):
    """Test handling of units within Reporter computations."""
    r = Reporter()

    # Create some dummy data
    dims = dict(coords=["a b c".split()], dims=["x"])
    r.add("energy:x", Quantity(xr.DataArray([1.0, 3, 8], **dims), units="MJ"))
    r.add("time", Quantity(xr.DataArray([5.0, 6, 8], **dims), units="hour"))
    r.add("efficiency", Quantity(xr.DataArray([0.9, 0.8, 0.95], **dims)))

    # Aggregation preserves units
    r.add("energy", (computations.sum, "energy:x", None, ["x"]))
    assert r.get("energy").attrs["_unit"] == ureg.parse_units("MJ")

    # Units are derived for a ratio of two quantities
    r.add("power", (computations.ratio, "energy:x", "time"))
    assert r.get("power").attrs["_unit"] == ureg.parse_units("MJ/hour")

    # Product of dimensioned and dimensionless quantities keeps the former
    r.add("energy2", (computations.product, "energy:x", "efficiency"))
    assert r.get("energy2").attrs["_unit"] == ureg.parse_units("MJ")


def test_platform_units(test_mp, caplog, ureg):
    """Test handling of units from ixmp.Platform.

    test_mp is loaded with some units including '-', '???', 'G$', etc. which
    are not parseable with pint; and others which are not defined in a default
    pint.UnitRegistry. These tests check the handling of those units.
    """

    # Prepare a Scenario with test data
    scen = ixmp.Scenario(
        test_mp, "reporting_platform_units", "reporting_platform_units", "new"
    )
    t, t_foo, t_bar, x = add_test_data(scen)
    rep = Reporter.from_scenario(scen)
    x_key = rep.full_key("x")

    # Convert 'x' to dataframe
    x = x.to_series().rename("value").reset_index()

    # Exception message, formatted as a regular expression
    msg = r"unit '{}' cannot be parsed; contains invalid character\(s\) '{}'"

    # Unit and components for the regex
    bad_units = [("-", "-", "-"), ("???", r"\?\?\?", r"\?"), ("E$", r"E\$", r"\$")]
    for unit, expr, chars in bad_units:
        # Add the unit
        test_mp.add_unit(unit)

        # Overwrite the parameter
        x["unit"] = unit
        scen.add_par("x", x)

        # Parsing units with invalid chars raises an intelligible exception
        with pytest.raises(ComputationError, match=msg.format(expr, chars)):
            rep.get(x_key)

    # Now using parseable but unrecognized units
    x["unit"] = "USD/kWa"
    scen.add_par("x", x)

    # Unrecognized units are added automatically, with log messages emitted
    caplog.clear()
    rep.get(x_key)
    # NB cannot use assert_logs here. reporting.utils.parse_units uses the
    #    pint application registry, so depending which tests are run and in
    #    which order, this unit may already be defined.
    if len(caplog.messages):
        assert "Add unit definition: kWa = [kWa]" in caplog.messages

    # Mix of recognized/unrecognized units can be added: USD is already in the
    # unit registry, so is not re-added
    x["unit"] = "USD/pkm"
    test_mp.add_unit("USD/pkm")
    scen.add_par("x", x)

    caplog.clear()
    rep.get(x_key)
    assert not any("Add unit definition: USD = [USD]" in m for m in caplog.messages)

    # Mixed units are discarded
    x.loc[0, "unit"] = "kg"
    scen.add_par("x", x)

    with assert_logs(
        caplog, "x: mixed units ['kg', 'USD/pkm'] discarded", at_level=logging.INFO
    ):
        rep.get(x_key)

    # Configured unit substitutions are applied
    rep.graph["config"]["units"] = dict(apply=dict(x="USD/pkm"))

    with assert_logs(
        caplog, "x: replace units dimensionless with USD/pkm", at_level=logging.INFO
    ):
        x = rep.get(x_key)

    # Applied units are pint objects with the correct dimensionality
    unit = x.attrs["_unit"]
    assert isinstance(unit, pint.Unit)
    assert unit.dimensionality == {"[USD]": 1, "[pkm]": -1}


def test_reporter_describe(test_mp, test_data_path, capsys):
    scen = make_dantzig(test_mp)
    r = Reporter.from_scenario(scen)

    # hexadecimal ID of *scen*
    id_ = (
        hex(id(scen))
        if os.name != "nt"
        else "{:#018X}".format(id(scen)).replace("X", "x")
    )

    # Describe one key
    desc1 = """'d:i':
- sum(dimensions=['j'], weights=None, ...)
- 'd:i-j':
  - data_for_quantity('par', 'd', 'value', ...)
  - 'scenario':
    - <ixmp.core.Scenario object at {id}>
  - 'config':
    - {{'filters': {{}}}}""".format(
        id=id_
    )
    assert desc1 == r.describe("d:i")

    # With quiet=True (default), nothing is printed to stdout
    out1, _ = capsys.readouterr()
    assert "" == out1

    # With quiet=False, description is also printed to stdout
    assert desc1 == r.describe("d:i", quiet=False)
    out1, _ = capsys.readouterr()
    assert desc1 + "\n" == out1

    # Description of all keys is as expected
    desc2 = (test_data_path / "report-describe.txt").read_text().format(id=id_)
    assert desc2 == r.describe(quiet=False) + "\n"

    # Since quiet=False, description is also printed to stdout
    out2, _ = capsys.readouterr()
    assert desc2 == out2


def test_reporter_visualize(test_mp, tmp_path):
    scen = make_dantzig(test_mp)
    r = Reporter.from_scenario(scen)

    r.visualize(str(tmp_path / "visualize.png"))

    # TODO compare to a specimen


def test_cli(ixmp_cli, test_mp, test_data_path):
    # Put something in the database
    make_dantzig(test_mp)
    test_mp.close_db()

    platform_name = test_mp.name

    # Delete the platform/close the database connection
    del test_mp

    cmd = [
        "--platform",
        platform_name,
        "--model",
        "canning problem",
        "--scenario",
        "standard",
        "report",
        "--config",
        str(test_data_path / "report-config-0.yaml"),
        "d_check",
    ]

    # 'report' command runs
    result = ixmp_cli.invoke(cmd)
    assert result.exit_code == 0

    # TODO warning should be logged

    # Reporting produces the expected command-line output
    assert result.output.endswith(
        "i          j       "  # Trailing whitespace
        """
san-diego  chicago     1.8
           new-york    2.5
           topeka      1.4
seattle    chicago     1.7
           new-york    2.5
           topeka      1.8
Name: value, dtype: float64
"""
    )


def test_aggregate(test_mp):
    scen = ixmp.Scenario(test_mp, "Group reporting", "group reporting", "new")
    t, t_foo, t_bar, x = add_test_data(scen)

    # Reporter
    rep = Reporter.from_scenario(scen)

    # Define some groups
    t_groups = {"foo": t_foo, "bar": t_bar, "baz": ["foo1", "bar5", "bar6"]}

    # Use the computation directly
    agg1 = computations.aggregate(Quantity(x), {"t": t_groups}, True)

    # Expected set of keys along the aggregated dimension
    assert set(agg1.coords["t"].values) == set(t) | set(t_groups.keys())

    # Sums are as expected
    assert_qty_allclose(agg1.sel(t="foo", drop=True), x.sel(t=t_foo).sum("t"))
    assert_qty_allclose(agg1.sel(t="bar", drop=True), x.sel(t=t_bar).sum("t"))
    assert_qty_allclose(
        agg1.sel(t="baz", drop=True), x.sel(t=["foo1", "bar5", "bar6"]).sum("t")
    )

    # Use Reporter convenience method
    key2 = rep.aggregate("x:t-y", "agg2", {"t": t_groups}, keep=True)

    # Group has expected key and contents
    assert key2 == "x:t-y:agg2"

    # Aggregate is computed without error
    agg2 = rep.get(key2)

    assert_qty_equal(agg1, agg2)

    # Add aggregates, without keeping originals
    key3 = rep.aggregate("x:t-y", "agg3", {"t": t_groups}, keep=False)

    # Distinct keys
    assert key3 != key2

    # Only the aggregated and no original keys along the aggregated dimension
    agg3 = rep.get(key3)
    assert set(agg3.coords["t"].values) == set(t_groups.keys())

    with pytest.raises(NotImplementedError):
        # Not yet supported; requires two separate operations
        rep.aggregate("x:t-y", "agg3", {"t": t_groups, "y": [2000, 2010]})


def test_filters(test_mp, tmp_path, caplog):
    """Reporting can be filtered ex ante."""
    scen = ixmp.Scenario(test_mp, "Reporting filters", "Reporting filters", "new")
    t, t_foo, t_bar, x = add_test_data(scen)

    rep = Reporter.from_scenario(scen)
    x_key = rep.full_key("x")

    def assert_t_indices(labels):
        assert set(rep.get(x_key).coords["t"].values) == set(labels)

    # 1. Set filters directly
    rep.graph["config"]["filters"] = {"t": t_foo}
    assert_t_indices(t_foo)

    # Reporter can be re-used by changing filters
    rep.graph["config"]["filters"] = {"t": t_bar}
    assert_t_indices(t_bar)

    rep.graph["config"]["filters"] = {}
    assert_t_indices(t)

    # 2. Set filters using a convenience method
    rep = Reporter.from_scenario(scen)
    rep.set_filters(t=t_foo)
    assert_t_indices(t_foo)

    # Clear filters using the convenience method
    rep.set_filters(t=None)
    assert_t_indices(t)

    # Clear using the convenience method with no args
    rep.set_filters(t=t_foo)
    assert_t_indices(t_foo)
    rep.set_filters()
    assert_t_indices(t)

    # 3. Set filters via configuration keys
    # NB passes through from_scenario() -> __init__() -> configure()
    rep = Reporter.from_scenario(scen, filters={"t": t_foo})
    assert_t_indices(t_foo)

    # Configuration key can also be read from file
    rep = Reporter.from_scenario(scen)

    # Write a temporary file containing the desired labels
    config_file = tmp_path / "config.yaml"
    config_file.write_text("\n".join(["filters:", f"  t: {repr(t_bar)}"]))

    rep.configure(config_file)
    assert_t_indices(t_bar)

    # Filtering too heavily:
    # Remove one value from the database at valid coordinates
    removed = {"t": t[:1], "y": list(x.coords["y"].values)[:1]}
    scen.remove_par("x", removed)

    # Set filters to retrieve only this coordinate
    rep.set_filters(**removed)

    # A warning is logged
    msg = "\n  ".join(
        [
            "0 values for par 'x' using filters:",
            repr(removed),
            "Subsequent computations may fail.",
        ]
    )
    with assert_logs(caplog, msg):
        rep.get(x_key)
