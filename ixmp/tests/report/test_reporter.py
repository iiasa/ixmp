import logging
import re

import pint
import pytest
from genno import ComputationError, configure

import ixmp
from ixmp.report.reporter import Reporter
from ixmp.testing import add_test_data, assert_logs, make_dantzig

pytestmark = pytest.mark.usefixtures("parametrize_quantity_class")

test_args = ("Douglas Adams", "Hitchhiker")


@pytest.fixture
def scenario(test_mp):
    # from test_feature_timeseries.test_new_timeseries_as_year_value
    scen = ixmp.Scenario(test_mp, *test_args, version="new", annotation="foo")
    scen.commit("importing a testing timeseries")
    yield scen


@pytest.mark.usefixtures("protect_rename_dims")
def test_configure(test_mp, test_data_path, request) -> None:
    # Configure globally; handles 'rename_dims' section
    configure(rename_dims={"i": "i_renamed"})

    # Test direct import
    from ixmp.report import RENAME_DIMS

    assert "i" in RENAME_DIMS

    # Reporting uses the RENAME_DIMS mapping of 'i' to 'i_renamed'
    scen = make_dantzig(test_mp, request=request)
    rep = Reporter.from_scenario(scen)
    assert "d:i_renamed-j" in rep, rep.graph.keys()
    assert ["seattle", "san-diego"] == rep.get("i_renamed")

    # Original name 'i' are not found in the reporter
    assert "d:i-j" not in rep, rep.graph.keys()
    pytest.raises(KeyError, rep.get, "i")


def test_reporter_from_scenario(scenario) -> None:
    r = Reporter.from_scenario(scenario)

    r.finalize(scenario)

    assert "scenario" in r.graph


def test_platform_units(test_mp, caplog, ureg) -> None:
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
    msg = r"unit '{}' cannot be parsed; contains invalid character\(s\) '{}+'"

    # Unit and components for the regex
    bad_units = [
        ("-", "-", "-"),
        ("???", r"\?\?\?", r"\?"),
        # Disabled pending https://github.com/hgrecco/pint/issues/1766
        # ("E$", r"E\$", r"\$"),
    ]
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

    # Protect from --verbose command-line option, which sets the level to DEBUG
    with caplog.at_level(logging.INFO):
        rep.get(x_key)

    # NB cannot use assert_logs here. report.util.parse_units uses the pint
    #    application registry, so depending which tests are run and in which order, this
    #    unit may already be defined.
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
        caplog, ["x: mixed units", "kg", "USD/pkm", "discarded"], at_level=logging.INFO
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


def test_cli(ixmp_cli, test_mp, test_data_path, request) -> None:
    # Put something in the database
    test_mp.open_db()
    make_dantzig(test_mp, request=request)
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
        f"{request.node.name}",
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
    assert re.match(
        "i          j       "  # Trailing whitespace
        r"""
san-diego  chicago     1\.8
           new-york    2\.5
           topeka      1\.4
seattle    chicago     1\.7
           new-york    2\.5
           topeka      1\.8
(Name: value, )?dtype: float64(, units: dimensionless)?""",
        result.output,
    ), result.output


def test_filters(test_mp, tmp_path, caplog) -> None:
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
    with assert_logs(
        caplog,
        (
            f"0 values for par 'x' using filters: {repr(removed)}",
            "May be the cause of subsequent errors",
        ),
        at_level=logging.DEBUG,
    ):
        rep.get(x_key)
