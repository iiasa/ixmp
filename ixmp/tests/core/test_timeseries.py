import logging
import re
from datetime import datetime, timedelta

import pandas as pd
import pytest
from numpy import testing as npt
from pandas.testing import assert_frame_equal

from ixmp import IAMC_IDX, Scenario, TimeSeries
from ixmp.testing import DATA, models

# string columns for timeseries checks
IDX_COLS = ["region", "variable", "unit", "year"]

COLS_FOR_YEARLY_DATA = ["model", "scenario"] + IDX_COLS + ["value"]
COLS_WITH_SUBANNUAL = COLS_FOR_YEARLY_DATA.copy()
COLS_WITH_SUBANNUAL.insert(4, "subannual")


# Utility methods
def expected(df, ts):
    """Modify *df* with the 'model' and 'scenario' name from *ts."""
    return df.assign(model=ts.model, scenario=ts.scenario)


def wide(df):
    """Transform *df* from long to wide format."""
    other_cols = [c for c in df.columns if c not in ["year", "value"] + IAMC_IDX]
    return (
        df.pivot_table(index=IAMC_IDX + other_cols, columns="year", values="value")
        .reset_index()
        .rename_axis(columns=None)
    )


def assert_geodata(obs, exp):
    obs = obs.sort_values("year")
    exp = exp.sort_values("year")
    for column in obs.columns:
        npt.assert_array_equal(exp.get(column), obs.get(column))


def assert_timeseries(scen, exp=DATA["timeseries"], cols=None, subannual=None):
    """Asserts scenario timeseries are similar to expected

    Compares region, variable, unit, year and subannual (if available).
    By default it assumes that datasets are sorted in correct order to compare.

    :param scen:    scenario object
    :param exp:     expected timeseries data
    :param cols:    (optional) column list to sort by
    :param subannual:   passed to 'timeseries()'
    """
    if subannual is None:
        obs = scen.timeseries(region="World")
    else:
        obs = scen.timeseries(region="World", subannual=subannual)

    if cols is not None:
        obs = obs.sort_values(by=cols)
        exp = exp.sort_values(by=cols)
    npt.assert_array_equal(exp[IDX_COLS], obs[IDX_COLS])
    npt.assert_array_almost_equal(exp["value"], obs["value"])
    if "subannual" in exp.columns:
        npt.assert_array_equal(exp[["subannual"]], obs[["subannual"]])


# Fixtures


class TestTimeSeries:
    """Tests of :class:`.TimeSeries`.

    Since :class:`.Scenario` is a subclass of TimeSeries, all TimeSeries functionality
    should work exactly the same way on Scenario instances. The :meth:`ts` fixture is
    parametrized to yield both TimeSeries and Scenario objects, so every test is run on
    each type.
    """

    @pytest.fixture(scope="function", params=[TimeSeries, Scenario])
    def ts(self, request, mp):
        """An empty TimeSeries with a temporary name on the :func:`mp`."""
        # Use a hash of the pytest node ID to avoid exceeding the maximum
        # length for a scenario name
        node = hash(request.node.nodeid.replace("/", " "))
        # Class of object to yield
        cls = request.param
        yield cls(mp, model=f"test-{node}", scenario=f"test-{node}", version="new")

    # Initialize TimeSeries
    @pytest.mark.parametrize("cls", [TimeSeries, Scenario])
    def test_init(self, test_mp, cls):
        # Something other than a Platform as mp argument
        with pytest.raises(TypeError):
            cls(None, "model name", "scenario name")

        # Invalid version argument
        with pytest.raises(ValueError):
            cls(test_mp, "model name", "scenario name", version=3.4)

    def test_init2(self, test_mp):
        # Scheme argument
        with pytest.raises(TypeError, match="'scheme' argument"):
            TimeSeries(test_mp, "m", "s", scheme="scheme")

    # TimeSeries methods

    def test_has_solution(self, ts):
        """:meth:`.TimeSeries.has_solution` is always :obj:`False`."""
        assert False is ts.has_solution()

    def test_default(self, mp, ts):
        # NB this is required before the is_default method can be used
        # FIXME should return False regardless
        ts.commit("")

        # Temporary TimeSeries is has not been set_as_default
        assert not ts.is_default()

        ts.set_as_default()
        assert ts.is_default()

        # NB TimeSeries cannot be cloned, so create a new one with the same
        #    name
        ts2 = TimeSeries(mp, ts.model, ts.scenario, version="new")
        ts2.commit("")
        ts2.set_as_default()

        assert ts2.is_default()
        # Original TimeSeries is no longer default
        assert not ts.is_default()

    # NOTE IXMP4Backend doesn't handle commits yet, so run_id is 1 immediately
    @pytest.mark.jdbc
    def test_run_id(self, ts):
        # New, un-committed TimeSeries has run_id of -1
        assert ts.run_id() == -1

        # The run ID is a positive integer
        ts.commit("")
        assert ts.run_id() > 0 and isinstance(ts.run_id(), int)

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_last_update(self, ts):
        # New, un-committed TimeSeries has no last update date
        assert ts.last_update() is None

        ts.commit("")

        # After committing, last_update() returns a string
        last_update = ts.last_update()
        actual = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S.%f")
        assert abs(actual - datetime.now()) < timedelta(seconds=1)

    @pytest.mark.parametrize("format", ["long", "wide"])
    def test_add_timeseries(self, ts, format):
        data = DATA[0] if format == "long" else wide(DATA[0])

        # Add data
        ts.add_timeseries(data)
        ts.commit("")

        # Error: column 'unit' is missing
        with pytest.raises(ValueError):
            ts.add_timeseries(DATA[0].drop("unit", axis=1))

    # NOTE Not yet implemented on IXMP4Backend properly. Doing so required optimization
    # items in ixmp4 using versioning/changelog correctly.
    @pytest.mark.jdbc
    def test_discard_changes(self, ts):
        ts.commit("")
        assert 0 == len(ts.timeseries())

        with ts.transact():
            # Add data, but discard before commit
            ts.add_timeseries(DATA[0])
            ts.discard_changes()

        assert 0 == len(ts.timeseries())

    # FIXME IXMP4Backend seems to handle timeseries() incorrectly; the last call returns
    # 0 rows
    @pytest.mark.jdbc
    @pytest.mark.parametrize("format", ["long", "wide"])
    def test_get(self, ts, format):
        data = DATA[0] if format == "long" else wide(DATA[0])

        ts.add_timeseries(data)
        ts.commit("")

        exp = expected(data, ts)
        args = {}

        if format == "wide":
            args["iamc"] = True

        # Data can be retrieved and has the expected value
        # NB this included check_like=True to be tolerant of JDBCBackend returning
        #    columns in unpredictable order. In pandas 1.2.0, this caused an exception;
        #    see pandas-dev/pandas#39168. Removed until the upstream bug is fixed.
        assert_frame_equal(exp, ts.timeseries(**args))

    @pytest.mark.parametrize(
        "year_arg",
        [
            [2020],  # Single element
            [2010, 2020],  # Multiple elements
            2020,  # bare int, not in a list
            # java.lang.java.lang.ClassCastException
            pytest.param(["2010"], marks=pytest.mark.xfail),
        ],
    )
    def test_get_year(self, ts, year_arg):
        """`year` arg to :meth:`.TimeSeries.timeseries` accepts only :class:`int`."""
        ts.add_timeseries(DATA[0])
        ts.commit("")

        # year filters with integer values are handled correctly (iiasa/ixmp#440)
        ts.timeseries(year=year_arg)

    # FIXME IXMP4Backend seems to handle timeseries() incorrectly; the last call returns
    # 0 rows
    @pytest.mark.jdbc
    @pytest.mark.parametrize("format", ["long", "wide"])
    def test_edit(self, mp, ts, format):
        """Tests that data can be overwritten."""
        data = expected(DATA[0], ts)
        all_data = [data.loc[0:0, :]]
        args = {}

        if format == "wide":
            data = wide(data)
            args["iamc"] = True

        ts.add_timeseries(data)
        ts.commit("initial data")

        # Overwrite existing data
        with ts.transact(message="overwrite existing data"):
            ts.add_timeseries(data)

        df = expected(DATA[2030], ts)
        all_data.append(df)
        if format == "wide":
            df = wide(df)

        # Overwrite and add new values at once
        with ts.transact(message="overwrite and add data"):
            ts.add_timeseries(df)

        # Close and re-open database
        mp.close_db()
        mp.open_db()

        # All four rows are retrieved
        exp = pd.concat(all_data).reset_index(drop=True)
        if format == "wide":
            exp = wide(exp)
        assert_frame_equal(exp, ts.timeseries(**args))

    # NOTE IXMP4Backend requires version-specifier or setting a default for the
    # corresponding Run since Run doesn't store `version`
    @pytest.mark.jdbc
    @pytest.mark.parametrize("cls", [TimeSeries, Scenario])
    def test_edit_with_region_synonyms(self, mp, ts, cls):
        info = dict(model=ts.model, scenario=ts.scenario)

        mp.add_region_synonym("Hell", "World")

        ts.add_timeseries(DATA[0])
        ts.commit("updating timeseries in IAMC format")

        ts = cls(mp, **info)
        assert_frame_equal(expected(DATA[0], ts), ts.timeseries())

        ts.check_out(timeseries_only=True)
        df = wide(DATA[2050]).replace("World", "Hell")
        ts.preload_timeseries()
        ts.add_timeseries(df)
        ts.commit("updating timeseries in IAMC format")

        assert_frame_equal(expected(DATA[2050], ts), ts.timeseries())

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    # TODO parametrize format as wide/long
    @pytest.mark.parametrize(
        "commit",
        [
            pytest.param(True),
            pytest.param(
                False,
                marks=pytest.mark.xfail(
                    reason="TimeSeries must be checked in to retrieve data."
                ),
            ),
        ],
    )
    def test_remove(self, mp, ts, commit):
        df = expected(DATA[2050], ts)

        ts.add_timeseries(DATA[2050])
        ts.commit("")

        if not commit:
            ts.check_out()

        assert_frame_equal(df, ts.timeseries())

        # Remove a single data point
        with ts.transact(condition=commit):
            ts.remove_timeseries(df[df.year == 2010])

        # Expected data remains
        exp = df[df.year != 2010].reset_index(drop=True)
        assert_frame_equal(exp, ts.timeseries())

        # Remove two data points
        with ts.transact(condition=commit):
            ts.remove_timeseries(df[df.year.isin([2030, 2050])])

        # Expected data remains
        exp = df[~df.year.isin([2010, 2030, 2050])].reset_index(drop=True)
        assert_frame_equal(exp, ts.timeseries())

        # Remove all remaining data
        with ts.transact(condition=commit):
            ts.remove_timeseries(df)

        # Result is empty
        assert ts.timeseries().empty

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_transact_discard(self, caplog, mp, ts):
        caplog.set_level(logging.INFO, "ixmp.util")

        df = expected(DATA[2050], ts)

        ts.add_timeseries(DATA[2050])
        ts.commit("")

        # Catch the deliberately-raised exception so that the test passes
        with pytest.raises(AttributeError):
            with ts.transact(discard_on_error=True):
                # Remove a single data point; this operation will not be committed
                ts.remove_timeseries(df[df.year == 2010])

                # Trigger AttributeError
                ts.foo_bar()

        # Reopen the database connection
        mp.open_db()

        # Exception was caught and logged
        assert caplog.messages[-4].startswith("Avoid locking ")
        assert re.match("Discard (timeseries|scenario) changes", caplog.messages[-3])
        assert "Close database connection" == caplog.messages[-2]

        # Data are unchanged
        assert_frame_equal(expected(DATA[2050], ts), ts.timeseries())

    # Geodata

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_add_geodata(self, ts):
        # Empty TimeSeries includes no geodata
        assert_frame_equal(DATA["geo"].loc[[False, False, False]], ts.get_geodata())

        # Data can be added
        ts.add_geodata(DATA["geo"])
        ts.commit("")

        # Added data can be retrieved
        obs = ts.get_geodata().sort_values("year").reset_index(drop=True)
        assert_frame_equal(DATA["geo"], obs)

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    @pytest.mark.parametrize(
        "rows", [(1,), (1, 2), (0, 1, 2)], ids=["single", "multiple", "all"]
    )
    def test_remove_geodata(self, ts, rows):
        ts.add_geodata(DATA["geo"])
        ts.remove_geodata(DATA["geo"].take(rows))
        ts.commit("")

        mask = [i not in rows for i in range(len(DATA["geo"]))]
        # Expected rows have been removed
        exp = DATA["geo"].iloc[mask].reset_index(drop=True)
        obs = ts.get_geodata().sort_values("year").reset_index(drop=True)
        assert_frame_equal(exp, obs)

    # NOTE remove_timeseries() not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    @pytest.mark.parametrize("format", ["long", "wide"])
    @pytest.mark.parametrize(
        "N",
        (
            256,
            # This fails at commit(), not at add_timeseries()
            pytest.param(
                257,
                marks=pytest.mark.xfail(raises=RuntimeError),
            ),
        ),
    )
    def test_long_variable_name(self, ts, format, N):
        """Variable names up to 256 characters can be added or removed."""
        data = (DATA[0] if format == "long" else wide(DATA[0])).copy()

        # Use long variable name, max 256 characters
        data.variable = "x" * N

        ts.add_timeseries(data)
        ts.commit("")

        data = ts.timeseries()

        with ts.transact():
            ts.remove_timeseries(data)

    # Metadata

    def test_set_meta(self, ts):
        # Raises TypeError when first argument is not str or dict
        with pytest.raises(TypeError):
            ts.set_meta(["foo", "bar"])

    # FIXME all tests below this line need cleanup

    @pytest.mark.parametrize("format", ["long", "wide"])
    def test_add_timeseries_with_extra_col(self, caplog, ts, format):
        _data = DATA[0].copy()
        _data["climate_model"] = [0, 1]
        data = _data if format == "long" else wide(_data)

        # check that extra column wasn't dropped by `wide(_data)
        assert "climate_model" in data.columns

        # Data added
        ts.add_timeseries(data)
        # TODO: add check that warning message is displayed
        ts.commit("")
        assert "Dropped extra column(s) ['climate_model'] from data" in caplog.messages

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_new_timeseries_as_iamc(self, test_mp):
        # TODO rescue use of subannual= here

        scen = TimeSeries(test_mp, *models["h2g2"], version="new", annotation="fo")
        scen.add_timeseries(
            DATA["timeseries"].pivot_table(values="value", index=IDX_COLS)
        )
        scen.commit("importing a testing timeseries")

        # compare returned dataframe - default behaviour set to 'auto'
        assert_timeseries(scen)
        # test behaviour of 'auto' explicitly
        assert_timeseries(scen, subannual="auto")
        # test behaviour of 'False' explicitly
        assert_timeseries(scen, subannual=False)

        # test behaviour of 'True' explicitly
        exp = (
            DATA["timeseries"].pivot_table(values="value", index=IDX_COLS).reset_index()
        )
        exp["model"] = "Douglas Adams"
        exp["scenario"] = "Hitchhiker"
        exp["subannual"] = "Year"
        assert_timeseries(scen, exp=exp, cols=COLS_WITH_SUBANNUAL, subannual=True)

    def test_new_timeseries_error(self, test_mp):
        scen = TimeSeries(test_mp, *models["h2g2"], version="new", annotation="fo")
        df = {"year": [2010, 2020], "value": [23.5, 23.6]}
        df = pd.DataFrame.from_dict(df)
        df["region"] = "World"
        df["variable"] = "Testing"
        # column `unit` is missing
        pytest.raises(ValueError, scen.add_timeseries, df)

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_new_subannual_timeseries_as_iamc(self, mp):
        mp.add_timeslice("Summer", "Season", 1.0 / 4)
        scen = TimeSeries(mp, *models["h2g2"], version="new", annotation="fo")
        timeseries = DATA["timeseries"].pivot_table(values="value", index=IDX_COLS)
        scen.add_timeseries(timeseries)
        scen.commit("adding yearly data")

        # add subannual timeseries data
        ts_summer = timeseries.copy()
        ts_summer["subannual"] = "Summer"
        scen.check_out()
        scen.add_timeseries(ts_summer)
        scen.commit("adding subannual data")

        # generate expected dataframe+
        ts_year = timeseries.copy()
        ts_year["subannual"] = "Year"
        exp = pd.concat([ts_year, ts_summer]).reset_index()
        exp["model"] = "Douglas Adams"
        exp["scenario"] = "Hitchhiker"

        # compare returned dataframe - default behaviour set to 'auto'
        assert_timeseries(scen, exp=exp[COLS_WITH_SUBANNUAL], cols=COLS_WITH_SUBANNUAL)
        # test behaviour of 'auto' explicitly
        assert_timeseries(
            scen,
            exp=exp[COLS_WITH_SUBANNUAL],
            cols=COLS_WITH_SUBANNUAL,
            subannual="auto",
        )
        # test behaviour of 'True' explicitly
        assert_timeseries(
            scen, exp=exp[COLS_WITH_SUBANNUAL], cols=COLS_WITH_SUBANNUAL, subannual=True
        )
        # setting False raises an error because subannual data exists
        pytest.raises(ValueError, scen.timeseries, subannual=False)

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_fetch_empty_geodata(self, mp):
        scen = TimeSeries(mp, *models["h2g2"], version="new", annotation="fo")
        empty = scen.get_geodata()
        assert_geodata(empty, DATA["geo"].loc[[False, False, False]])

    # NOTE Not yet implemented on IXMP4Backend
    @pytest.mark.jdbc
    def test_remove_multiple_geodata(self, mp):
        scen = TimeSeries(mp, *models["h2g2"], version="new", annotation="fo")
        scen.add_geodata(DATA["geo"])
        row = DATA["geo"].loc[[False, True, True]]
        scen.remove_geodata(row)
        scen.commit("adding geodata (references to map layers)")
        assert_geodata(scen.get_geodata(), DATA["geo"].loc[[True, False, False]])
