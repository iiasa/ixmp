import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp import Platform, Scenario, TimeSeries, config as ixmp_config
from ixmp.testing import make_dantzig
from ixmp.tests.core.test_timeseries import DATA, expected, wide


@pytest.fixture(scope="class")
def mp(request, tmp_env, tmp_path_factory):
    """An empty ixmp.Platform connected to a temporary, in-memory database.

    This is a lightly-modified clone of ixmp.testing.test_mp fixture.
    """
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace("/", " ")

    # Add a platform
    # ixmp_config.add_platform(platform_name, "dbapi", ":memory:")
    ixmp_config.add_platform(
        platform_name, "dbapi", tmp_path_factory.mktemp(platform_name) / "test.db"
    )

    # Launch Platform
    mp = Platform(name=platform_name)
    yield mp

    # Remove from config
    ixmp_config.remove_platform(platform_name)


@pytest.fixture(scope="function", params=[TimeSeries, Scenario])
def ts(request, mp):
    """Copied from :func:`core.test_timeseries.ts`."""
    # Use a hash of the pytest node ID to avoid exceeding the maximum length for a
    # scenario name
    node = hash(request.node.nodeid.replace("/", " "))
    # Class of object to yield
    cls = request.param
    yield cls(mp, model=f"test-{node}", scenario="test", version="new")


class TestDatabaseBackend:
    def test_init_backend(self, mp):
        """A Platform backed by DatabaseBackend can be initialized."""

    @pytest.mark.parametrize("kind", ("model", "scenario"))
    def test_names(self, mp, kind):
        setter = getattr(mp, f"add_{kind}_name")
        getter = getattr(mp, f"get_{kind}_names")

        items = ("foo", "bar", "baz")
        for i in items:
            setter(i)

        assert items == tuple(getter())

    def test_node(self, mp):
        """Nodes can be stored and retrieved."""
        mp.add_region("AT", "country")
        mp.regions()

    def test_unit(self, mp):
        """Units can be stored and retrieved."""
        units = ["kg", "km"]
        for unit in units:
            mp.add_unit(unit)

        assert units == mp.units()

        # NB only works when using a file-backed `mp` fixture. With :memory:, nothing
        # is persisted, so this doesn't work.
        mp.close_db()
        mp.open_db()

        assert units == mp.units()

    def test_timeslice(self, mp):
        """Time slices can be stored and retrieved."""
        items = (
            dict(name="Spring", category="season", duration=1.0 / 9),
            dict(name="Summer", category="season", duration=2.0 / 9),
            dict(name="Autumn", category="season", duration=4.0 / 9),
            dict(name="Winter", category="season", duration=2.0 / 9),
        )
        for item in items:
            mp.add_timeslice(**item)

        pdt.assert_frame_equal(pd.DataFrame(items), mp.timeslices())

    def test_ts(self, mp):
        """Test Backend.{init,set_as_default,is_default}."""
        args = dict(model="Foo model", scenario="Baz scenario", version="new")
        ts0 = TimeSeries(mp, **args)
        assert 1 == ts0.version

        ts1 = TimeSeries(mp, **args)
        assert 2 == ts1.version

        ts1.set_as_default()
        assert ts1.is_default()

        assert not ts0.is_default()

        del ts0, ts1

        args.pop("version")
        ts2 = TimeSeries(mp, **args)
        assert 2 == ts2.version
        assert ts2.is_default()

    @pytest.mark.parametrize("fmt", ["long", "wide"])
    def test_tsdata(self, ts, fmt):
        # Copied from core.test_timeseries.test_add_timeseries
        data = DATA[0] if fmt == "long" else wide(DATA[0])

        # Data added
        ts.add_timeseries(data)
        ts.commit("")

        # Error: column 'unit' is missing
        with pytest.raises(ValueError):
            ts.add_timeseries(DATA[0].drop("unit", axis=1))

        # Copied from core.test_timeseries.test_get
        exp = expected(data, ts)
        args = {}

        if fmt == "wide":
            args["iamc"] = True

        # Data can be retrieved and has the expected value
        obs = ts.timeseries(**args)

        pdt.assert_frame_equal(exp, obs)

    def test_geodata(self, ts):
        # Copied from core.test_timeseries.test_add_geodata

        # Empty TimeSeries includes no geodata
        pdt.assert_frame_equal(DATA["geo"].loc[[False, False, False]], ts.get_geodata())

        # Data can be added
        ts.add_geodata(DATA["geo"])
        ts.commit("")

        # Added data can be retrieved
        obs = ts.get_geodata().sort_values("year").reset_index(drop=True)
        pdt.assert_frame_equal(DATA["geo"], obs)

    @pytest.mark.parametrize(
        "solve",
        (
            False,
            pytest.param(
                True,
                marks=pytest.mark.xfail(
                    raises=NotImplementedError,
                    reason=".backend.io.s_write_gdx() is not implemented",
                ),
            ),
        ),
    )
    def test_make_dantzig(self, mp, solve):
        make_dantzig(mp, solve=solve)
