import pandas as pd
import pandas.testing as pdt
import pytest

from ixmp import Platform, TimeSeries, config as ixmp_config
from ixmp.testing import make_dantzig


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

    def test_make_dantzig(self, mp):
        make_dantzig(mp)
