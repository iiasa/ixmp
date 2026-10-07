"""Tests of :class:`.JDBCBackend` connected to a PostgreSQL database.

These use the PostgreSQL server given by :program:`pytest --ixmp-postgres=…`, the same
server as for tests of :class:`.IXMP4Backend`. They are skipped if no such server is
available. Each test uses a new, empty database that is dropped afterwards.
"""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp
from ixmp.testing import DATA, KEY_ENGINE, make_dantzig

if TYPE_CHECKING:
    from sqlalchemy import Engine

pytestmark = pytest.mark.jdbc


def assert_par_equal(expected: Any, actual: Any) -> None:
    """Assert that parameter data are equal, ignoring the order of rows."""
    assert isinstance(expected, pd.DataFrame) and isinstance(actual, pd.DataFrame)
    pdt.assert_frame_equal(
        expected.sort_values(["i", "j"], ignore_index=True),
        actual.sort_values(["i", "j"], ignore_index=True),
    )


@pytest.fixture(scope="module")
def engine(pytestconfig: pytest.Config) -> "Engine":
    """A :class:`sqlalchemy.Engine` connected to the PostgreSQL server for testing."""
    engine = pytestconfig.stash.get(KEY_ENGINE, None)
    if engine is None:  # pragma: no cover
        pytest.skip(reason="No PostgreSQL database available")
    return engine


@pytest.fixture
def pg_kwargs(engine: "Engine", worker_id: str) -> Generator[dict[str, Any], Any, None]:
    """Keyword arguments for :class:`.Platform` connecting to an empty database."""
    from sqlalchemy import text

    url = engine.url
    # Bracket an IPv6 address
    host = f"[{url.host}]" if ":" in (url.host or "") else url.host
    name = f"ixmp_test_jdbc_{worker_id}"

    with engine.connect() as connection:
        connection.execute(text(f"DROP DATABASE IF EXISTS {name}"))
        connection.execute(text(f"CREATE DATABASE {name}"))

    try:
        yield dict(
            backend="jdbc",
            driver="postgresql",
            url=f"{host}:{url.port or 5432}/{name}",
            user=url.username or "",
            password=url.password or "",
        )
    finally:
        with engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)"))


@pytest.fixture
def mp(pg_kwargs: dict[str, Any]) -> Generator[ixmp.Platform, Any, None]:
    """A Platform connected to an empty PostgreSQL database."""
    mp = ixmp.Platform(**pg_kwargs)
    try:
        yield mp
    finally:
        mp.close_db()


def test_connect(
    mp: ixmp.Platform, engine: "Engine", pg_kwargs: dict[str, Any]
) -> None:
    """JDBCBackend connects to an empty database and creates its schema."""
    from sqlalchemy import create_engine, text

    # Connection works, and the database is empty
    assert [] == list(mp.scenario_list(default=False).index)

    # ixmp_source has created tables in the database
    url = engine.url.set(database=pg_kwargs["url"].rpartition("/")[2])
    db_engine = create_engine(url)
    try:
        with db_engine.connect() as connection:
            n = connection.execute(
                text(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                )
            ).scalar_one()
    finally:
        db_engine.dispose()
    assert 0 < n


def test_data(mp: ixmp.Platform, pg_kwargs: dict[str, Any]) -> None:
    """Sets and parameters can be added, committed, and read back."""
    scen = ixmp.Scenario(mp, "model name", "scenario name", version="new")
    scen.init_set("i")
    scen.add_set("i", ["seattle", "san-diego"])
    scen.init_set("j")
    scen.add_set("j", ["new-york", "chicago"])
    scen.init_par("d", idx_sets=["i", "j"])
    d = pd.DataFrame(
        [
            ["seattle", "new-york", 2.5, "km"],
            ["seattle", "chicago", 1.7, "km"],
            ["san-diego", "new-york", 2.5, "km"],
        ],
        columns=["i", "j", "value", "unit"],
    )
    scen.add_par("d", d)
    scen.init_scalar("f", 90.0, "USD/km")
    scen.commit("Add data")
    scen.set_as_default()

    # Data are unchanged when read from the same scenario
    assert {"seattle", "san-diego"} == set(scen.set("i"))
    assert_par_equal(d, scen.par("d"))

    # Disconnect and reconnect: the data were stored in PostgreSQL, not only in memory
    del scen
    mp.close_db()
    mp2 = ixmp.Platform(**pg_kwargs)
    try:
        scen2 = ixmp.Scenario(mp2, "model name", "scenario name")
        assert {"new-york", "chicago"} == set(scen2.set("j"))
        assert_par_equal(d, scen2.par("d"))
        assert 90.0 == scen2.scalar("f")["value"]
    finally:
        mp2.close_db()


def test_clone(mp: ixmp.Platform) -> None:
    """A scenario can be cloned; the clone is independent of the original."""
    scen = make_dantzig(mp)

    # Clone to a different scenario name
    clone = scen.clone(scenario="clone", keep_solution=False)
    assert clone.scenario == "clone"
    for name in ("i", "j"):
        assert set(scen.set(name)) == set(clone.set(name))
    assert_par_equal(scen.par("d"), clone.par("d"))

    # Modify the clone only
    clone.check_out()
    clone.add_set("i", "new-i")
    clone.commit("Add an element")
    assert "new-i" in set(clone.set("i")) and "new-i" not in set(scen.set("i"))

    # Both are listed
    assert {scen.scenario, "clone"} == set(
        mp.scenario_list(model=scen.model, default=False)["scenario"]
    )


def test_timeseries(mp: ixmp.Platform) -> None:
    """Time series data, including ±∞, can be added, committed, and read back."""
    ts = ixmp.TimeSeries(mp, "model name", "scenario name", version="new")
    data = DATA[0].assign(value=[np.inf, -np.inf])
    ts.add_timeseries(data)
    ts.commit("Add time series data")

    ts = ixmp.TimeSeries(mp, "model name", "scenario name")
    result = ts.timeseries().sort_values("year", ignore_index=True)

    pdt.assert_frame_equal(
        data.reindex(columns=result.columns).sort_values("year", ignore_index=True),
        result,
        check_dtype=False,
    )


@pytest.mark.xfail(
    raises=RuntimeError, reason="SQL error in ixmp_source with PostgreSQL ≥ 15"
)
def test_geodata(mp: ixmp.Platform) -> None:
    """Geodata can be added and committed."""
    ts = ixmp.TimeSeries(mp, "model name", "scenario name", version="new")
    ts.add_geodata(DATA["geo"])
    ts.commit("Add geodata")
