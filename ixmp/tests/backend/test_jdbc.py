import gc
import logging
import os
import platform
from sys import getrefcount

import jpype
import numpy as np
import pytest
from pytest import raises

import ixmp
import ixmp.backend.jdbc
from ixmp.backend.jdbc import DRIVER_CLASS, java
from ixmp.testing import DATA, add_random_model_data, bool_param_id, make_dantzig
from ixmp.testing.resource import memory_usage

log = logging.getLogger(__name__)


@pytest.mark.flaky(
    reruns=5,
    rerun_delay=2,
    condition="GITHUB_ACTIONS" in os.environ and platform.system() == "Linux",
    reason="Flaky; see iiasa/ixmp#489",
)
def test_jvm_warn(recwarn):
    """Test that no warnings are issued on JVM start-up.

    A warning message is emitted e.g. for JPype 0.7 if the 'convertStrings'
    kwarg is not provided to jpype.startJVM.

    NB this function should be in test_core.py, but because pytest executes
    tests in file, then code order, it must be before the call to ix.Platform()
    below.
    """

    # Start the JVM for the first time in the test session
    from ixmp.backend.jdbc import start_jvm

    start_jvm()

    if jpype.__version__ > "0.7":
        # Zero warnings were recorded
        assert len(recwarn) == 0, recwarn.pop().message


def test_close_default_logging(test_mp_f, capfd):
    """Platform.close_db() doesn't throw needless exceptions."""
    # Use the session-scoped fixture to avoid affecting other tests in this file
    mp = test_mp_f

    # Close once
    mp.close_db()

    # Close again, once already closed
    mp.close_db()

    # With the default log level, WARNING, nothing is printed
    captured = capfd.readouterr()
    assert captured.out == ""


# NOTE IXMP4Backend's close_db() is a noop
@pytest.mark.jdbc
def test_close_increased_logging(test_mp_f, capfd):
    """Platform.close_db() doesn't throw needless exceptions."""
    # Use the session-scoped fixture to avoid affecting other tests in this file
    mp = test_mp_f

    # Close once
    mp.close_db()

    # Set higher log level INFO
    level = mp.get_log_level()
    mp.set_log_level(logging.INFO)

    # Close again, once already closed
    # With logging.INFO, a message is printed
    mp.close_db()
    captured = capfd.readouterr()
    msg = "Database connection could not be closed or was already closed"
    try:
        assert msg in captured.out
    finally:
        mp.set_log_level(level)


VE = pytest.mark.xfail(raises=ValueError)


class TestJDBCBackend:
    @pytest.fixture(scope="class")
    def klass(self):
        """The JDBCBackend class."""
        yield ixmp.backend.jdbc.JDBCBackend

    @pytest.fixture(scope="function")
    def mp(self):
        """A Platform connected to a JDBCBackend."""
        yield ixmp.Platform(
            backend="jdbc", driver="hsqldb", url="jdbc:hsqldb:mem://ixmptest"
        )

    @pytest.fixture()
    def be(self, mp):
        """The Backend object itself."""
        yield mp._backend

    @pytest.mark.parametrize(
        "args, kwargs, expected",
        (
            # Advertised forms
            (
                ("oracle", "url", "user", "pass"),
                dict(),
                dict(driver="oracle", url="url", user="user", password="pass"),
            ),
            (("hsqldb",), dict(url="url"), dict(driver="hsqldb", url="url")),
            # Invalid
            pytest.param(tuple(), dict(), None, marks=VE),
            pytest.param(("not a driver",), dict(), None, marks=VE),
            pytest.param(
                ("oracle", "url", "u", "p", "-Xmx12G", "extra?!"),
                dict(),
                None,
                marks=VE,
            ),
            pytest.param(
                ("hsqldb", "path", "-Xmx12G", "extra?!"), dict(), None, marks=VE
            ),
            pytest.param(("oracle", "url", "missing pass"), dict(), None, marks=VE),
            pytest.param(("hsqldb",), dict(), None, marks=VE),
        ),
    )
    def test_handle_config(self, klass, args, kwargs, expected):
        """Test :meth:`JDBCBackend.handle_config`."""
        assert expected == klass.handle_config(args, kwargs)

    def test_handle_config_path(self, tmp_path, klass):
        """Test :meth:`JDBCBackend.handle_config` for HyperSQL paths.

        This is separate from :func:`test_handle_config` because the
        :class:`~pathlib.Path` object stored/returned varies across platforms.
        """
        args = ("hsqldb", str(tmp_path))
        kwargs = dict()

        assert dict(driver="hsqldb", path=tmp_path) == klass.handle_config(args, kwargs)

    @pytest.mark.parametrize(
        "name, idx_sets",
        (
            ("C", ["node", "year"]),
            ("COST_NODAL", ["node", "year"]),
            ("COST_NODAL_NET", ["node", "year"]),
            ("DEMAND", ["node", "commodity", "level", "year", "time"]),
            ("GDP", ["node", "year"]),
            ("I", ["node", "year"]),
            # Expected exception class raised on invalid arguments
            pytest.param(
                "C",
                ["node", "year", "technology"],
                marks=pytest.mark.xfail(raises=NotImplementedError),
            ),
        ),
    )
    def test_init_item(self, mp, be, name, idx_sets):
        """init_item() supports items that have fixed indices in ixmp_source."""

        # Create an ixmp.Scenario, coerce its scheme to MESSAGE, and then call
        # Backend.init(). This creates a Java MsgScenario object by circumventing
        # safeguards in Scenario.__init__().
        s = ixmp.Scenario(mp, "model name", "scenario name", version="new")
        s.scheme = "MESSAGE"
        be.init(s, "")

        # Initialize the item; succeeds
        be.init_item(s, type="var", name=name, idx_sets=idx_sets, idx_names=idx_sets)

    def test_delete_item(self, mp, be):
        s = ixmp.Scenario(mp, "model name", "scenario name", version="new")
        with pytest.raises(KeyError):
            be.delete_item(s, "set", "foo")

    def test_set_data_inf(self, mp):
        """:meth:`JDBCBackend.set_data` errors on :data:`numpy.inf` values."""
        # Make `mp` think it is connected to an Oracle database
        mp._backend._properties["jdbc.driver"] = DRIVER_CLASS["oracle"]

        # TimeSeries object and data to add
        ts = ixmp.TimeSeries(mp, "model name", "scenario name", version="new")
        data = DATA[0].assign(value=[np.inf, -np.inf])

        with pytest.raises(
            ValueError, match=r"infinity \(at region=World, variable=Testing\)"
        ):
            ts.add_timeseries(data)  # Calls JDBCBackend.set_data

    def test_set_unit(self, caplog, be):
        be.set_unit("", "comment")
        # No warning issued under pytest/driver=hsqldb; the exception only occurs with
        # driver=oracle
        assert [] == caplog.messages

    def test_read_file(self, tmp_path, be):
        """Cannot read CSV files."""
        with pytest.raises(NotImplementedError):
            be.read_file(tmp_path / "test.csv", ixmp.ItemType.ALL, filters={})

    def test_write_file(self, tmp_path, be):
        """Cannot write CSV files."""
        with pytest.raises(NotImplementedError):
            be.write_file(tmp_path / "test.csv", ixmp.ItemType.ALL, filters={})

    # Specific to JDBCBackend
    def test_gc(self, monkeypatch, be):
        monkeypatch.setattr(ixmp.backend.jdbc, "_GC_AGGRESSIVE", True)
        be.gc()


# TODO IXMP4Backend needs to handle change_scalar() correctly
@pytest.mark.jdbc
def test_exceptions(test_mp):
    """Ensure that Python exceptions are raised for some actions."""
    s = ixmp.Scenario(test_mp, "model name", "scenario name", "new")
    s.init_par("test_exception", [])
    s.commit("")

    with pytest.raises(RuntimeError):
        s.change_scalar("test_exception", 42, unit="kg")


def test_pass_properties():
    ixmp.Platform(
        driver="hsqldb", url="jdbc:hsqldb:mem://ixmptest", user="ixmp", password="ixmp"
    )


def test_invalid_properties_file(test_data_path):
    # HyperSQL creates a file with a .properties suffix for every file-based
    # database, but these files do not contain the information needed to
    # instantiate a database connection
    with pytest.raises(ValueError, match="Config file contains no database URL"):
        ixmp.Platform(dbprops=test_data_path / "hsqldb.properties")


def test_connect_message(capfd, caplog, request):
    msg = (
        f"connected to database 'jdbc:hsqldb:mem://{request.node.name}_0' "
        "(user: ixmp)..."
    )

    ixmp.Platform(
        backend="jdbc",
        driver="hsqldb",
        url=f"jdbc:hsqldb:mem://{request.node.name}_0",
        log_level="INFO",
    )

    # Java code via JPype does not log to the standard Python logger
    assert not any(msg in m for m in caplog.messages)

    # NB cannot inspect capfd here. Depending on the order in which the tests were run,
    #    a previous run may have left the Java log level higher than INFO, in which
    #    case the Java Platform object would not write to stderr before set_log_level()
    #    in the above call. Try again now that the level is INFO:
    msg = (
        f"connected to database 'jdbc:hsqldb:mem://{request.node.name}_1' "
        "(user: ixmp)..."
    )
    ixmp.Platform(
        backend="jdbc",
        driver="hsqldb",
        url=f"jdbc:hsqldb:mem://{request.node.name}_1",
    )

    # Instead, log messages are printed to stdout
    captured = capfd.readouterr()
    assert msg in captured.out


@pytest.mark.parametrize("arg", [True, False])
def test_cache_arg(arg, request):
    """Test 'cache' argument, passed to CachingBackend."""
    mp = ixmp.Platform(
        backend="jdbc",
        driver="hsqldb",
        url="jdbc:hsqldb:mem://test_cache_false",
        cache=arg,
    )
    scen = make_dantzig(mp, request=request)

    # Maybe put something in the cache
    scen.par("a")

    assert len(mp._backend._cache) == (1 if arg else 0)


# This variable formerly had 'warns' as the third element in some tuples, to
# test for deprecation warnings.
INIT_PARAMS: tuple[tuple, ...] = (
    # Handled in JDBCBackend:
    (
        ["nonexistent.properties"],
        dict(),
        raises,
        ValueError,
        "platform name " r"'nonexistent.properties' not among \['default'",
    ),
    (["nonexistent.properties"], dict(name="default"), raises, TypeError, None),
    # Using the dbtype keyword argument
    (
        [],
        dict(dbtype="HSQLDB"),
        raises,
        TypeError,
        r"JDBCBackend\(\) got an unexpected keyword argument 'dbtype'",
    ),
    # Initialize with driver='oracle' and path
    (
        [],
        dict(backend="jdbc", driver="oracle", path="foo/bar"),
        raises,
        ValueError,
        None,
    ),
    # …with driver='oracle' and no url
    ([], dict(backend="jdbc", driver="oracle"), raises, ValueError, None),
    # …with driver='hsqldb' and no path
    ([], dict(backend="jdbc", driver="hsqldb"), raises, ValueError, None),
    # …with driver='hsqldb' and url
    (
        [],
        dict(backend="jdbc", driver="hsqldb", url="example.com:1234:SCHEMA"),
        raises,
        ValueError,
        None,
    ),
)


@pytest.mark.parametrize("args,kwargs,action,kind,match", INIT_PARAMS)
def test_init(tmp_env, args, kwargs, action, kind, match):
    """Semantics for JDBCBackend.__init__()."""
    with action(kind, match=match):
        ixmp.Platform(*args, **kwargs)


def test_gh_216(test_mp, request):
    scen = make_dantzig(test_mp, request=request)

    filters = dict(i=["seattle", "beijing"])

    # Java code in ixmp_source would raise an exception because 'beijing' is
    # not in set i; but JDBCBackend removes 'beijing' from the filters before
    # calling the underlying method (https://github.com/iiasa/ixmp/issues/216)
    scen.par("a", filters=filters)


@pytest.fixture
def exception_verbose_true():
    """A fixture which ensures JDBCBackend raises verbose exceptions.

    The set value is not disturbed for other tests/code.
    """
    tmp = ixmp.backend.jdbc._EXCEPTION_VERBOSE  # Store current value
    ixmp.backend.jdbc._EXCEPTION_VERBOSE = True  # Ensure True
    yield
    ixmp.backend.jdbc._EXCEPTION_VERBOSE = tmp  # Restore value


# FIMXE This raises a RunNotFound on IXMP4Backend
@pytest.mark.jdbc
def test_verbose_exception(test_mp, exception_verbose_true):
    # Exception stack trace is logged for debugging
    with pytest.raises(RuntimeError) as exc_info:
        ixmp.Scenario(test_mp, model="foo", scenario="bar", version=-1)

    exc_msg = exc_info.value.args[0]
    assert (
        "There exists no Scenario 'foo|bar' (version: -1)  in the database!" in exc_msg
    )
    assert "at.ac.iiasa.ixmp.database.DbDAO.getRunId" in exc_msg
    assert "at.ac.iiasa.ixmp.Platform.getScenario" in exc_msg


def test_del_ts(request):
    mp = ixmp.Platform(
        backend="jdbc", driver="hsqldb", url=f"jdbc:hsqldb:mem:{request.node.name}"
    )

    backend: ixmp.backend.jdbc.JDBCBackend = mp._backend  # type: ignore

    # Number of Java objects referenced by the JDBCBackend: force to 0 before test
    backend.jindex.clear()
    N_obj = len(backend.jindex)
    assert N_obj == 0

    # Number of new objects to create
    N = 8

    # Create a list of `N` Scenario objects
    # A new instance of make_dantzig() with an incremented version number
    scenarios = [make_dantzig(mp, request=request)]
    # N-1 clones with distinct scenario names
    for i in range(1, N):
        # Use a name ending "… clone 9", "… clone 10" to avoid overlap
        name = f"{scenarios[0].scenario} clone {N_obj + i}"
        scenarios.append(scenarios[0].clone(scenario=name))

    # Number of referenced objects has increased by 8
    assert len(backend.jindex) == N_obj + N

    # Pop and free the objects
    for i in range(N):
        s = scenarios.pop(0)

        # Number of references to `s`
        N_ref = getrefcount(s)

        # The variable 's' should be the only reference to this Scenario object
        if 1 != N_ref - 1:
            # Show information about any other references
            if refs := gc.get_referrers(s):  # pragma: no cover
                lines = [f"{len(refs)} unexpected references to {s}:"]
                lines.extend(map(str, refs))
                log.warning("\n".join(lines))

        # Python ID of the Scenario object/instance
        s_id = id(s)

        # Underlying Java object
        s_jobj = backend.jindex[s]

        # Now delete the Scenario object
        # del s # should work, but doesn't always resolve to s.__del__()
        backend.del_ts(s)

        # Number of referenced objects decreases by 1
        assert len(backend.jindex) == N_obj + N - (i + 1)
        # No object with `s_id` remains in JDBCBackend.jindex
        assert s_id not in map(id, backend.jindex)

        # s_jobj is the only remaining reference to the Java object
        assert getrefcount(s_jobj) - 1 == 1
        del s_jobj

    # Backend is again empty
    assert len(backend.jindex) == N_obj


# NB coverage is omitted because this test is not included in the standard suite
@pytest.mark.performance
def test_gc_lowmem(request):  # pragma: no cover
    """Test Java-side garbage collection (GC).

    By default, this test limits the JVM memory to 7 MiB. To change this limit, use the
    command-line option --jvm-mem-limit=7.
    """

    # Create a platform with a small memory limit (default 7 MiB)
    mp = ixmp.Platform(
        backend="jdbc",
        driver="hsqldb",
        url="jdbc:hsqldb:mem:test_gc",
        jvmargs=f"-Xmx{request.config.getoption('--jvm-mem-limit', 7)}M",
    )
    # Force Java GC
    jpype.java.lang.System.gc()

    def allocate_scenarios(n):
        for i in range(n):
            scenarios.append(ixmp.Scenario(mp, "foo", f"bar{i}", version="new"))

    scenarios = []
    # Fill *scenarios* with Scenario object until out of memory
    raises(jpype.java.lang.OutOfMemoryError, allocate_scenarios, 100000)
    # Record the maximum number
    max = len(scenarios)

    # Clean up and GC Python and Java memory
    scenarios = []
    gc.collect()
    jpype.java.lang.System.gc()

    # Can allocate *max* scenarios again
    allocate_scenarios(max)
    # …but not twice as many
    raises(jpype.java.lang.OutOfMemoryError, allocate_scenarios, max)


@pytest.fixture(scope="session")
def rc_data_size():  # pragma: no cover
    """Number of data rows for :meth:`test_reload_cycle` and its fixtures."""
    return 5e4


@pytest.fixture(scope="session")
def reload_cycle_scenario(request, tmp_path_factory, rc_data_size):  # pragma: no cover
    """Set up a Platform with *rc_data_size* of  random data."""
    # Command-line option for the JVM memory limit
    kwarg = dict()
    max_memory = int(request.config.getoption("--ixmp-jvm-mem"))
    if max_memory > 0:
        kwarg["jvmargs"] = f"-Xmx{max_memory}M"

    # Path for this database
    path = tmp_path_factory.mktemp("reload_cycle") / "base"

    # Create the Platform. This should be the first in the process, so the
    # jvmargs are used in :func:`.jdbc.start_jvm`.
    mp = ixmp.Platform(backend="jdbc", driver="hsqldb", path=path, **kwarg)

    s0 = ixmp.Scenario(mp, model="foo", scenario="bar 0", version="new")

    # Add data

    # currently omitted: time series data with *rc_data_size* elements
    # s0.add_timeseries(random_ts_data(rc_data_size))

    # A set named 'random_set' and parameter 'random_par' with *rc_data_size*
    # elements
    add_random_model_data(s0, rc_data_size)

    s0.commit("")

    yield s0


@pytest.mark.performance
@pytest.mark.parametrize("cache", [True, False], ids=bool_param_id("cache"))
@pytest.mark.parametrize("gc", [True, False], ids=bool_param_id("gc"))
@pytest.mark.parametrize("gdx", [True, False], ids=bool_param_id("gdx"))
def test_reload_cycle(
    resource_limit,
    reload_cycle_scenario,
    tmp_path,
    cache,
    gc,
    gdx,
    rc_data_size,
    N_cycles=2,
):  # pragma: no cover
    """Test a cycle of Platform/Scenario reloading.

    This test simulates the usage in the 'runscripts' often used for the
    IIASA MESSAGEix-GLOBIOM global model. Namely:

    1. A large Scenario is created (see the :meth:`reload_cycle_scenario` and
       :meth:`rc_data_size` fixtures).
    2. The Platform instance is discarded, and recreated.
    3. A base Scenario is loaded.
    4. This Scenario is cloned.
    5. The Scenario is solved. (This tests omits this step, but when the `gdx`
       argument is :obj:`True`, the GDX file is written.)
    6. Repeat from (2).

    In order to use this test:

    - Adjust the value in :meth:`rc_data_size`.
    - Adjust the keyword argument `N_cycles`, the minimum number of cycles (
      steps (2) through (5)) required for the test pass.
    - Run by invoking pytest with, e.g.::

        pytest -m performance -k reload_cycle \
          --verbose --log-cli-level=DEBUG -rA \
          --resource-limit=DATA:1024 \
          --jvm-mem-limit=256

      Line by line, these options:

      - Select the current test.
      - Set verbose output, including log messages from the DEBUG level,
        even when the tests pass.
      - Limit Python's resource.RLIMIT_DATA to 1024 MiB.
      - Limit the JVM memory usage to 256 MiB.
    """
    # NB coverage is omitted because this test is not included in the standard
    #    suite

    # Clone reload_cycle_scenario onto a new Platform for this test
    platform_args = dict(
        backend="jdbc",
        driver="hsqldb",
        path=tmp_path / "testdb",
        cache=cache,
        log_level="WARNING",
    )
    mp = ixmp.Platform(**platform_args)
    reload_cycle_scenario.clone(platform=mp)

    # Throw away the reference to mp
    mp = None

    # GC before cycling
    java.System.gc()

    # Set the garbage collection behaviour of JDBCBackend
    ixmp.backend.jdbc._GC_AGGRESSIVE = gc

    pre = memory_usage("setup")

    for i in range(1, N_cycles + 1):
        # New Platform instance; throw away old reference
        mp = ixmp.Platform(**platform_args)

        # Load existing Scenario
        s0 = ixmp.Scenario(mp, model="foo", scenario=f"bar {i - 1}", version=1)

        memory_usage(f"pass {i} -- platform instantiated")

        # Clone Scenario
        s1 = s0.clone(scenario=f"bar {i}")

        memory_usage(f"pass {i} -- cloned")

        # Load data into memory
        df_par = s1.par("random_par")
        # commented: omitted
        # df_ts = s1.timeseries()

        memory_usage(f"pass {i} -- data loaded")

        # The variable 's0' is the only reference to this Scenario object
        assert getrefcount(s0) - 1 == 1

        if gdx:
            # Write to file
            mp._backend.write_file(
                tmp_path / "test.gdx",
                ixmp.ItemType.SET | ixmp.ItemType.PAR,
                filters=dict(scenario=s1),
            )

            memory_usage(f"pass {i} -- GDX written")

        # Use, then throw away, references to s0 and data
        assert len(df_par) >= rc_data_size
        s0, df_par = None, None
        # commented: omitted
        # assert len(df_ts) == rc_data_size
        # df_ts = None

        memory_usage(f"pass {i} -- replaced")

        # Throw away reference to mp
        mp = None

    post = memory_usage("post")

    log.info(
        "JVM memory usage gained {:.3f} MiB / cycle".format(
            (post[-2] - pre[-2]) / N_cycles
        )
    )
    log.info(
        "Total memory usage gained {:.3f} MiB / cycle".format(
            (post[0] - pre[0]) / N_cycles
        )
    )

    # Throw away reference to s1
    s1 = None

    memory_usage("shutdown")


# TODO Not yet implemented by IXMP4Backend
@pytest.mark.jdbc
def test_docs(test_mp, request):
    scen = make_dantzig(test_mp, request=request)
    # test model docs
    test_mp.set_doc("model", {scen.model: "Dantzig model"})
    assert test_mp.get_doc("model") == {"canning problem": "Dantzig model"}

    # test timeseries variables docs
    gdp = (
        "Gross Domestic Product (GDP) is the monetary value of all "
        "finished goods and services made within a country during "
        "a specific period."
    )
    test_mp.set_doc("timeseries", dict(GDP=gdp))
    assert test_mp.get_doc("timeseries", "GDP") == gdp

    # test bad domain
    ex = raises(ValueError, test_mp.set_doc, "baddomain", {})
    exp = (
        "No such domain: baddomain, existing domains: "
        "scenario, model, region, metadata, timeseries"
    )
    assert ex.value.args[0] == exp


def test_cache_clear(test_mp, request):
    """Removing set elements causes the cache to be cleared entirely."""
    scen = make_dantzig(test_mp, request=request)

    # Load an item so that it is cached
    d0 = scen.par("d")

    # Remove a set element
    scen.check_out()
    scen.remove_set("j", "topeka")

    # The retrieved item content reflects the removal of 'topeka'; not the
    # cached value used to return d0
    assert scen.par("d").shape[0] < d0.shape[0]
