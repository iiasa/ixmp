"""Utilities for testing ixmp.

These include:

- pytest hooks, `fixtures <https://docs.pytest.org/en/latest/fixture.html>`_:

  .. autosummary::
     :nosignatures:

     ixmp_cli
     tmp_env
     test_mp

  …and assertions:

  .. autosummary::
     assert_logs

- Methods for setting up and populating test ixmp databases:

  .. autosummary::
     add_test_data
     create_test_platform
     make_dantzig
     populate_test_platform

- Methods to run and retrieve values from Jupyter notebooks:

  .. autosummary::
     run_notebook
     get_cell_output

"""

import logging
import os
import platform
import shutil
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Sequence, TypeAlias

import pint
import pytest
from click.testing import CliRunner, Result

# TODO Import from typing when dropping support for Python 3.11
from typing_extensions import override

from ixmp import Platform, Scenario, cli
from ixmp import config as ixmp_config
from ixmp.backend import available
from ixmp.backend.ixmp4 import IXMP4Backend
from ixmp.util.ixmp4 import is_ixmp4backend

from .data import (
    DATA,
    HIST_DF,
    TS_DF,
    add_random_model_data,
    add_test_data,
    make_dantzig,
    models,
    populate_test_platform,
    random_model_data,
    random_ts_data,
)
from .jupyter import get_cell_output, run_notebook
from .resource import resource_limit

if TYPE_CHECKING:
    from _pytest.mark import ParameterSet
    from ixmp4.core import Run

    try:
        # Pint 0.25.1 or later
        UReg: TypeAlias = pint.UnitRegistry[float]
    except TypeError:
        # Python 3.10 / earlier pint version without parametrized generic
        UReg: TypeAlias = pint.UnitRegistry  # type: ignore[no-redef,misc]

log = logging.getLogger(__name__)

__all__ = [
    "DATA",
    "HIST_DF",
    "TS_DF",
    "add_random_model_data",
    "add_test_data",
    "assert_logs",
    "create_test_platform",
    "get_cell_output",
    "ixmp_cli",
    "make_dantzig",
    "models",
    "populate_test_platform",
    "random_model_data",
    "random_ts_data",
    "resource_limit",
    "run_notebook",
    "test_mp",
    "tmp_env",
]

#: :any:`True` if testing is occurring on GitHub Actions runners/machines.
GHA = "GITHUB_ACTIONS" in os.environ

_uname = platform.uname()

#: Pytest marks for use throughout the test suite.
#:
#: - ``ixmp4#209``: https://github.com/iiasa/ixmp4/pull/209,
#:   https://github.com/unionai-oss/pandera/pull/2158.
MARK = {
    "IXMP4Backend Never": pytest.mark.xfail(
        reason="Not implemented on IXMP4Backend", raises=NotImplementedError
    ),
    "IXMP4Backend Not Yet": pytest.mark.xfail(
        reason="Not yet supported by IXMP4Backend"
    ),
    "ixmp4#209": pytest.mark.xfail(
        condition=platform.python_version_tuple() >= ("3", "14", "0"),
        reason="ixmp4/pandera do not yet support Python 3.14",
    ),
    "pytest#10843": pytest.mark.xfail(
        condition=GHA
        and _uname.system == "Windows"
        and ("2025" in _uname.release or _uname.version >= "10.0.26100"),
        reason="https://github.com/pytest-dev/pytest/issues/10843",
    ),
}

# Pytest hooks


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add the ``--user-config`` command-line option to pytest."""
    parser.addoption(
        "--ixmp-jvm-mem",
        action="store",
        default=-1,
        help="Memory limit, in MiB, for the Java Virtual Machine (JVM) started by the "
        "ixmp JDBCBackend",
    )
    parser.addoption(
        "--ixmp-resource-limit",
        action="store",
        default="DATA:-1",
        help=(
            "Limit a Python resource via the ixmp.testing.resource_limit fixture. Use "
            "e.g. 'DATA:500' to set RLIMIT_DATA to 500 MiB."
        ),
    )
    parser.addoption(
        "--ixmp-user-config",
        action="store_true",
        help="Use the user's existing ixmp config, including platforms.",
    )


def pytest_sessionstart(session: pytest.Session) -> None:
    """Unset configuration read from the user's home directory or ``IXMP_DATA.

    If :mod:`ixmp.testing` is required as a pytest plugin, this hook will *always* run,
    even if the :func:`tmp_env` fixture is not used.

    - *Unless* :program:`pytest … --ixmp-user-config` is given:

      - The user's existing configuration is discarded and replaced with defaults.
      - The path to the "local" (also "default") platform, which by default is in the
        user's home directory, is cleared. This setting **must** be repopulated with a
        valid path if :py:`ixmp.Platform()`, :py:`ixmp.Platform("default")`, or
        :py:`ixmp.Platform("local")` is to be used. One way to do this is by using
        the :func:`tmp_env` fixture.

    - :data:`.jdbc._GC_AGGRESSIVE` is set to :any:`False` to disable aggressive garbage
      collection, which can be slow.
    """
    from ixmp.backend import available, jdbc

    if not session.config.option.ixmp_user_config:
        ixmp_config.clear()
        # Further clear an automatic reference to the user's home directory. See fixture
        # tmp_env below.
        ixmp_config.values["platform"]["local"].pop("path")

    jdbc._GC_AGGRESSIVE = False

    if "ixmp4" in available():
        from sqlalchemy import create_engine, text
        from xdist import get_xdist_worker_id

        db_name = f"ixmp_test_{get_xdist_worker_id(session)}"

        engine = create_engine(
            "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
            isolation_level="AUTOCOMMIT",
        )
        with engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE {db_name}"))

        engine.dispose()


def pytest_sessionfinish(
    session: pytest.Session, exitstatus: int | pytest.ExitCode
) -> None:
    if "ixmp4" in available():
        from sqlalchemy import create_engine, text
        from xdist import get_xdist_worker_id

        db_name = f"ixmp_test_{get_xdist_worker_id(session)}"

        engine = create_engine(
            "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
            isolation_level="AUTOCOMMIT",
        )
        with engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE {db_name}"))

        engine.dispose()


def pytest_report_header(config: pytest.Config, start_path: Path) -> str:
    """Add the ixmp configuration to the pytest report header."""
    return f"ixmp config: {repr(ixmp_config.values)}"


# NOTE https://docs.pytest.org/en/latest/example/markers.html#marking-platform-specific-tests-with-pytest
# sound like what we need, but I couldn't quite get it to work. Instead, this is more
# following https://pytest-with-eric.com/introduction/pytest-generate-tests/
def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize tests for the two backend options."""
    if "backend" not in metafunc.fixturenames:
        return

    import ixmp.backend

    # Subset of marker names applied to the test function
    marker_names = sorted(
        set(m.name for m in metafunc.definition.iter_markers())
        & {"ixmp4", "ixmp4_209", "ixmp4_never", "ixmp4_not_yet", "jdbc"}
    )

    # Argument values for pytest.parametrize()
    argvalues: list["str | ParameterSet"] = []

    # Iterate over all available backends
    for backend_name in sorted(ixmp.backend.available()):
        # Match on the backend name followed by 0 or more marker names
        match [backend_name] + marker_names:
            case ["jdbc", "ixmp4", *_] | ["ixmp4", *_, "jdbc"]:
                # These markers mean "even though a parametrized fixture is used, this
                # test should run only for {IXMP4,JDBC}Backend"
                continue
            case ["ixmp4", "ixmp4_never"]:  # "Won't ever be implemented on IXMP4"
                mark: Any = MARK["IXMP4Backend Never"]
            case ["ixmp4", "ixmp4_not_yet"]:  # "Not yet supported on IXMP4"
                mark = MARK["IXMP4Backend Not Yet"]
            # FIXME Remove the following 2 case blocks once iiasa/ixmp4#209 is resolved
            case ["ixmp4", *_, "ixmp4_209"]:
                mark = MARK["ixmp4#209"]
            case ["ixmp4", "ixmp4_209", *_]:
                # ixmp4_209 + other markers like _{never,not_yet} → use a broader XFAIL
                mark = MARK["IXMP4Backend Not Yet"]
            case _:
                mark = []

        argvalues.append(pytest.param(backend_name, marks=mark))

    metafunc.parametrize("backend", argvalues, indirect=True)


# Session-scoped fixtures


class Runner(CliRunner):
    @override
    # Why does mypy ignore the decorator?
    def invoke(self, args: Sequence[str]) -> Result:  # type: ignore[override]
        return super().invoke(cli.main, args=args, env=self.tmp_env)

    def __init__(self, env: os._Environ[str]) -> None:
        super().__init__()

        self.tmp_env = env


@pytest.fixture(scope="session")
def ixmp_cli(tmp_env: os._Environ[str]) -> Generator["Runner", Any, None]:
    """A CliRunner object that invokes the ixmp command-line interface."""

    yield Runner(env=tmp_env)


@pytest.fixture(scope="module")
def mp(test_mp: Platform) -> Generator[Platform, Any, None]:
    """A :class:`.Platform` containing test data.

    This fixture is **module** -scoped, and is used in :mod:`.test_platform`,
    :mod:`.test_timeseries`, and :mod:`.test_scenario`. :mod:`.test_meta` overrides this
    with a **function** -scoped fixture; see there for more details.
    """
    populate_test_platform(test_mp)
    yield test_mp


@pytest.fixture(scope="session")
def refcount_offset() -> int:
    """Offset for :func:`sys.getrefcount` return values, changed in Python 3.14."""
    return 1 if platform.python_version_tuple() < ("3", "14") else 0


@pytest.fixture(scope="session")
def test_data_path() -> Path:
    """Path to the directory containing test data."""
    return Path(__file__).parents[1].joinpath("tests", "data")


# NOTE We need to declare this as module-scope explicitly; otherwise, pytest creates
# backend for pytest_generate_tests as function-scoped fixture automatically
@pytest.fixture(scope="module")
def backend(request: pytest.FixtureRequest) -> Literal["ixmp4", "jdbc"]:
    # pytest_generate_tests() applies these marks, pytest always only registers Any
    return request.param  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def default_platform_name(backend: str) -> str:
    """Name of the default platform according to the `backend`."""
    return {"ixmp4": "ixmp4-local", "jdbc": "local"}[backend]


@pytest.fixture(scope="module")
def test_mp(
    request: pytest.FixtureRequest,
    tmp_env: os._Environ[str],
    test_data_path: Path,
    backend: Literal["ixmp4", "jdbc"],
    worker_id: str,
) -> Generator[Platform, Any, None]:
    """An empty :class:`.Platform` connected to a temporary, in-memory database.

    This fixture has **module** scope: the same Platform is reused for all tests in a
    module.
    """
    yield from _platform_fixture(
        request, tmp_env, test_data_path, backend=backend, worker_id=worker_id
    )


@pytest.fixture(scope="session")
def tmp_env(
    pytestconfig: pytest.Config, tmp_path_factory: pytest.TempPathFactory
) -> Generator[os._Environ[str], Any, None]:
    """Temporary environment for testing.

    In this environment:

    - The environment variable ``IXMP_DATA`` points to a temporary directory that is
      unique to the test session.
    - *Unless* :program:`pytest … --ixmp-user-config` is given, the "local" (also
      "default") platform path is set to a file within the ``IXMP_DATA`` directory.
    - The :file:`config.json` file is saved in ``IXMP_DATA``.

    For :mod:`ixmp.tests`, this fixture is automatically invoked for every test session.
    In downstream packages that use :mod:`ixmp.testing`, this *may not* be the case.

    Returns
    -------
    dict
        A reference to :data:`os.environ` with the ``IXMP_DATA`` key set.
    """
    base_temp = tmp_path_factory.getbasetemp()
    os.environ["IXMP_DATA"] = str(base_temp)

    if not pytestconfig.option.ixmp_user_config:
        # Clear user's config. This (harmlessly) duplicates pytest_sessionstart, above.
        ixmp_config.clear()
        # Replace an automatic reference to the user's home directory with path to a
        # default/local platform within the pytest temporary directory
        localdb = base_temp.joinpath("localdb", "default")
        ixmp_config.values["platform"]["local"]["path"] = localdb

    # Save for other processes
    ixmp_config.save()

    try:
        import ixmp4.conf

        # Replace an automatic reference to the user's home directory with a
        # subdirectory of the pytest temporary directory
        ixmp4.conf.settings.storage_directory = base_temp.joinpath("ixmp4")
        # Ensure this directory and a further subdirectory "databases" exist
        ixmp4.conf.settings.storage_directory.joinpath("databases").mkdir(
            parents=True, exist_ok=True
        )
    except ImportError:
        pass

    yield os.environ


@pytest.fixture(scope="session")
def tutorial_path() -> Path:
    """Path to the directory containing the tutorials."""
    return Path(__file__).parents[2].joinpath("tutorial")


@pytest.fixture(scope="session")
def ureg() -> Generator["UReg", Any, None]:
    """Application-wide units registry."""
    # Pylance registers an ApplicationRegistry, so maybe try `follow-untyped-imports`?
    registry = pint.get_application_registry()  # type: ignore[no-untyped-call]

    # Used by .compat.ixmp, .compat.pyam
    registry.define("USD = [USD]")
    registry.define("case = [case]")

    yield registry


# Function-scoped fixtures


@pytest.fixture(scope="function")
def protect_pint_app_registry() -> Generator[None, Any, None]:
    """Protect pint's application registry.

    Use this fixture on tests which invoke code that calls
    :meth:`pint.set_application_registry`. It ensures that the environment for
    other tests is not altered.
    """
    import pint

    # Use deepcopy() in case the wrapped code modifies the application
    # registry without swapping out the UnitRegistry instance for a different
    # one
    saved = deepcopy(pint.get_application_registry())  # type: ignore[no-untyped-call]
    yield
    pint.set_application_registry(saved)  # type: ignore[no-untyped-call]


@pytest.fixture(scope="function")
def protect_rename_dims() -> Generator[None, Any, None]:
    """Protect :data:`RENAME_DIMS`.

    Use this fixture on tests which invoke code that imports :mod:`message_ix`, e.g.
    :func:`show_versions`. Importing :mod:`message_ix` has the side effect of adding
    values to :data:`RENAME_DIMS`. Using this fixture ensures that the environment for
    other tests is not altered.
    """
    from ixmp.report.common import RENAME_DIMS

    saved = deepcopy(RENAME_DIMS)  # Probably just copy() is sufficient
    yield
    RENAME_DIMS.clear()
    RENAME_DIMS.update(saved)


@pytest.fixture(scope="function")
def test_mp_f(
    request: pytest.FixtureRequest,
    tmp_env: os._Environ[str],
    test_data_path: Path,
    backend: Literal["ixmp4", "jdbc"],
    worker_id: str,
) -> Generator[Platform, Any, None]:
    """An empty :class:`Platform` connected to a temporary, in-memory database.

    This fixture has **function** scope: the same Platform is reused for one test
    function.

    See also
    --------
    test_mp
    """
    yield from _platform_fixture(
        request, tmp_env, test_data_path, backend=backend, worker_id=worker_id
    )


@pytest.fixture
def ixmp4_backend(test_mp: Platform) -> IXMP4Backend:
    assert isinstance(test_mp._backend, IXMP4Backend)
    return test_mp._backend


@pytest.fixture
def scenario(test_mp: Platform, request: pytest.FixtureRequest) -> Scenario:
    return Scenario(
        mp=test_mp,
        model=request.node.nodeid + "model",
        scenario="scenario",
        version="new",
    )


@pytest.fixture
def run(ixmp4_backend: IXMP4Backend, scenario: Scenario) -> "Run":
    # NOTE New Scenario-backing Runs are locked per default, but our tests expect them
    # to be lockable for `transact()`
    _run = ixmp4_backend.index[scenario]
    _run._unlock()
    return _run


# Assertions


@contextmanager
def assert_logs(
    caplog: pytest.LogCaptureFixture,
    message_or_messages: str | Iterable[str] | None = None,
    at_level: int | None = None,
) -> Generator[None, Any, None]:
    """Assert that *message_or_messages* appear in logs.

    Use assert_logs as a context manager for a statement that is expected to trigger
    certain log messages. assert_logs checks that these messages are generated.

    Example
    -------

    def test_foo(caplog):
        with assert_logs(caplog, 'a message'):
            logging.getLogger(__name__).info('this is a message!')

    Parameters
    ----------
    caplog : object
        The pytest caplog fixture.
    message_or_messages : str or list of str
        String(s) that must appear in log messages.
    at_level : int, optional
        Messages must appear on 'ixmp' or a sub-logger with at least this level.
    """
    __tracebackhide__ = True

    # Wrap a string in a list
    expected = [message_or_messages] if isinstance(message_or_messages, str) else []

    # Record the number of records prior to the managed block
    first = len(caplog.records)

    # Use the pytest caplog fixture's built-in context manager to temporarily set the
    # level of the 'ixmp' logger if requested; otherwise, ctx does nothing
    ctx = (
        caplog.at_level(at_level, logger="ixmp")
        if at_level is not None
        else nullcontext()
    )

    try:
        with ctx:
            yield  # Nothing provided to the managed block
    finally:
        # List of bool indicating whether each of `expected` was found
        found = [any(e in msg for msg in caplog.messages[first:]) for e in expected]

        if not all(found):
            # Format a description of the missing messages
            lines = chain(
                ["Did not log:"],
                [f"    {repr(msg)}" for i, msg in enumerate(expected) if not found[i]],
                ["among:"],
                ["    []"]
                if len(caplog.records) == first
                else [f"    {repr(msg)}" for msg in caplog.messages[first:]],
            )
            pytest.fail("\n".join(lines))


# Utility functions


def bool_param_id(name: str) -> Callable[[Any], str]:
    """Parameter ID callback for :meth:`pytest.mark.parametrize`.

    This formats a boolean value as 'name0' (False) or 'name1' (True) for
    easier selection with e.g. ``pytest -k 'name0'``.
    """
    return lambda value: "{}{}".format(name, int(value))


def create_test_platform(
    tmp_path: Path, data_path: Path, name: str, **properties: object
) -> Path:
    """Create a Platform for testing using specimen files '*name*.*'.

    Any of the following files from *data_path* are copied to *tmp_path*:

    - *name*.lobs, *name*.script, i.e. the contents of a :class:`.JDBCBackend`
      HyperSQL database.
    - *name*.properties.

    The contents of *name*.properties (if it exists) are formatted using the
    *properties* keyword arguments.

    Returns
    -------
    pathlib.Path
        the path to the .properties file, if any, else the .lobs file without
        suffix.
    """
    # Copy files
    any_files = False
    for suffix in ".lobs", ".properties", ".script":
        src = (data_path / name).with_suffix(suffix)
        dst = (tmp_path / name).with_suffix(suffix)
        try:
            shutil.copyfile(str(src), str(dst))
        except FileNotFoundError:
            pass
        else:
            any_files = True

    if not any_files:
        raise ValueError(f"no files for test platform {repr(name)}")

    # Create properties file
    props_file = (tmp_path / name).with_suffix(".properties")

    try:
        props = props_file.read_text()
    except FileNotFoundError:
        # No properties file; return the stub
        return tmp_path / name
    else:
        props = props.format(db_path=str(tmp_path / name), **properties)
        props_file.write_text(props)
        return props_file


# Private utilities


def _platform_fixture(
    request: pytest.FixtureRequest,
    tmp_env: os._Environ[str],
    test_data_path: Path,
    backend: Literal["jdbc", "ixmp4"],
    worker_id: str,
) -> Generator[Platform, Any, None]:
    """Helper for :func:`test_mp` and other fixtures."""
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace("/", " ")

    # Construct positional and keyword arguments to Config.add_platform()
    if backend == "jdbc":
        args = ["hsqldb"]
        kwargs: dict[str, Any] = dict(url=f"jdbc:hsqldb:mem:{platform_name}")
    elif backend == "ixmp4":
        args = []
        kwargs = dict(
            ixmp4_name=f"ixmp_test_{worker_id}",
            dsn=f"postgresql+psycopg://postgres:postgres@localhost:5432/ixmp_test_{worker_id}",
            jdbc_compat=True,
        )
        if request.scope == "function":
            # NOTE Need this to recreate an empty DB in ixmp4 for test_mp_f
            from ixmp4.data.backend.db import SqlAlchemyBackend

            from ixmp.backend.ixmp4 import IXMP4Backend

            _backend = IXMP4Backend(**kwargs)
            assert isinstance(_backend._backend, SqlAlchemyBackend)
            _backend._backend.close()
            # TODO Properly isinstance check and remove these once we drop Python 3.9
            _backend._backend.teardown()

    # Add platform to ixmp configuration
    ixmp_config.add_platform(platform_name, backend, *args, **kwargs)

    # Launch Platform
    mp = Platform(name=platform_name)

    if is_ixmp4backend(mp._backend):
        from ixmp4.data.backend.db import SqlAlchemyBackend

        assert isinstance(mp._backend._backend, SqlAlchemyBackend)
        mp._backend._backend.setup()

    yield mp

    # Teardown: don't show log messages when destroying the platform, even if the test
    # using the fixture modified the log level
    mp._backend.set_log_level(logging.CRITICAL)

    # NOTE Following the teardown in ixmp4's backend fixtures. Due to the setup above,
    # mp._backend._backend is always of type PostgresTestBackend
    if is_ixmp4backend(mp._backend):
        assert isinstance(mp._backend._backend, SqlAlchemyBackend)
        mp._backend._backend.close()
        mp._backend._backend.teardown()

    del mp

    # Remove from configuration
    ixmp_config.remove_platform(platform_name)


# NOTE This is a workaround for https://github.com/iiasa/ixmp4/issues/205
@pytest.fixture(scope="function")
def _rollback_ixmp4_session(mp: Platform) -> Generator[None, None, None]:
    yield

    if is_ixmp4backend(mp._backend):
        from ixmp4.data.backend.test import PostgresTestBackend

        assert isinstance(mp._backend._backend, PostgresTestBackend)
        mp._backend._backend.session.rollback()
