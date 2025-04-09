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
import shutil
import sys
from collections.abc import Generator
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from itertools import chain
from pathlib import Path
from typing import Any, Literal

import pint
import pytest
from click.testing import CliRunner

from ixmp import BACKENDS, Platform, cli
from ixmp import config as ixmp_config

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

# Parametrize platforms for jdbc and ixmp4
backends = list(BACKENDS.keys())
# ixmp4 is not published for Python 3.9
if sys.version_info < (3, 10):
    backends.remove("ixmp4")


# Pytest hooks


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add the ``--user-config`` command-line option to pytest."""
    parser.addoption(
        "--ixmp-jvm-mem",
        action="store",
        help="Heap space to allocated for the JDBCBackend/JVM.",
    )
    parser.addoption(
        "--ixmp-user-config",
        action="store_true",
        help="Use the user's existing config/'local' platform.",
    )


def pytest_sessionstart(session: pytest.Session) -> None:
    """Unset any configuration read from the user's directory."""
    from ixmp.backend import jdbc

    if not session.config.option.ixmp_user_config:
        ixmp_config.clear()
        # Further clear an automatic reference to the user's home directory. See fixture
        # tmp_env below.
        ixmp_config.values["platform"]["local"].pop("path")

    # Disable slow, aggressive garbage collection
    jdbc._GC_AGGRESSIVE = False


def pytest_report_header(config, start_path) -> str:
    """Add the ixmp configuration to the pytest report header."""
    return f"ixmp config: {repr(ixmp_config.values)}"


# Session-scoped fixtures


@pytest.fixture(scope="session")
def ixmp_cli(tmp_env):
    """A CliRunner object that invokes the ixmp command-line interface."""

    class Runner(CliRunner):
        def invoke(self, *args, **kwargs):
            return super().invoke(cli.main, *args, env=tmp_env, **kwargs)

    yield Runner()


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
def test_data_path() -> Path:
    """Path to the directory containing test data."""
    return Path(__file__).parents[1].joinpath("tests", "data")


@pytest.fixture(scope="module", params=backends)
def test_mp(
    request: pytest.FixtureRequest, tmp_env, test_data_path
) -> Generator[Platform, Any, None]:
    """An empty :class:`.Platform` connected to a temporary, in-memory database.

    This fixture has **module** scope: the same Platform is reused for all tests in a
    module.
    """
    yield from _platform_fixture(
        request, tmp_env, test_data_path, backend=request.param
    )


@pytest.fixture(scope="session")
def tmp_env(
    pytestconfig: pytest.Config, tmp_path_factory: pytest.TempPathFactory
) -> Generator[os._Environ[str], Any, None]:
    """Return the os.environ dict with the IXMP_DATA variable set.

    IXMP_DATA will point to a temporary directory that is unique to the test session.
    ixmp configuration (i.e. the 'config.json' file) can be written and read in this
    directory without modifying the current user's configuration.
    """
    base_temp = tmp_path_factory.getbasetemp()
    os.environ["IXMP_DATA"] = str(base_temp)

    if not pytestconfig.option.ixmp_user_config:
        # Set the path for the default/local platform in the test directory
        localdb = base_temp.joinpath("localdb", "default")
        ixmp_config.values["platform"]["local"]["path"] = localdb

    # Save for other processes
    ixmp_config.save()

    yield os.environ


@pytest.fixture(scope="session")
def tutorial_path() -> Path:
    """Path to the directory containing the tutorials."""
    return Path(__file__).parents[2].joinpath("tutorial")


@pytest.fixture(scope="session")
def ureg() -> Generator[pint.UnitRegistry, Any, None]:
    """Application-wide units registry."""
    registry = pint.get_application_registry()

    # Used by .compat.ixmp, .compat.pyam
    registry.define("USD = [USD]")
    registry.define("case = [case]")

    yield registry


# Function-scoped fixtures


@pytest.fixture(scope="function")
def protect_pint_app_registry():
    """Protect pint's application registry.

    Use this fixture on tests which invoke code that calls
    :meth:`pint.set_application_registry`. It ensures that the environment for
    other tests is not altered.
    """
    import pint

    # Use deepcopy() in case the wrapped code modifies the application
    # registry without swapping out the UnitRegistry instance for a different
    # one
    saved = deepcopy(pint.get_application_registry())
    yield
    pint.set_application_registry(saved)


@pytest.fixture(scope="function")
def protect_rename_dims():
    """Protect :data:`RENAME_DIMS`.

    Use this fixture on tests which invoke code that imports :mod:`message_ix`, e.g.
    :func:`show_versions`. Importing :mod:`message_ix` has the side effect of adding
    values to :data:`RENAME_DIMS`. Using this fixture ensures that the environment for
    other tests is not altered.
    """
    from ixmp.report import RENAME_DIMS

    saved = deepcopy(RENAME_DIMS)  # Probably just copy() is sufficient
    yield
    RENAME_DIMS.clear()
    RENAME_DIMS.update(saved)


@pytest.fixture(scope="function", params=backends)
def test_mp_f(request: pytest.FixtureRequest, tmp_env, test_data_path):
    """An empty :class:`Platform` connected to a temporary, in-memory database.

    This fixture has **function** scope: the same Platform is reused for one test
    function.

    See also
    --------
    test_mp
    """
    yield from _platform_fixture(
        request, tmp_env, test_data_path, backend=request.param
    )


# Assertions


@contextmanager
def assert_logs(caplog, message_or_messages=None, at_level=None):
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
    expected = (
        [message_or_messages]
        if isinstance(message_or_messages, str)
        else message_or_messages
    )

    # Record the number of records prior to the managed block
    first = len(caplog.records)

    if at_level is not None:
        # Use the pytest caplog fixture's built-in context manager to temporarily set
        # the level of the 'ixmp' logger
        ctx = caplog.at_level(at_level, logger="ixmp")
    else:
        # ctx does nothing
        ctx = nullcontext()

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


def bool_param_id(name):
    """Parameter ID callback for :meth:`pytest.mark.parametrize`.

    This formats a boolean value as 'name0' (False) or 'name1' (True) for
    easier selection with e.g. ``pytest -k 'name0'``.
    """
    return lambda value: "{}{}".format(name, int(value))


def create_test_platform(tmp_path, data_path, name, **properties):
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


# TODO Check if this setup is necessary when running all tests (not just test_platform)
def _setup_ixmp4_platform(mp: Platform) -> None:
    """Set up an ixmp4-backed Platform with things hardcoded in Java."""
    mp.add_unit("cases", comment="As pre-defined in Java.")
    mp.add_unit("km", comment="As pre-defined in Java.")
    mp.add_region(region="World", hierarchy="common")


def _platform_fixture(
    request: pytest.FixtureRequest,
    tmp_env,
    test_data_path,
    backend: Literal["jdbc", "ixmp4"],
) -> Generator[Platform, Any, None]:
    """Helper for :func:`test_mp` and other fixtures."""
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace("/", " ")

    # Add a platform
    if backend == "jdbc":
        ixmp_config.add_platform(
            platform_name, backend, "hsqldb", url=f"jdbc:hsqldb:mem:{platform_name}"
        )
    elif backend == "ixmp4":
        from ixmp4.conf.base import PlatformInfo
        from ixmp4.data.backend import SqliteTestBackend

        # Setup ixmp4 backend and run DB migrations
        sqlite = SqliteTestBackend(
            PlatformInfo(name=platform_name, dsn="sqlite:///:memory:")
        )
        sqlite.setup()

        # Add ixmp4 backend to ixmp platforms
        ixmp_config.add_platform(platform_name, backend, _backend=sqlite)

    # Launch Platform
    mp = Platform(name=platform_name, backend=backend)
    if backend == "ixmp4":
        _setup_ixmp4_platform(mp=mp)
    yield mp

    # Teardown: don't show log messages when destroying the platform, even if
    # the test using the fixture modified the log level
    mp._backend.set_log_level(logging.CRITICAL)
    del mp

    # Remove from config
    ixmp_config.remove_platform(platform_name)

    if backend == "ixmp4":
        assert sqlite  # to satisfy type checkers

        # Close DB connection and remove platform
        sqlite.close()
        sqlite.teardown()
