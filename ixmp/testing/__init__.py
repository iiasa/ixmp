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

from ixmp import Platform, Scenario, cli
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

GHA = "GITHUB_ACTIONS" in os.environ

# Provide a skip marker since ixmp4 is not published for Python 3.9
min_ixmp4_version = pytest.mark.skipif(
    sys.version_info < (3, 10), reason="ixmp4 requires Python 3.10 or higher"
)

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
    from ixmp.backend import jdbc

    if not session.config.option.ixmp_user_config:
        ixmp_config.clear()
        # Further clear an automatic reference to the user's home directory. See fixture
        # tmp_env below.
        ixmp_config.values["platform"]["local"].pop("path")

    jdbc._GC_AGGRESSIVE = False


def pytest_report_header(config, start_path) -> str:
    """Add the ixmp configuration to the pytest report header."""
    return f"ixmp config: {repr(ixmp_config.values)}"


# NOTE https://docs.pytest.org/en/latest/example/markers.html#marking-platform-specific-tests-with-pytest
# sound like what we need, but I couldn't quite get it to work. Instead, this is more
# following https://pytest-with-eric.com/introduction/pytest-generate-tests/
def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize tests for the two backend options."""
    if "backend" in metafunc.fixturenames:
        import ixmp.backend

        ba = ixmp.backend.available()
        markers = set(m.name for m in metafunc.definition.iter_markers())

        metafunc.parametrize("backend", sorted(set(ba) & markers) or ba, indirect=True)


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


# NOTE We need to declare this as module-scope explicitly; otherwise, pytest creates
# backend for pytest_generate_tests as function-scoped fixture automatically
@pytest.fixture(scope="module")
def backend(request):
    return request.param


@pytest.fixture(scope="module")
def test_mp(
    request: pytest.FixtureRequest,
    tmp_env,
    test_data_path,
    backend: Literal["ixmp4", "jdbc"],
) -> Generator[Platform, Any, None]:
    """An empty :class:`.Platform` connected to a temporary, in-memory database.

    This fixture has **module** scope: the same Platform is reused for all tests in a
    module.
    """
    yield from _platform_fixture(request, tmp_env, test_data_path, backend=backend)


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


@pytest.fixture(scope="function")
def test_mp_f(
    request: pytest.FixtureRequest,
    tmp_env,
    test_data_path,
    backend: Literal["ixmp4", "jdbc"],
) -> Generator[Platform, Any, None]:
    """An empty :class:`Platform` connected to a temporary, in-memory database.

    This fixture has **function** scope: the same Platform is reused for one test
    function.

    See also
    --------
    test_mp
    """
    yield from _platform_fixture(request, tmp_env, test_data_path, backend=backend)


# NOTE No type hint for Python 3.9 compliance
@pytest.fixture
def ixmp4_backend(test_mp: Platform):
    from ixmp.backend.ixmp4 import IXMP4Backend

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


# NOTE No type hint for Python 3.9 compliance
@pytest.fixture
def run(ixmp4_backend, scenario: Scenario):
    return ixmp4_backend.index[scenario]


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

    # Construct positional and keyword arguments to Config.add_platform()
    if backend == "jdbc":
        args = ["hsqldb"]
        kwargs: dict[str, Any] = dict(url=f"jdbc:hsqldb:mem:{platform_name}")
    elif backend == "ixmp4":
        args = []
        kwargs = dict(
            ixmp4_name=platform_name, dsn="sqlite:///:memory:", jdbc_compat=True
        )

    # Add platform to ixmp configuration
    ixmp_config.add_platform(platform_name, backend, *args, **kwargs)

    # Launch Platform
    mp = Platform(name=platform_name)

    yield mp

    # Teardown: don't show log messages when destroying the platform, even if the test
    # using the fixture modified the log level
    mp._backend.set_log_level(logging.CRITICAL)

    del mp

    # Remove from configuration
    ixmp_config.remove_platform(platform_name)

    if backend == "ixmp4":
        import ixmp4.conf
        from ixmp4.core.exceptions import PlatformNotFound

        try:
            ixmp4.conf.settings.toml.remove_platform(key=platform_name)
        except PlatformNotFound:
            # This occurs e.g. if `mp` was not used in a way that triggered addition of
            # the name & DSN to the ixmp4 configuration.
            pass
