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
     assert_qty_allclose
     assert_qty_equal

- Methods for setting up and populating test ixmp databases:

  .. autosummary::
     make_dantzig
     create_test_platform
     populate_test_platform

- Methods to run and retrieve values from Jupyter notebooks:

  .. autosummary::
     run_notebook
     get_cell_output

"""
import contextlib
import logging
import os
from collections import namedtuple
from contextlib import contextmanager
from copy import deepcopy
from itertools import chain, product
from math import ceil

try:
    import resource

    has_resource_module = True
except ImportError:
    # Windows
    has_resource_module = False
import shutil
import sys

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from . import cli
from . import config as ixmp_config
from .core import IAMC_IDX, Platform, Scenario, TimeSeries
from .reporting import Quantity
from .reporting.testing import assert_qty_allclose, assert_qty_equal  # noqa: F401

log = logging.getLogger(__name__)

models = {
    "dantzig": {
        "model": "canning problem",
        "scenario": "standard",
    },
}


# pytest hooks and fixtures


def pytest_sessionstart(session):
    """Unset any configuration read from the user's directory."""
    ixmp_config.clear()
    # Further clear an automatic reference to the user's home directory.
    # See fixture tmp_env below
    ixmp_config.values["platform"]["local"].pop("path")


def pytest_report_header(config, startdir):
    """Add the ixmp configuration to the pytest report header."""
    return f"ixmp config: {repr(ixmp_config.values)}"


@pytest.fixture(scope="session")
def ixmp_cli(tmp_env):
    """A CliRunner object that invokes the ixmp command-line interface."""

    class Runner(CliRunner):
        def invoke(self, *args, **kwargs):
            return super().invoke(cli.main, *args, env=tmp_env, **kwargs)

    yield Runner()


@pytest.fixture(params=["AttrSeries", "SparseDataArray"])
def parametrize_quantity_class(request):
    """Fixture to run tests twice, for both reporting Quantity classes."""
    pre = Quantity.CLASS

    Quantity.CLASS = request.param
    yield

    Quantity.CLASS = pre


@pytest.fixture
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


@pytest.fixture(scope="session")
def tmp_env(tmp_path_factory):
    """Return the os.environ dict with the IXMP_DATA variable set.

    IXMP_DATA will point to a temporary directory that is unique to the
    test session. ixmp configuration (i.e. the 'config.json' file) can be
    written and read in this directory without modifying the current user's
    configuration.
    """
    base_temp = tmp_path_factory.getbasetemp()
    os.environ["IXMP_DATA"] = str(base_temp)

    # Set the path for the default/local platform in the test directory
    localdb = base_temp / "localdb" / "default"
    ixmp_config.values["platform"]["local"]["path"] = localdb

    # Save for other processes
    ixmp_config.save()

    yield os.environ


@pytest.fixture(scope="class")
def test_mp(request, tmp_env, test_data_path):
    """An empty ixmp.Platform connected to a temporary, in-memory database."""
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace("/", " ")

    # Add a platform
    ixmp_config.add_platform(
        platform_name, "jdbc", "hsqldb", url=f"jdbc:hsqldb:mem:{platform_name}"
    )

    # Launch Platform
    mp = Platform(name=platform_name)
    yield mp

    # Teardown: don't show log messages when destroying the platform, even if
    # the test using the fixture modified the log level
    mp._backend.set_log_level(logging.CRITICAL)
    del mp

    # Remove from config
    ixmp_config.remove_platform(platform_name)


def bool_param_id(name):
    """Parameter ID callback for :meth:`pytest.mark.parametrize`.

    This formats a boolean value as 'name0' (False) or 'name1' (True) for
    easier selection with e.g. ``pytest -k 'name0'``.
    """
    return lambda value: "{}{}".format(name, int(value))


# Create and populate ixmp databases

MODEL = "canning problem"
SCENARIO = "standard"
HIST_DF = pd.DataFrame(
    [[MODEL, SCENARIO, "DantzigLand", "GDP", "USD", 850.0, 900.0, 950.0]],
    columns=IAMC_IDX + [2000, 2005, 2010],
)
INP_DF = pd.DataFrame(
    [[MODEL, SCENARIO, "DantzigLand", "Demand", "cases", 850.0, 900.0]],
    columns=IAMC_IDX + [2000, 2005],
)
TS_DF = pd.concat([HIST_DF, INP_DF], sort=False)
TS_DF.sort_values(by="variable", inplace=True)
TS_DF.index = range(len(TS_DF.index))


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


def populate_test_platform(platform):
    """Populate *platform* with data for testing.

    Many of the tests in test_core.py depend on this set of data.

    The data consist of:

    - 3 versions of the Dantzig cannery/transport Scenario.

      - Version 2 is the default.
      - All have :obj:`HIST_DF` and :obj:`TS_DF` as time-series data.

    - 1 version of a TimeSeries with model name 'Douglas Adams' and scenario
      name 'Hitchhiker', containing 2 values.
    """
    s1 = make_dantzig(platform, solve=True)

    s2 = s1.clone()
    s2.set_as_default()

    s2.clone()

    s4 = TimeSeries(platform, "Douglas Adams", "Hitchhiker", version="new")
    s4.add_timeseries(
        pd.DataFrame.from_dict(
            dict(
                region="World",
                variable="Testing",
                unit="???",
                year=[2010, 2020],
                value=[23.7, 23.8],
            )
        )
    )
    s4.commit("")
    s4.set_as_default()


def make_dantzig(mp, solve=False):
    """Return :class:`ixmp.Scenario` of Dantzig's canning/transport problem.

    Parameters
    ----------
    mp : ixmp.Platform
        Platform on which to create the scenario.
    solve : bool or os.PathLike
        If not :obj:`False`, then *solve* is interpreted as a path to a
        directory, and the model ``transport_ixmp.gms`` in the directory is run
        for the scenario.

    See also
    --------
    .DantzigModel
    """
    # add custom units and region for timeseries data
    try:
        mp.add_unit("USD/km")
    except Exception:
        # Unit already exists. Pending bugfix from zikolach
        pass
    mp.add_region("DantzigLand", "country")

    # Initialize a new Scenario, and use the DantzigModel class' initialize()
    # method to populate it
    annot = "Dantzig's transportation problem for illustration and testing"
    scen = Scenario(
        mp,
        **models["dantzig"],
        version="new",
        annotation=annot,
        scheme="dantzig",
        with_data=True,
    )

    # commit the scenario
    scen.commit("Import Dantzig's transport problem for testing.")

    # set this new scenario as the default version for the model/scenario name
    scen.set_as_default()

    if solve:
        # Solve the model using the GAMS code provided in the `tests` folder
        scen.solve(model="dantzig", case="transport_standard")

    # add timeseries data for testing `clone(keep_solution=False)`
    # and `remove_solution()`
    scen.check_out(timeseries_only=True)
    scen.add_timeseries(HIST_DF, meta=True)
    scen.add_timeseries(INP_DF)
    scen.commit("Import Dantzig's transport problem for testing.")

    return scen


# Run and check values from Jupyter notebook

nbformat = pytest.importorskip("nbformat")


def run_notebook(nb_path, tmp_path, env=None, kernel=None, allow_errors=False):
    """Execute a Jupyter notebook via :mod:`nbclient` and collect output.

    Parameters
    ----------
    nb_path : path-like
        The notebook file to execute.
    tmp_path : path-like
        A directory in which to create temporary output.
    env : dict-like, optional
        Execution environment for ``nbconvert``.
        If not supplied, :obj:`os.environ` is used.
    kernel : str, optional
        Jupyter kernel to use. Default: 'python2' or 'python3', matching the
        current Python version.
    allow_errors : bool, optional
        Whether to pass the ``--allow-errors`` option to ``nbconvert``. If
        :obj:`True`, the execution always succeeds, and cell output contains
        exception information.

    Returns
    -------
    nb : :class:`nbformat.NotebookNode`
        Parsed and executed notebook.
    errors : list
        Any execution errors.
    """
    import nbformat
    from nbclient import NotebookClient

    # Workaround for https://github.com/jupyter/nbclient/issues/85
    if (
        sys.version_info[0] == 3
        and sys.version_info[1] >= 8
        and sys.platform.startswith("win")
    ):
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Read the notebook
    with open(nb_path, encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    # Create a client and use it to execute the notebook
    client = NotebookClient(
        nb,
        timeout=60,
        kernel_name=kernel or f"python{sys.version_info[0]}",
        allow_errors=allow_errors,
        resources=dict(metadata=dict(path=tmp_path)),
    )

    # Execute the notebook.
    # `env` is passed from nbclient to jupyter_client.launcher.launch_kernel()
    client.execute(env=env or os.environ.copy())

    # Retrieve error information from cells
    errors = [
        output
        for cell in nb.cells
        if "outputs" in cell
        for output in cell["outputs"]
        if output.output_type == "error"
    ]

    return nb, errors


def get_cell_output(nb, name_or_index, kind="data"):
    """Retrieve a cell from *nb* according to its metadata *name_or_index*:

    The Jupyter notebook format allows specifying a document-wide unique 'name'
    metadata attribute for each cell:

    https://nbformat.readthedocs.io/en/latest/format_description.html
    #cell-metadata

    Return the cell matching *name_or_index* if a string; or the cell at the
    int index; or raise ValueError.

    Parameters
    ----------
    kind : str, optional
        Kind of cell output to retrieve. For 'data', the data in format
        'text/plain' is run through :func:`eval`. To retrieve an exception
        message, use 'evalue'.
    """
    if isinstance(name_or_index, int):
        cell = nb.cells[name_or_index]
    else:
        for i, _cell in enumerate(nb.cells):
            try:
                if _cell.metadata.jupyter.name == name_or_index:
                    cell = _cell
                    break
            except AttributeError:
                continue

    try:
        result = cell["outputs"][0][kind]
    except NameError:  # pragma: no cover
        raise ValueError(f"no cell named {name_or_index}")
    else:
        return eval(result["text/plain"]) if kind == "data" else result


# Assertions for testing


@contextmanager
def assert_logs(caplog, message_or_messages=None, at_level=None):
    """Assert that *message_or_messages* appear in logs.

    Use assert_logs as a context manager for a statement that is expected to
    trigger certain log messages. assert_logs checks that these messages are
    generated.

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
        Messages must appear on 'ixmp' or a sub-logger with at least this
        level.
    """
    # Wrap a string in a list
    expected = (
        [message_or_messages]
        if isinstance(message_or_messages, str)
        else message_or_messages
    )

    # Record the number of records prior to the managed block
    first = len(caplog.records)

    if at_level is not None:
        # Use the pytest caplog fixture's built-in context manager to
        # temporarily set the level of the 'ixmp' logger
        ctx = caplog.at_level(at_level, logger="ixmp")
    else:
        # Python 3.6 compatibility: use suppress for nullcontext
        nullcontext = getattr(contextlib, "nullcontext", contextlib.suppress)
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


# Data structure for memory information used by :meth:`memory_usage`.
MemInfo = namedtuple(
    "MemInfo",
    [
        "profiled",
        "max_rss",
        "jvm_total",
        "jvm_free",
        "jvm_used",
        "python",
    ],
)


def format_meminfo(arr, cls=float):
    """Return a namedtuple for *array*, with values as *cls*."""
    # Format strings
    cls = "{: >7.2f}".format if cls is str else cls
    return MemInfo(*map(cls, arr))


# Variables for memory_usage
_COUNT = 0
_PREV = np.zeros(6)
_RT = None


def memory_usage(message="", reset=False):
    """Profile memory usage from within a test function.

    The Python package ``memory_profiler`` and :mod:`jpype` are used to report
    memory usage. A message is logged at the ``DEBUG`` level, similar to::

       DEBUG    ixmp.testing:testing.py:527  42 <message>
       MemInfo(profiled=' 533.76', max_rss=' 534.19', jvm_total=' 213.50',
               jvm_free='  79.22', jvm_used=' 134.28', python='399.48')
       MemInfo(profiled='   0.14', max_rss='   0.00', jvm_total='   0.00',
               jvm_free=' -37.75', jvm_used='  37.75', python=' -37.61')

    Parameters
    ----------
    message : str, optional
        A string added to the log message, to aid in identifying points in
        profiled code.
    reset : bool, optional
        If :obj:`True`, start profiling anew.

    Returns
    -------
    collections.namedtuple
        A ``MemInfo`` tuple with the following fields, all in MiB:

        - ``profiled``: the instantaneous memory usage reported by
          memory_profiler.
        - ``max_rss``: the maximum resident set size (i.e. the maximum over
          the entire life of the process) reported by
          :meth:`resource.getrusage`.
        - ``jvm_total``: total memory allocated for the Java Virtual Machine
          (JVM) underlying JPype, used by :class:`.JDBCBackend`; the same as
          ``java.lang.Runtime.getRuntime().totalMemory()``.
        - ``jvm_free``: memory allocated to the JVM that is free.
        - ``jvm_used``: memory used by the JVM, i.e. ``jvm_total`` minus
          ``jvm_free``.
        - ``python``: a rough estimate of Python memory usage, i.e.
          ``profiled`` minus ``jvm_used``. This may not be accurate.
    """
    import memory_profiler

    from ixmp.backend.jdbc import java

    global _COUNT, _PREV, _RT

    if reset:
        _COUNT = 0
        _PREV = np.zeros(6)
    else:
        _COUNT += 1

    try:
        # Get the Java runtime
        runtime = _RT or java.Runtime.getRuntime()
    except AttributeError:
        # JVM not loaded
        runtime = None

    # Collect data
    result = [
        memory_profiler.memory_usage()[0],
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,  # to MiB
    ]
    if runtime:
        _RT = runtime
        result.extend(
            [
                _RT.totalMemory() / 1024 ** 2,
                _RT.freeMemory() / 1024 ** 2,
            ]
        )
    else:
        result.extend([0, 0])

    # JVM total - JVM free = JVM used
    result.append(result[-2] - result[-1])

    # Python used - JVM used = only-Python used?
    result.append(result[0] - result[-1])

    # Convert to a numpy array
    result = np.array(result)

    # Difference versus previous call
    delta = result - _PREV

    # Store current *result* to compute *delta* in subsequent calls
    _PREV = result

    # Log the results
    msg = "\n".join(
        [
            f"{_COUNT:3d} {message}",
            repr(format_meminfo(result, str)),
            repr(format_meminfo(delta, str)),
        ]
    )
    log.debug(msg)

    # Return the current result
    return format_meminfo(result)


def random_ts_data(length):
    """A :class:`pandas.DataFrame` of time series data with *length* rows.

    Suitable for passage to :meth:`TimeSeries.add_timeseries`.
    """
    return pd.DataFrame.from_dict(
        dict(
            region="World",
            variable=[f"foo|{i}" for i in range(int(length))],
            year=2020,
            value=np.random.rand(int(length)),
            unit="GWa",
        )
    )


def add_random_model_data(scenario, length):
    """Add a set and parameter with given *length* to *scenario*.

    The set is named 'random_set'. The parameter is named 'random_par', and
    has two dimensions indexed by 'random_set'.
    """
    set_data, par_data = random_model_data(length)
    scenario.init_set("random_set")
    scenario.add_set("random_set", set_data)
    scenario.init_par(
        "random_par",
        idx_sets=["random_set", "random_set"],
        idx_names=["random_set0", "random_set1"],
    )
    scenario.add_par("random_par", par_data)
    return len(par_data)


def random_model_data(length):
    """Random (set, parameter) data with at least *length* elements.

    See also
    --------
    add_random_model_data
    """
    # Dimension size
    dim_len = ceil(length ** 0.5)
    set_data = list(str(i) for i in range(dim_len))

    # Revised length, possibly slightly higher than original
    length = dim_len ** 2

    par_data = pd.concat(
        [
            pd.DataFrame.from_dict(
                dict(region="World", value=np.random.rand(length), unit="GWa")
            ),
            pd.DataFrame(
                data=product(set_data, set_data), columns=["random_set0", "random_set1"]
            ),
        ],
        axis=1,
    )

    return set_data, par_data


@pytest.fixture(scope="function")
def resource_limit(request):
    """A fixture that limits Python :mod:`resources`.

    See the documentation (``pytest --help``) for the ``--resource-limit``
    command-line option that selects (1) the specific resource and (2) the
    level of the limit.

    The original limit, if any, is reset after the test function in which the
    fixture is used.
    """
    if not has_resource_module:
        pytest.skip("Python module 'resource' not available (non-Unix OS)")

    name, value = request.config.getoption("--resource-limit").split(":")
    res = getattr(resource, f"RLIMIT_{name.upper()}")
    value = int(value)

    if res in (resource.RLIMIT_AS, resource.RLIMIT_DATA, resource.RLIMIT_RSS):
        value = value * 1024 ** 2  # MiB → bytes

    if value > 0:
        # Store existing limit
        before = resource.getrlimit(res)

        log.debug(f"Change {res} from {before} to ({value}, {before[1]})")
        resource.setrlimit(res, (value, before[1]))

    try:
        yield
    finally:
        if value > 0:
            log.debug(f"Restore {res} to {before}")
            resource.setrlimit(res, before)
