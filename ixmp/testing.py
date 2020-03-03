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
     populate_test_platform

- Methods to run and retrieve values from Jupyter notebooks:

  .. autosummary::
     run_notebook
     get_cell_output

"""
from contextlib import contextmanager
import io
import os
import subprocess
import sys

from click.testing import CliRunner
import pandas as pd
from pandas.testing import assert_series_equal
import pytest

from . import cli, config as ixmp_config
from .core import Platform, TimeSeries, Scenario, IAMC_IDX


models = {
    'dantzig': {
        'model': 'canning problem',
        'scenario': 'standard',
    },
}


# pytest hooks and fixtures


def pytest_sessionstart(session):
    """Unset any configuration read from the user's directory."""
    ixmp_config.clear()
    # Further clear an automatic reference to the user's home directory.
    # See fixture tmp_env below
    ixmp_config.values['platform']['local'].pop('path')


def pytest_report_header(config, startdir):
    """Add the ixmp configuration to the pytest report header."""
    return 'ixmp config: {!r}'.format(ixmp_config.values)


@pytest.fixture(scope='session')
def ixmp_cli(tmp_env):
    """A CliRunner object that invokes the ixmp command-line interface."""
    class Runner(CliRunner):
        def invoke(self, *args, **kwargs):
            return super().invoke(cli.main, *args, env=tmp_env, **kwargs)

    yield Runner()


@pytest.fixture(scope='session')
def tmp_env(tmp_path_factory):
    """Return the os.environ dict with the IXMP_DATA variable set.

    IXMP_DATA will point to a temporary directory that is unique to the
    test session. ixmp configuration (i.e. the 'config.json' file) can be
    written and read in this directory without modifying the current user's
    configuration.
    """
    base_temp = tmp_path_factory.getbasetemp()
    os.environ['IXMP_DATA'] = str(base_temp)

    # Set the path for the default/local platform in the test directory
    localdb = base_temp / 'localdb' / 'default'
    ixmp_config.values['platform']['local']['path'] = localdb

    # Save for other processes
    ixmp_config.save()

    yield os.environ


@pytest.fixture(scope='class')
def test_mp(request, tmp_env, test_data_path):
    """An empty ixmp.Platform connected to a temporary, in-memory database."""
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace('/', ' ')

    # Add a platform
    ixmp_config.add_platform(platform_name, 'jdbc', 'hsqldb',
                             url=f'jdbc:hsqldb:mem:{platform_name}')

    # Launch Platform
    yield Platform(name=platform_name)

    # Teardown: remove from config
    ixmp_config.remove_platform(platform_name)


# Create and populate ixmp databases

MODEL = "canning problem"
SCENARIO = "standard"
HIST_DF = pd.DataFrame(
    [[MODEL, SCENARIO, 'DantzigLand', 'GDP', 'USD', 850., 900., 950.], ],
    columns=IAMC_IDX + [2000, 2005, 2010],
)
INP_DF = pd.DataFrame(
    [[MODEL, SCENARIO, 'DantzigLand', 'Demand', 'cases', 850., 900.], ],
    columns=IAMC_IDX + [2000, 2005],
)
TS_DF = pd.concat([HIST_DF, INP_DF], sort=False)
TS_DF.sort_values(by='variable', inplace=True)
TS_DF.index = range(len(TS_DF.index))


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

    s4 = TimeSeries(platform, 'Douglas Adams', 'Hitchhiker', version='new')
    s4.add_timeseries(pd.DataFrame.from_dict(dict(
        region='World', variable='Testing', unit='???', year=[2010, 2020],
        value=[23.7, 23.8])))
    s4.commit('')
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
        mp.add_unit('USD/km')
    except Exception:
        # Unit already exists. Pending bugfix from zikolach
        pass
    mp.add_region('DantzigLand', 'country')

    # Initialize a new Scenario, and use the DantzigModel class' initialize()
    # method to populate it
    annot = "Dantzig's transportation problem for illustration and testing"
    scen = Scenario(mp, **models['dantzig'], version='new', annotation=annot,
                    scheme='dantzig', with_data=True)

    # commit the scenario
    scen.commit("Import Dantzig's transport problem for testing.")

    # set this new scenario as the default version for the model/scenario name
    scen.set_as_default()

    if solve:
        # Solve the model using the GAMS code provided in the `tests` folder
        scen.solve(model='dantzig', case='transport_standard')

    # add timeseries data for testing `clone(keep_solution=False)`
    # and `remove_solution()`
    scen.check_out(timeseries_only=True)
    scen.add_timeseries(HIST_DF, meta=True)
    scen.add_timeseries(INP_DF)
    scen.commit("Import Dantzig's transport problem for testing.")

    return scen


# Run and check values from Jupyter notebook

nbformat = pytest.importorskip('nbformat')


def run_notebook(nb_path, tmp_path, env=None, kernel=None):
    """Execute a Jupyter notebook via ``nbconvert`` and collect output.

    Modified from
    https://blog.thedataincubator.com/2016/06/testing-jupyter-notebooks/

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

    Returns
    -------
    nb : :class:`nbformat.NotebookNode`
        Parsed and executed notebook.
    errors : list
        Any execution errors.
    """
    # Process arguments
    env = env or os.environ
    major_version = sys.version_info[0]
    kernel = kernel or 'python{}'.format(major_version)

    os.chdir(nb_path.parent)
    fname = tmp_path / 'test.ipynb'
    args = [
        "jupyter", "nbconvert", "--to", "notebook", "--execute",
        "--ExecutePreprocessor.timeout=60",
        "--ExecutePreprocessor.kernel_name={}".format(kernel),
        "--output", str(fname), str(nb_path)]
    subprocess.check_call(args, env=env)

    nb = nbformat.read(io.open(fname, encoding='utf-8'),
                       nbformat.current_nbformat)

    errors = [
        output for cell in nb.cells if "outputs" in cell
        for output in cell["outputs"] if output.output_type == "error"
    ]

    fname.unlink()

    return nb, errors


def get_cell_output(nb, name_or_index):
    """Retrieve a cell from *nb* according to its metadata *name_or_index*:

    The Jupyter notebook format allows specifying a document-wide unique 'name'
    metadata attribute for each cell:

    https://nbformat.readthedocs.io/en/latest/format_description.html
    #cell-metadata

    Return the cell matching *name_or_index* if a string; or the cell at the
    int index; or raise ValueError.
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
        return eval(cell['outputs'][0]['data']['text/plain'])
    except NameError:
        raise ValueError('no cell named {!r}'.format(name_or_index))


# Assertions for testing


@contextmanager
def assert_logs(caplog, message_or_messages=None):
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
    """
    # Wrap a string in a list
    expected = [message_or_messages] if isinstance(message_or_messages, str) \
        else message_or_messages

    # Record the number of records prior to the managed block
    first = len(caplog.records)

    try:
        yield  # Nothing provided to the managed block
    finally:
        found = [any(e in msg for msg in caplog.messages[first:])
                 for e in expected]
        if not all(found):
            missing = [msg for i, msg in enumerate(expected) if not found[i]]
            raise AssertionError(f'Did not log {missing}')


def assert_qty_equal(a, b, check_attrs=True, **kwargs):
    """Assert that Quantity objects *a* and *b* are equal.

    When Quantity is AttrSeries, *a* and *b* are first passed through
    :meth:`as_quantity`.
    """
    from xarray import DataArray
    from xarray.testing import assert_equal as assert_xr_equal

    from .reporting.quantity import AttrSeries, Quantity, as_quantity

    if Quantity is AttrSeries:
        # Convert pd.Series automatically
        a = as_quantity(a) if isinstance(a, (pd.Series, DataArray)) else a
        b = as_quantity(b) if isinstance(b, (pd.Series, DataArray)) else b

        assert_series_equal(a, b, check_dtype=False, **kwargs)
    elif Quantity is DataArray:  # pragma: no cover
        assert_xr_equal(a, b, **kwargs)

    # check attributes are equal
    if check_attrs:
        assert a.attrs == b.attrs


def assert_qty_allclose(a, b, check_attrs=True, **kwargs):
    """Assert that Quantity objects *a* and *b* have numerically close values.

    When Quantity is AttrSeries, *a* and *b* are first passed through
    :meth:`as_quantity`.
    """
    from xarray import DataArray
    from xarray.testing import assert_allclose as assert_xr_allclose

    from .reporting.quantity import AttrSeries, Quantity, as_quantity

    if Quantity is AttrSeries:
        # Convert pd.Series automatically
        a = as_quantity(a) if isinstance(a, (pd.Series, DataArray)) else a
        b = as_quantity(b) if isinstance(b, (pd.Series, DataArray)) else b

        assert_series_equal(a, b, **kwargs)
    elif Quantity is DataArray:  # pragma: no cover
        kwargs.pop('check_dtype', None)
        assert_xr_allclose(a, b, **kwargs)

    # check attributes are equal
    if check_attrs:
        assert a.attrs == b.attrs
