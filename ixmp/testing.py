"""Utilities for testing ixmp.

These include:

- pytest hooks, fixtures:

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
     create_local_testdb
     make_dantzig

- Methods to run and retrieve values from Jupyter notebooks:

  .. autosummary::
     run_notebook
     get_cell_output

"""
from contextlib import contextmanager
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys

from click.testing import CliRunner
import pandas as pd
from pandas.testing import assert_series_equal
import pytest

from . import cli, config as ixmp_config
from .core import Platform, Scenario, IAMC_IDX


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


@pytest.fixture(scope='function')
def test_mp(request, tmp_env, test_data_path):
    """An ixmp.Platform connected to a temporary, local database."""
    yield from create_test_mp(request, test_data_path, 'ixmptest')


def create_test_mp(request, path, name):
    # Name of the test function, without the preceding 'test_'
    dirname = request.node.name.split('test_', 1)[1]
    # Long, unique name for the platform.
    # Remove '/' so that the name can be used in URL tests.
    platform_name = request.node.nodeid.replace('/', ' ')

    # Path to the database
    db_path = Path(os.environ['IXMP_DATA']) / 'localdb' / dirname
    db_path.parent.mkdir(exist_ok=True)

    # Create the database
    create_local_testdb(db_path, path / 'testdb', name)

    # Add a platform
    ixmp_config.add_platform(platform_name, 'jdbc', 'hsqldb', db_path)

    # launch Platform and connect to testdb (reconnect if closed)
    mp = Platform(name=platform_name)
    mp.open_db()

    yield mp

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


def create_local_testdb(db_path, data_path, name='ixmptest'):
    """Create a local database for testing at *db_path*.

    The files {name}.lobs and {name}.script are copied and renamed from
    *data_path*.
    """
    for suffix in '.lobs', '.script':
        # NB explicit Path(...) here is necessary because this function is
        #    called directly from rixmp; see conftest.R
        src = (Path(data_path) / name).with_suffix(suffix)
        dst = Path(db_path).with_suffix(src.suffix)
        shutil.copyfile(str(src), str(dst))


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
    """
    # add custom units and region for timeseries data
    try:
        mp.add_unit('USD_per_km')
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
        scen.solve(model=str(Path(solve) / 'transport_ixmp'),
                   case='transport_standard')

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
        assert all(any(e in msg for msg in caplog.messages[first:])
                   for e in expected)


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
    elif Quantity is DataArray:
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
    elif Quantity is DataArray:
        kwargs.pop('check_dtype', None)
        assert_xr_allclose(a, b, **kwargs)

    # check attributes are equal
    if check_attrs:
        assert a.attrs == b.attrs
