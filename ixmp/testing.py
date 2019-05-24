"""Utilities for testing ixmp.

These include:

- pytest fixtures: tmp_env, test_mp, and test_mp_props.
- Methods for setting up and populating test ixmp databases:
  create_local_testdb() and dantzig_transport()
- Methods to run and retrieve values from Jupyter notebooks:
  run_notebook() and get_cell_output()

"""
import io
import os
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
import shutil
import sys
import subprocess

import pandas as pd
import pytest

from .core import Scenario


# pytest fixtures

@pytest.fixture(scope='session')
def tmp_env(tmp_path_factory):
    """Return the os.environ dict with the IXMP_DATA variable set.

    IXMP_DATA will point to a temporary directory that is unique to the
    test session. ixmp configuration (i.e. the 'config.json' file) can be
    written and read in this directory without modifying the current user's
    configuration.
    """
    os.environ['IXMP_DATA'] = str(tmp_path_factory.mktemp('config'))

    yield os.environ


@pytest.fixture(scope='session')
def test_mp(tmp_path_factory, test_data_path):
    """An ixmp.Platform connected to a temporary, local database.

    *test_mp* is used across the entire test session, so the contents of the
    database may reflect other tests already run.
    """
    import ixmp

    # casting to Path(str()) is a hotfix due to errors upstream in pytest on
    # Python 3.5 (at least, perhaps others), there is an implicit cast to
    # python2's pathlib which is incompatible with python3's pathlib Path
    # objects.  This can be taken out once it is resolved upstream and CI is
    # setup on multiple Python3.x distros.
    db_path = Path(str(tmp_path_factory.mktemp('test_mp')))
    test_props = create_local_testdb(db_path, test_data_path / 'testdb')

    # launch Platform and connect to testdb (reconnect if closed)
    mp = ixmp.Platform(test_props)
    mp.open_db()

    yield mp


@pytest.fixture(scope='session')
def test_mp_props(tmp_path_factory, test_data_path):
    """Path to a database properties file referring to a test database."""
    # casting to Path(str()) is a hotfix due to errors upstream in pytest on
    # Python 3.5 (at least, perhaps others), there is an implicit cast to
    # python2's pathlib which is incompatible with python3's pathlib Path
    # objects.  This can be taken out once it is resolved upstream and CI is
    # setup on multiple Python3.x distros.
    db_path = Path(str(tmp_path_factory.mktemp('test_mp_props')))
    test_props = create_local_testdb(db_path, test_data_path / 'testdb')

    yield test_props


# Create and populate ixmp databases

MODEL = "canning problem"
SCENARIO = "standard"
TS_DF = pd.DataFrame(
    [[MODEL, SCENARIO, 'DantzigLand', 'Demand', 'cases', 900], ],
    columns=['model', 'scenario', 'region', 'variable', 'unit', 2005],
)


def create_local_testdb(db_path, data_path):
    """Create a local database for testing in the directory *db_path*.

    Returns the path to a database properties file in the directory. Contents
    are copied from *data_path*.
    """
    # Copy test database
    dst = db_path / 'testdb'
    # str() here is for py2 compatibility
    shutil.copytree(str(data_path), str(dst))

    # Create properties file
    props = (data_path / 'test.properties_template').read_text()
    test_props = dst / 'test.properties'
    test_props.write_text(props.format(here=str(dst).replace("\\", "/")))

    return test_props


def dantzig_transport(mp, solve=False):
    """Define and optionally solve Dantzig's transport problem.

    Parameters
    ----------
    mp: :class:`ixmp.Platform`
        Platform to add the
    solve: False or path-like
        If not False, then *solve* is interpreted as a path, and the model
        'transport_ixmp' in the indicated directory is run on the Scenario.

    Returns
    -------
    :class:`ixmp.Scenario`
        A scenario containing the transport problem.
    """
    # add custom units and region for timeseries data
    mp.add_unit('USD_per_km')
    mp.add_region('DantzigLand', 'country')

    # initialize a new (empty) instance of an `ixmp.Scenario`
    model = 'canning problem'
    scenario = 'standard'
    annot = "Dantzig's transportation problem for illustration and testing"
    scen = Scenario(mp, model, scenario, version='new', annotation=annot)

    # define sets
    scen.init_set('i')
    scen.add_set('i', ['seattle', 'san-diego'])
    scen.init_set('j')
    scen.add_set('j', ['new-york', 'chicago', 'topeka'])

    # capacity of plant i in cases
    # add parameter elements one-by-one (string and value)
    scen.init_par('a', idx_sets='i')
    scen.add_par('a', 'seattle', 350, 'cases')
    scen.add_par('a', 'san-diego', 600, 'cases')

    # demand at market j in cases
    # add parameter elements as dataframe (with index names)
    scen.init_par('b', idx_sets='j')
    b_data = pd.DataFrame([
        ['new-york', 325, 'cases'],
        ['chicago', 300, 'cases'],
        ['topeka', 275, 'cases'],
    ], columns=['j', 'value', 'unit'])
    scen.add_par('b', b_data)

    # distance in thousands of miles
    # add parameter elements as dataframe (with index names)
    scen.init_par('d', idx_sets=['i', 'j'])
    d_data = pd.DataFrame([
        ['seattle', 'new-york', 2.5, 'km'],
        ['seattle', 'chicago', 1.7, 'km'],
        ['seattle', 'topeka', 1.8, 'km'],
        ['san-diego', 'new-york', 2.5, 'km'],
        ['san-diego', 'chicago', 1.8, 'km'],
        ['san-diego', 'topeka', 1.4, 'km'],
    ], columns='i j value unit'.split())
    scen.add_par('d', d_data)

    # cost per case per 1000 miles
    # initialize scalar with a value and a unit
    scen.init_scalar('f', 90.0, 'USD_per_km')

    # initialize the decision variables and equations
    scen.init_var('z', None, None)
    scen.init_var('x', idx_sets=['i', 'j'])
    scen.init_equ('demand', idx_sets=['j'])

    # add timeseries data for testing the clone across platforms
    scen.add_timeseries(TS_DF)

    scen.commit("Import Dantzig's transport problem for testing.")

    # set this new scenario as the default version for the model/scenario name
    scen.set_as_default()

    if solve:
        # Solve the model using the GAMS code provided in the `tests` folder
        scen.solve(model=str(solve / 'transport_ixmp'),
                   case='transport_standard')

    return scen


# Run and check values from Jupyter notebook

nbformat = pytest.importorskip('nbformat')


def run_notebook(nb_path, tmp_path, env=os.environ, kernel=None):
    """Execute a Jupyter notebook via nbconvert and collect output.

    Modified from
    https://blog.thedataincubator.com/2016/06/testing-jupyter-notebooks/

    Parameters
    ----------
    nb_path : path-like
        The notebook file to execute.
    tmp_path : path-like
        A directory in which to create temporary output.
    env : dict-like
        Execution environment for `nbconvert`.
    kernel : str
        Jupyter kernel to use. Default: `python2` or `python3`, matching the
        current Python version.

    Returns
    -------
    nb : :class:`nbformat.NotebookNode`
        Parsed and executed notebook.
    errors : list
        Any execution errors.
    """
    major_version = sys.version_info[0]
    kernel = kernel or 'python{}'.format(major_version)
    # str() here is for python2 compatibility
    os.chdir(str(nb_path.parent))
    fname = tmp_path / 'test.ipynb'
    args = [
        "jupyter", "nbconvert", "--to", "notebook", "--execute",
        "--ExecutePreprocessor.timeout=60",
        "--ExecutePreprocessor.kernel_name={}".format(kernel),
        "--output", str(fname), str(nb_path)]
    subprocess.check_call(args, env=env)

    # str() here is for python2 compatibility
    nb = nbformat.read(io.open(str(fname), encoding='utf-8'),
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
        for i, cell in enumerate(nb.cells):
            try:
                cell_name = cell.metadata.jupyter.name
                if cell_name == name_or_index:
                    break
            except AttributeError:
                continue

        raise ValueError('no cell named {!r}'.format(name_or_index))

    return eval(cell['outputs'][0]['data']['text/plain'])
