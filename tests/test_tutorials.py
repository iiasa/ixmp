import io
import os
import subprocess
import sys
import nbformat
import pytest

import numpy as np

from testing_utils import here

from test_r import r_installed

xport_path = os.path.join(here, '..', 'tutorial', 'transport')

# taken from the execellent example here:
# https://blog.thedataincubator.com/2016/06/testing-jupyter-notebooks/


def _notebook_run(path, kernel=None):
    """Execute a notebook via nbconvert and collect output.
    :returns (parsed nb object, execution errors)
    """
    major_version = sys.version_info[0]
    kernel = kernel or 'python{}'.format(major_version)
    dirname, __ = os.path.split(path)
    os.chdir(dirname)
    fname = os.path.join(here, 'test.ipynb')
    args = [
        "jupyter", "nbconvert", "--to", "notebook", "--execute",
        "--ExecutePreprocessor.timeout=60",
        "--ExecutePreprocessor.kernel_name={}".format(kernel),
        "--output", fname, path]
    subprocess.check_call(args)

    nb = nbformat.read(io.open(fname, encoding='utf-8'),
                       nbformat.current_nbformat)

    errors = [
        output for cell in nb.cells if "outputs" in cell
        for output in cell["outputs"] if output.output_type == "error"
    ]

    os.remove(fname)

    return nb, errors


@pytest.mark.skip_win_ci
def test_py_transport():
    fname = os.path.join(xport_path, 'py_transport.ipynb')
    nb, errors = _notebook_run(fname)
    assert errors == []

    obs = eval(nb.cells[-5]['outputs'][0]['data']['text/plain'])['lvl']
    exp = 153.6750030517578
    assert np.isclose(obs, exp)


@pytest.mark.skip_win_ci
def test_py_transport_scenario():
    fname = os.path.join(xport_path, 'py_transport_scenario.ipynb')
    nb, errors = _notebook_run(fname)
    assert errors == []

    obs = eval(nb.cells[-9]['outputs'][0]['data']['text/plain'])['lvl']
    exp = 153.6750030517578
    assert np.isclose(obs, exp)

    obs = eval(nb.cells[-8]['outputs'][0]['data']['text/plain'])['lvl']
    exp = 161.3249969482422
    assert np.isclose(obs, exp)


@pytest.mark.skipif(not r_installed(), reason='requires R to be installed')
def test_R_transport():
    fname = os.path.join(xport_path, 'R_transport.ipynb')
    nb, errors = _notebook_run(fname, kernel='IR')
    assert errors == []


@pytest.mark.skipif(not r_installed(), reason='requires R to be installed')
def test_R_transport_scenario():
    fname = os.path.join(xport_path, 'R_transport_scenario.ipynb')
    nb, errors = _notebook_run(fname, kernel='IR')
    assert errors == []
