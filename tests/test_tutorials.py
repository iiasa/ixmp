import numpy as np
import pytest

from ixmp.testing import get_cell_output, run_notebook
from test_r import r_installed


def test_py_transport(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / 'transport' / 'py_transport.ipynb'
    nb, errors = run_notebook(fname, tmp_path, tmp_env)
    assert errors == []

    # FIXME use get_cell_by_name instead of assuming cell count/order is fixed
    assert np.isclose(get_cell_output(nb, -5)['lvl'], 153.6750030517578)


def test_py_transport_scenario(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / 'transport' / 'py_transport_scenario.ipynb'
    nb, errors = run_notebook(fname, tmp_path, tmp_env)
    assert errors == []

    # FIXME use get_cell_by_name instead of assuming cell count/order is fixed
    assert np.isclose(get_cell_output(nb, -9)['lvl'], 153.6750030517578)
    assert np.isclose(get_cell_output(nb, -8)['lvl'], 161.3249969482422)


@pytest.mark.skipif(not r_installed(), reason='requires R to be installed')
def test_R_transport(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / 'transport' / 'R_transport.ipynb'
    nb, errors = run_notebook(fname, tmp_path, tmp_env, kernel='IR')
    assert errors == []


@pytest.mark.skipif(not r_installed(), reason='requires R to be installed')
def test_R_transport_scenario(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / 'transport' / 'R_transport_scenario.ipynb'
    nb, errors = run_notebook(fname, tmp_path, tmp_env, kernel='IR')
    assert errors == []
