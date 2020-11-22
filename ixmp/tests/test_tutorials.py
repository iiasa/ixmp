import numpy as np
import pytest

from ixmp.testing import get_cell_output, run_notebook


def test_py_transport(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "py_transport.ipynb"
    nb, errors = run_notebook(fname, tmp_path, tmp_env)
    assert errors == []

    # FIXME use get_cell_by_name instead of assuming cell count/order is fixed
    assert np.isclose(get_cell_output(nb, -5)["lvl"], 153.6750030517578)


def test_py_transport_scenario(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "py_transport_scenario.ipynb"
    nb, errors = run_notebook(fname, tmp_path, tmp_env)
    assert errors == []

    assert np.isclose(get_cell_output(nb, "scen-z")["lvl"], 153.675)
    assert np.isclose(get_cell_output(nb, "scen-detroit-z")["lvl"], 161.324)


@pytest.mark.rixmp
def test_R_transport(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "R_transport.ipynb"
    nb, errors = run_notebook(fname, tmp_path, tmp_env, kernel="IR")
    assert errors == []


@pytest.mark.rixmp
def test_R_transport_scenario(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "R_transport_scenario.ipynb"
    nb, errors = run_notebook(fname, tmp_path, tmp_env, kernel="IR")
    assert errors == []
