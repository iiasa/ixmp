import os
import platform
import sys

import numpy as np
import pytest

from ixmp.testing import get_cell_output, run_notebook

group_base_name = platform.system() + platform.python_version()

GHA = "GITHUB_ACTIONS" in os.environ


def default_args():
    """Default arguments for :func:`.run_notebook."""
    if GHA:
        # Use a longer timeout
        return dict(timeout=30)
    else:
        return dict()


@pytest.mark.xdist_group(name=f"{group_base_name}-0")
def test_py_transport(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "py_transport.ipynb"
    args = default_args()
    nb, errors = run_notebook(fname, tmp_path, tmp_env, **args)
    assert errors == []

    # FIXME use get_cell_by_name instead of assuming cell count/order is fixed
    assert np.isclose(get_cell_output(nb, -5)["lvl"], 153.6750030517578)


@pytest.mark.xdist_group(name=f"{group_base_name}-0")
def test_py_transport_scenario(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "py_transport_scenario.ipynb"
    args = default_args()
    nb, errors = run_notebook(fname, tmp_path, tmp_env, **args)
    assert errors == []

    assert np.isclose(get_cell_output(nb, "scen-z")["lvl"], 153.675)
    assert np.isclose(get_cell_output(nb, "scen-detroit-z")["lvl"], 161.324)


@pytest.mark.xdist_group(name=f"{group_base_name}-1")
@pytest.mark.rixmp
# TODO investigate and resolve the cause of the time outs; remove this mark
@pytest.mark.skipif(
    "GITHUB_ACTIONS" in os.environ and sys.platform == "linux", reason="Times out"
)
def test_R_transport(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "R_transport.ipynb"
    args = default_args()
    nb, errors = run_notebook(fname, tmp_path, tmp_env, kernel_name="IR", **args)
    assert errors == []


@pytest.mark.xdist_group(name=f"{group_base_name}-1")
@pytest.mark.rixmp
# TODO investigate and resolve the cause of the time outs; remove this mark
@pytest.mark.skipif(
    "GITHUB_ACTIONS" in os.environ and sys.platform == "linux", reason="Times out"
)
def test_R_transport_scenario(tutorial_path, tmp_path, tmp_env):
    fname = tutorial_path / "transport" / "R_transport_scenario.ipynb"
    args = default_args()
    nb, errors = run_notebook(fname, tmp_path, tmp_env, kernel_name="IR", **args)
    assert errors == []
