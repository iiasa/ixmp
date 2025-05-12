import os
import platform
import sys

import numpy as np
import pytest

from ixmp.backend import available
from ixmp.testing import get_cell_output, run_notebook

group_base_name = platform.system() + platform.python_version()

GHA = "GITHUB_ACTIONS" in os.environ


FLAKY = pytest.mark.flaky(
    reruns=5,
    rerun_delay=2,
    condition=GHA and platform.system() == "Windows",
    reason="Flaky; see iiasa/ixmp#543",
)


@pytest.fixture(scope="session")
def default_args():
    """Default arguments for :func:`.run_notebook."""
    # Use a longer timeout for GHA
    return dict(timeout=30) if GHA else dict()


@FLAKY
@pytest.mark.xdist_group(name=f"{group_base_name}-0")
@pytest.mark.parametrize("ixmp_backend", available())
def test_py_transport(tmp_path, tmp_env, tutorial_path, default_args, ixmp_backend):
    fname = tutorial_path.joinpath("transport", "py_transport.ipynb")

    # Set the "default" platform to be either the platform named "local" or
    # "ixmp4-local", according to the ixmp_backend parameter
    p = {"jdbc": "local", "ixmp4": "ixmp4-local"}[ixmp_backend]

    # Notebook runs without error
    nb, errors = run_notebook(
        fname, tmp_path, tmp_env, default_platform=p, **default_args
    )
    assert errors == []

    # FIXME use get_cell_by_name instead of assuming cell count/order is fixed
    assert np.isclose(get_cell_output(nb, -5)["lvl"], 153.6750030517578)


@pytest.mark.xdist_group(name=f"{group_base_name}-0")
def test_py_transport_scenario(tutorial_path, tmp_path, tmp_env, default_args):
    fname = tutorial_path / "transport" / "py_transport_scenario.ipynb"
    nb, errors = run_notebook(fname, tmp_path, tmp_env, **default_args)
    assert errors == []

    assert np.isclose(get_cell_output(nb, "scen-z")["lvl"], 153.675)
    assert np.isclose(get_cell_output(nb, "scen-detroit-z")["lvl"], 161.324)


@FLAKY
@pytest.mark.xdist_group(name=f"{group_base_name}-1")
@pytest.mark.rixmp
# TODO investigate and resolve the cause of the time outs; remove this mark
@pytest.mark.skipif(GHA and sys.platform == "linux", reason="Times out")
def test_R_transport(tutorial_path, tmp_path, tmp_env, default_args):
    fname = tutorial_path / "transport" / "R_transport.ipynb"
    nb, errors = run_notebook(
        fname, tmp_path, tmp_env, kernel_name="IR", **default_args
    )
    assert errors == []


@pytest.mark.xdist_group(name=f"{group_base_name}-1")
@pytest.mark.rixmp
# TODO investigate and resolve the cause of the time outs; remove this mark
@pytest.mark.skipif(GHA and sys.platform == "linux", reason="Times out")
def test_R_transport_scenario(tutorial_path, tmp_path, tmp_env, default_args):
    fname = tutorial_path / "transport" / "R_transport_scenario.ipynb"
    nb, errors = run_notebook(
        fname, tmp_path, tmp_env, kernel_name="IR", **default_args
    )
    assert errors == []
