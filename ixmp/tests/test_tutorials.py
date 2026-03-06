import os
import platform
import sys
from importlib.metadata import version
from pathlib import Path
from typing import TypedDict

import numpy as np
import pytest
from packaging.version import Version as V

from ixmp.testing import GHA, get_cell_output, run_notebook

GROUP_BASE_NAME = platform.system() + platform.python_version()

MARK = {
    0: pytest.mark.flaky(
        reruns=5,
        rerun_delay=2,
        condition=GHA and platform.system() == "Windows",
        reason="Flaky; see iiasa/ixmp#543",
    ),
    1: pytest.mark.skipif(
        GHA and sys.platform == "darwin" and sys.version_info[:2] in {(3, 11), (3, 12)},
        reason="Times out",
    ),
    2: pytest.mark.skipif(GHA and sys.platform == "linux", reason="Times out"),
    3: pytest.mark.xfail(
        V(version("pandas")) >= V("3"),
        reason="https://github.com/iiasa/ixmp/issues/628",
    ),
}


class DefaultKwargs(TypedDict, total=False):
    default_platform: str
    timeout: int


@pytest.fixture
def default_args(default_platform_name: str) -> DefaultKwargs:
    """Default arguments for :func:`.run_notebook.

    - Set the "default" platform to be either the platform named "local" or
      "ixmp4-local", according to the `backend` fixture.
    - Use a longer timeout on GitHub Actions.
    """
    return dict(default_platform=default_platform_name, timeout=60 if GHA else 0)


@MARK[0]
@MARK[1]
@pytest.mark.xdist_group(name=f"{GROUP_BASE_NAME}-0")
def test_py_transport(
    tmp_path: Path,
    tmp_env: os._Environ[str],
    tutorial_path: Path,
    default_args: DefaultKwargs,
) -> None:
    fname = tutorial_path.joinpath("transport", "py_transport.ipynb")

    nb, errors = run_notebook(fname, tmp_path, tmp_env, **default_args)
    assert errors == []

    # FIXME use get_cell_by_name instead of assuming cell count/order is fixed
    assert np.isclose(get_cell_output(nb, -5)["lvl"], 153.6750030517578)


@MARK[1]
@pytest.mark.xdist_group(name=f"{GROUP_BASE_NAME}-0")
def test_py_transport_scenario(
    tutorial_path: Path,
    tmp_path: Path,
    tmp_env: os._Environ[str],
    default_args: DefaultKwargs,
) -> None:
    fname = tutorial_path / "transport" / "py_transport_scenario.ipynb"
    nb, errors = run_notebook(fname, tmp_path, tmp_env, **default_args)
    assert errors == []

    assert np.isclose(get_cell_output(nb, "scen-z")["lvl"], 153.675)
    assert np.isclose(get_cell_output(nb, "scen-detroit-z")["lvl"], 161.324)


@MARK[0]
@MARK[2]
@pytest.mark.xdist_group(name=f"{GROUP_BASE_NAME}-1")
@pytest.mark.rixmp
def test_R_transport(
    tutorial_path: Path,
    tmp_path: Path,
    tmp_env: os._Environ[str],
    default_args: DefaultKwargs,
) -> None:
    fname = tutorial_path / "transport" / "R_transport.ipynb"
    nb, errors = run_notebook(
        fname, tmp_path, tmp_env, kernel_name="IR", **default_args
    )
    assert errors == []


@MARK[2]
@MARK[3]
@pytest.mark.xdist_group(name=f"{GROUP_BASE_NAME}-1")
@pytest.mark.rixmp
def test_R_transport_scenario(
    tutorial_path: Path,
    tmp_path: Path,
    tmp_env: os._Environ[str],
    default_args: DefaultKwargs,
) -> None:
    fname = tutorial_path / "transport" / "R_transport_scenario.ipynb"
    nb, errors = run_notebook(
        fname, tmp_path, tmp_env, kernel_name="IR", **default_args
    )
    assert errors == []
