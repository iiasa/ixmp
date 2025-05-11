"""Testing Juypter notebooks."""

import os
import sys
from typing import Optional
from warnings import warn

import pytest

nbformat = pytest.importorskip("nbformat")


def run_notebook(
    nb_path, tmp_path, env=None, *, default_platform: Optional[str] = None, **kwargs
):
    """Execute a Jupyter notebook via :mod:`nbclient` and collect output.

    Parameters
    ----------
    nb_path : os.PathLike
        The notebook file to execute.
    tmp_path : os.PathLike
        A directory in which to create temporary output.
    env : dict, optional
        Execution environment for :mod:`nbclient`. Default: :obj:`os.environ`.
    default_platform :
        If given, adjust the ixmp :file:`config.json` in `env` to use the named platform
        as the default before executing the notebook, and restore the prior value
        afterwards.
    kwargs :
        Keyword arguments for :class:`nbclient.NotebookClient`. Defaults are set for:

        "allow_errors"
           Default :data:`False`. If :obj:`True`, the execution always succeeds, and
           cell output contains exception information rather than code outputs.

        "kernel_version"
           Jupyter kernel to use. Default: either "python2" or "python3", matching the
           current Python major version.

           .. warning:: Any existing configuration for this kernel on the local system—
              such as an IPython start-up file—will be executed when the kernel starts.
              Code that enables GUI features can interfere with :func:`run_notebook`.

        "timeout"
            in seconds; default 10.

    Returns
    -------
    nb : :class:`nbformat.NotebookNode`
        Parsed and executed notebook.
    errors : list
        Any execution errors.
    """
    import nbformat
    from nbclient import NotebookClient

    from ixmp import config

    # Read the notebook
    nb = nbformat.read(nb_path, as_version=4)

    # Set default keywords
    kwargs.setdefault("allow_errors", False)
    kernel = kwargs.pop("kernel", None)
    if kernel:  # pragma: no cover
        warn(
            '"kernel" keyword argument to run_notebook(); use "kernel_name"',
            DeprecationWarning,
            2,
        )
    kwargs.setdefault("kernel_name", kernel or f"python{sys.version_info[0]}")
    kwargs.setdefault("timeout", 10)

    env = env or os.environ.copy()

    if default_platform:
        # Set the default platform in the tmp_env
        assert config.path is not None and config.path.is_relative_to(env["IXMP_DATA"])

        # Store the default platform
        default_platform_pre = config.values["platform"]["default"]
        # Change the default platform
        config.add_platform("default", default_platform)
        # Save config.json
        config.save()

    # Create a client and use it to execute the notebook
    client = NotebookClient(nb, **kwargs, resources=dict(metadata=dict(path=tmp_path)))

    # Execute the notebook.
    # `env` is passed from nbclient to jupyter_client.launcher.launch_kernel()
    client.execute(env=env)

    # Retrieve error information from cells
    errors = [
        output
        for cell in nb.cells
        if "outputs" in cell
        for output in cell["outputs"]
        if output.output_type == "error"
    ]

    if default_platform:
        # Restore the default platform name
        config.add_platform("default", default_platform_pre)
        config.save()

    return nb, errors


def get_cell_output(nb, name_or_index, kind="data"):
    """Retrieve a cell from `nb` according to its metadata `name_or_index`:

    The Jupyter notebook format allows specifying a document-wide unique 'name' metadata
    attribute for each cell:

    https://nbformat.readthedocs.io/en/latest/format_description.html
    #cell-metadata

    Return the cell matching `name_or_index` if :class:`str`; or the cell at the
    :class:`int` index; or raise :class:`ValueError`.

    Parameters
    ----------
    kind : str, optional
        Kind of cell output to retrieve. For 'data', the data in format 'text/plain' is
        run through :func:`eval`. To retrieve an exception message, use 'evalue'.
    """
    import numpy

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
        # NB eval(…, globals=…) as a keyword argument is only possible with Python ≥3.13
        return eval(result["text/plain"], {"np": numpy}) if kind == "data" else result
