"""Testing Juypter notebooks."""
import os
import sys
from warnings import warn

import pytest

nbformat = pytest.importorskip("nbformat")


def run_notebook(nb_path, tmp_path, env=None, **kwargs):
    """Execute a Jupyter notebook via :mod:`nbclient` and collect output.

    Parameters
    ----------
    nb_path : path-like
        The notebook file to execute.
    tmp_path : path-like
        A directory in which to create temporary output.
    env : dict-like, optional
        Execution environment for :mod:`nbclient`. Default: :obj:`os.environ`.
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

    # Workaround for https://github.com/jupyter/nbclient/issues/85
    if (
        sys.version_info[0] == 3
        and sys.version_info[1] >= 8
        and sys.platform.startswith("win")
    ):
        import asyncio

        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

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

    # Create a client and use it to execute the notebook
    client = NotebookClient(nb, **kwargs, resources=dict(metadata=dict(path=tmp_path)))

    # Execute the notebook.
    # `env` is passed from nbclient to jupyter_client.launcher.launch_kernel()
    client.execute(env=env or os.environ.copy())

    # Retrieve error information from cells
    errors = [
        output
        for cell in nb.cells
        if "outputs" in cell
        for output in cell["outputs"]
        if output.output_type == "error"
    ]

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
        return eval(result["text/plain"]) if kind == "data" else result
