# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full list see
# the documentation: https://www.sphinx-doc.org/en/master/usage/configuration.html

import importlib.metadata
from pathlib import Path
from typing import Optional

# -- Project information ---------------------------------------------------------------

project = "ixmp"
copyright = "2017–%Y, IIASA Energy, Climate, and Environment (ECE) program"
author = "ixmp Developers"


# -- General configuration -------------------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions coming
# with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    # First party
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    # Others
    "ixmp.util.sphinx_linkcode_github",
    "genno.compat.sphinx.rewrite_refs",
    "sphinxcontrib.bibtex",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and directories to
# ignore when looking for source files. This pattern also affects html_static_path and
# html_extra_path.
exclude_patterns = ["_build", "README.rst", "Thumbs.db", ".DS_Store"]

# Warn about *all* references where the target cannot be found
nitpicky = True

# A string of reStructuredText that will be included at the beginning of every source
# file that is read.
version = importlib.metadata.version("ixmp")
rst_prolog = rf"""
.. role:: py(code)
   :language: python

.. role:: strike
.. role:: underline

.. |MESSAGEix| replace:: MESSAGE\ :emphasis:`ix`
.. |ixmp| replace:: :emphasis:`ix modeling platform`
.. |version| replace:: {version}
"""

# -- Options for HTML output -----------------------------------------------------------

# A list of CSS files.
html_css_files = ["custom.css"]

html_favicon = "_static/favicon.svg"

# The name of an image file (relative to this directory) to place at the top of the
# sidebar.
html_logo = "_static/combined-logo-white.png"

# Add any paths that contain custom static files (such as style sheets) here, relative
# to this directory. They are copied after the builtin static files, so a file named
# "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# The theme to use for HTML and HTML Help pages.
html_theme = "sphinx_rtd_theme"

# -- Options for genno.compat.sphinx.rewrite_refs --------------------------------------

reference_aliases = {
    r"(genno\.|)Quantity": "genno.core.attrseries.AttrSeries",
    "AnyQuantity": ":data:`genno.core.quantity.AnyQuantity`",
}


# -- Options for sphinx.ext.extlinks ---------------------------------------------------

extlinks = {
    "issue": ("https://github.com/iiasa/ixmp/issue/%s", "#%s"),
    "pull": ("https://github.com/iiasa/ixmp/pull/%s", "PR #%s"),
}

# -- Options for sphinx.ext.intersphinx ------------------------------------------------


def local_inv(name: str, *parts: str) -> Optional[str]:
    """Construct the path to a local intersphinx inventory."""

    from importlib.util import find_spec

    spec = find_spec(name)
    if spec is None:
        return None

    if 0 == len(parts):
        parts = ("doc", "_build", "html")
    assert spec.origin is not None
    return str(Path(spec.origin).parents[1].joinpath(*parts, "objects.inv"))


intersphinx_mapping = {
    "dask": ("https://docs.dask.org/en/stable/", None),
    "genno": ("https://genno.readthedocs.io/en/latest/", (local_inv("genno"), None)),
    "jpype": ("https://jpype.readthedocs.io/en/latest", None),
    "message_ix": ("https://docs.messageix.org/en/latest/", None),
    "message-ix-models": (
        "https://docs.messageix.org/projects/models/en/latest/",
        None,
    ),
    "nbclient": ("https://nbclient.readthedocs.io/en/latest/", None),
    "nbformat": ("https://nbformat.readthedocs.io/en/latest/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "openpyxl": ("https://openpyxl.readthedocs.io/en/stable/", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "pint": ("https://pint.readthedocs.io/en/stable/", None),
    "pyam": ("https://pyam-iamc.readthedocs.io/en/stable/", None),
    "python": ("https://docs.python.org/3/", None),
    "sparse": ("https://sparse.pydata.org/en/stable/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
    "xarray": ("https://docs.xarray.dev/en/stable", None),
}

# -- Options for sphinx.ext.linkcode / ixmp.util.sphinx_linkcode_github ----------------

linkcode_github_repo_slug = "iiasa/ixmp"

# -- Options for sphinx.ext.napoleon ---------------------------------------------------

napoleon_preprocess_types = True
napoleon_type_aliases = {
    # Standard library
    "callable": ":ref:`callable <python:callable-types>`",
    "iterable": ":class:`collections.abc.Iterable`",
    "sequence": ":class:`collections.abc.Sequence`",
    # Upstream
    "Quantity": ":class:`genno.Quantity`",
    # This package
    "Platform": "~ixmp.Platform",
    "Scenario": "~ixmp.Scenario",
}

# -- Options for sphinx.ext.todo -------------------------------------------------------

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# -- Options for sphinxcontrib.bibtext -------------------------------------------------

bibtex_bibfiles = ["references.bib"]
