Auto-documentation of the ixmp package
======================================

The documentation of the ix modeling platform is generated from .rst files in
``doc/source``, and from numpy-format_ docstrings in the Python code.


Dependencies
------------

1. Sphinx_ v3.0 or higher
2. `sphinx_rtd_theme`
3. `sphinxcontrib.bibtex`
4. `numpydoc`

These can be installed as 'extra' dependencies of the ``ixmp`` package. From
the top-level directory of the repository, run::

    pip install .[docs]


Writing in reStructuredText
---------------------------

There are a number of guides out there, e.g. on docutils_.


Building the docs locally
-------------------------

Install the dependencies, above. Repeat the installation steps to be able to
refer to code that was changed since the initial installation.

From the command line, run::

    make html

The build documentation is in ``doc/build/html/`` and can be viewed by opening
``doc/build/html/index.html`` in a web browser.


Read the Docs
-------------

The official version of the documentation is hosted on Read The Docs (RTD) at
https://docs.messageix.org/projects/ixmp/. RTD builds the docs using a command
similar to::

    sphinx-build -T -E -d _build/doctrees-readthedocs -D language=en . \
      _build/html

This command is executed in the directory containing ``conf.py``, i.e.
``doc/source/``. Note that this is different from ``doc/``, where the above
``make`` tools are invoked. Use this to test whether the documentation build
works on RTD.


.. _numpy-format: https://numpydoc.readthedocs.io/en/latest/format.html
.. _Sphinx: http://sphinx-doc.org/
.. _docutils: http://docutils.sourceforge.net/docs/user/rst/quickref.html
