Auto-documentation of the ixmp package
======================================

The documentation of the ix modeling platform is generated
partly automatically from docstrings in the Python and R packages.

Dependencies
------------

1. `Sphinx <http://sphinx-doc.org/>`_ v1.1.2 or higher
2. `sphinxcontrib.bibtex`
3. `sphinxcontrib-fulltoc`
4. `numpydoc`
5. `cloud_sptheme`

Writing in Restructed Text
--------------------------

There are a number of guides out there, e.g. on `docutils
<http://docutils.sourceforge.net/docs/user/rst/quickref.html>`_.

Building the Site
-----------------

On *nix, from the command line, run::

    make html

On Windows, from the command line, run::

    ./make.bat

You can then view the site by::

    cd build
    python -m SimpleHTTPServer

and pointing your browser at http://localhost:8000/html/
