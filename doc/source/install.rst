Installation
************

A desktop or laptop computer is sufficient for most purposes using :mod:`ixmp`.
Most users will have :mod:`ixmp` installed automatically when `installing MESSAGEix`_.

The sections below cover other use cases:

- Installing *ixmp* to be used alone (i.e., with models or frameworks other than |MESSAGEix|):

  - see the sections `Install system dependencies`_,
  - then `Install ixmp via Anaconda`_.

- Installing *ixmp* from source, for development purposes: see `Install ixmp from source`_.

- Installing the R API to *ixmp*:

  - Start with `Install system dependencies`_.
  - Then install *ixmp* either via Anaconda, or from source.
  - Finally, see `Install rixmp`_.

**Contents:**

.. contents::
   :local:

Install system dependencies
===========================

GAMS
----

:mod:`ixmp` requires `GAMS`_.

1. Download GAMS for your operating system; either the `latest version`_ or `version 29`_ (see note below).

2. Run the installer.

3. Ensure that the ``PATH`` environment variable on your system includes the path to the GAMS program:

   - on Windows, in the GAMS installer…

      - Check the box labeled “Use advanced installation mode.”
      - Check the box labeled “Add GAMS directory to PATH environment variable” on the Advanced Options page.

   - on other platforms (macOS or Linux), add the following line to a file such as :file:`~/.bash_profile` (macOS), :file:`~/.bashrc`, or :file:`~/.profile`::

       export PATH=$PATH:/path/to/gams-directory-with-gams-binary

.. note::
   :mod:`message_ix` requires GAMS version 24.8; :mod:`ixmp` has no minimum requirement *per se*.
   The latest version is recommended.

   GAMS is proprietary software and requires a license to solve optimization problems.
   To run both the :mod:`ixmp` and :mod:`message_ix` tutorials and test suites, a “free demonstration” license is required; the free license is suitable for these small models.
   Versions of GAMS up to `version 29`_ include such a license with the installer; since version 30, the free demo license is no longer included, but may be requested via the GAMS website.

.. note::
   If you only have a license for an older version of GAMS, install both the older and the latest versions.


Graphviz
--------

:meth:`ixmp.reporting.Reporter.visualize` uses `Graphviz`_, a program for graph visualization.
Installing ixmp causes the python :mod:`graphviz` package to be installed.
If you want to use :meth:`.visualize` or run the test suite, the Graphviz program itself must also be installed; otherwise it is **optional**.

If you `Install ixmp via Anaconda`_, Graphviz is installed automatically via `its conda-forge package`_.
For other methods of installation, see the `Graphviz download page`_ for downloads and instructions for your system.


Install ``ixmp`` via Anaconda
=============================

After installing GAMS, we recommend that new users install Anaconda, and then use it to install :mod:`ixmp`.
Advanced users may choose to install :mod:`ixmp` from source code (next section).

4. Install Python via `Anaconda`_.
   We recommend the latest version; currently Python 3.8.

5. Open a command prompt.
   We recommend Windows users use the “Anaconda Prompt” to avoid permissions issues when installing and using :mod:`ixmp`.
   This program is available in the Windows Start menu after installing Anaconda.

6. Install the ``ixmp`` package::

    $ conda install -c conda-forge ixmp


Install ``ixmp`` from source
============================

4. (Optional) If you intend to contribute changes to *ixmp*, first register a Github account, and fork the `ixmp repository <https://github.com/iiasa/ixmp>`_.
   This will create a new repository ``<user>/ixmp``.

5. Clone either the main repository, or your fork; using the `Github Desktop`_ client, or the command line::

    $ git clone git@github.com:iiasa/ixmp.git

    # or:
    $ git clone git@github.com:USER/ixmp.git

6. Open a command prompt in the :file:`ixmp/` directory that is created, and type::

    $ pip install --editable .[docs,tests,tutorial]

   The ``--editable`` flag ensures that changes to the source code are picked up every time ``import ixmp`` is used in Python code.
   The ``[docs,tests,tutorial]`` extra dependencies ensure additional dependencies are installed.

7. (Optional) Run the built-in test suite to check that :mod:`ixmp` functions correctly on your system::

    $ pytest


Install development tools
-------------------------

Developers making changes to the :mod:`ixmp` source **may** need one or more of the following tools.
Users developing models using existing functionality **should not** need these tools.

Git
   Use one of:

   - https://git-scm.com/downloads
   - https://desktop.github.com
   - https://www.gitkraken.com

   In addition, set up an account at https://github.com, and familiarize yourself with forking and cloning repositories, as well as pulling, committing and pushing changes.

Java Development Kit (JDK)
   - Install the Java Development Kit (JDK) for Java SE version 8 from https://www.oracle.com/technetwork/java/javase/downloads/index.html

     .. note:: At this point, ixmp is not compatible with JAVA SE 9.

   - Follow the `JDK website instructions <https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/>`_ to set the ``JAVA_HOME`` environment variable; if ``JAVA_HOME`` does not exist, add it as a new system variable.

   - Update your ``PATH`` environment variable to point to the JRE binaries and server installation (e.g., :file:`C:\\Program Files\\Java\\jdk[YOUR JDK VERSION]\\jre\\bin\\`, :file:`C:\\Program Files\\Java\\jdk[YOUR JDK VERSION]\\jre\\bin\\server`).

     .. warning:: Do not overwrite the existing ``PATH`` environment variable, but add to the list of existing paths.



Install ``rixmp``
=================

See also the :ref:`rixmp documentation <rixmp>`.

1. `Install R <https://www.r-project.org>`_.

   .. warning::
      Ensure the the R version installed is either 32- *or* 64-bit (and >= 3.5.0), consistently with GAMS and Java.
      Having both 32- and 64-bit versions of R, or mixed 32- and 64-bit versions of different packages, can cause errors.

2. Enter the directory ``rixmp/`` and use R to build and install the package and its dependencies, including reticulate_::

    $ cd rixmp
    $ Rscript -e "install.packages(c('knitr', 'reticulate'), repos='http://cran.rstudio.com/')"
    $ R CMD build .

3. Check that there is only one ``*tar.gz`` in the folder, then run::

    $ R CMD INSTALL rixmp_*

4. (Optional) Run the built-in test suite to check that *ixmp* and *rixmp* functions, as in *Install ixmp from source 6.* (installing
   the R ``devtools`` package might be a pre-requisite). In the ``ixmp`` directory type::

    $ pytest -m rixmp

5. (Optional) To use rixmp in Jupyter notebooks, install the `IR kernel <https://irkernel.github.io>`_.

6. (Optional) Install `Rtools <https://cran.r-project.org/bin/windows/Rtools/>`_ and add the path to the environment variables.

.. _reticulate: https://rstudio.github.io/reticulate/


Troubleshooting
===============

Run ``ixmp show-versions`` on the command line to check that you have all dependencies installed, or when reporting issues.

For Anaconda users experiencing problems during installation of ixmp, check that the following paths are part of the ``PATH`` environment variable, and add them if missing::

    C:\[YOUR ANACONDA LOCATION]\Anaconda3;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Scripts;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Library\bin;

.. _`installing MESSAGEix`: https://docs.messageix.org/en/stable/getting_started.html
.. _`Anaconda`: https://www.continuum.io/downloads
.. _`GAMS`: http://www.gams.com
.. _`latest version`: https://www.gams.com/download/
.. _`version 29`: https://www.gams.com/29/
.. _`its conda-forge package`: https://anaconda.org/conda-forge/graphviz
.. _`Graphviz download page`: https://www.graphviz.org/download/
.. _`Github Desktop`: https://desktop.github.com
