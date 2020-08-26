Installation
************

Most users will have :mod:`ixmp` installed automatically when `installing MESSAGEix`_.
The sections below cover other use cases.

Ensure you have first read the :doc:`prerequisites <prereqs>` for understanding and using |MESSAGEix|.
These include specific points of knowledge that are necessary to understand these instructions and choose among different installation options.

Use cases for installing ixmp directly include:

- Installing *ixmp* to be used alone (i.e., with models or frameworks other than |MESSAGEix|).
  Follow the sections:

  - `Install system dependencies`_, then
  - `Using Anaconda`_.

- Installing *ixmp* from source, for development purposes, e.g. to be used with a source install of :mod:`message_ix`.
  Follow the sections:

  - `Install system dependencies`_, then
  - `From source`_.

- Installing the :doc:`rixmp <api-r>` R package, to use the :mod:`ixmp`, alone, from R.
  Again, to use |MESSAGEix| from R, it is sufficient to install ``rmessageix``; and not necessary to also/separately install ``rximp``.

  - Start with `Install system dependencies`_.
  - Then install :mod:`ixmp` from source.
  - Finally, see `Install rixmp`_.

**Contents:**

.. contents::
   :local:

Install system dependencies
===========================

GAMS (required)
---------------

:mod:`ixmp` requires `GAMS`_.

1. Download GAMS for your operating system; either the `latest version`_ or, for users not familiar with GAMS licenses, `version 29`_ (see note below).

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


Graphviz (optional)
-------------------

:meth:`ixmp.reporting.Reporter.visualize` uses `Graphviz`_, a program for graph visualization.
Installing ixmp causes the python :mod:`graphviz` package to be installed.
If you want to use :meth:`.visualize` or run the test suite, the Graphviz program itself must also be installed; otherwise it is **optional**.

If you install :mod:`ixmp` using Anaconda, Graphviz is installed automatically via `its conda-forge package`_.
For other methods of installation, see the `Graphviz download page`_ for downloads and instructions for your system.


Install :mod:`ixmp`
===================

Using Anaconda
--------------

After installing GAMS, we recommend that new users install Anaconda, and then use it to install :mod:`ixmp`.
Advanced users may choose to install :mod:`ixmp` from source code (next section).

4. Install Python via either `Miniconda`_ or `Anaconda`_. [1]_
   We recommend the latest version; currently Python 3.8.

5. Open a command prompt.
   We recommend Windows users use the “Anaconda Prompt” to avoid issues with permissions and environment variables when installing and using :mod:`ixmp`.
   This program is available in the Windows Start menu after installing Anaconda.

6. Configure conda to install :mod:`ixmp` from the conda-forge channel [2]_::

    $ conda config --prepend channels conda-forge

7. Create a new conda enviroment.
   This step is **required** if using Anaconda, but *optional* if using Miniconda.
   This example uses the name ``ixmp_env``, but you can use any name of your choice::

    $ conda create --name ixmp_env
    $ conda activate ixmp_env

6. Install the ``ixmp`` package into the current environment (either ``base``, or another name from step 7, e.g. ``ixmp_env``)::

    $ conda install -c conda-forge ixmp

.. [1] See the `conda glossary`_ for the differences between Anaconda and Miniconda, and the definitions of the terms ‘channel’ and ‘environment’ here.
.. [2] The ‘$’ character at the start of these lines indicates that the command text should be entered in the terminal or prompt, depending on the operating system.
       Do not retype the ‘$’ character itself.

.. note:: When using Anaconda (not Miniconda), steps (5) through (8) can also be performed using the graphical Anaconda Navigator.
   See the `Anaconda Navigator documentation`_ for how to perform the various steps.


From source
-----------

4. (Optional) If you intend to contribute changes to *ixmp*, first register a Github account, and fork the `ixmp repository <https://github.com/iiasa/ixmp>`_.
   This will create a new repository ``<user>/ixmp``.
   (Please also see :message_ix:doc:`contributing`.)

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


Install ``rixmp``
=================

``rixmp`` is the R interface to :mod:`ixmp`; see :doc:`its documentaiton <api-r>`.
You only need to install ``rixmp`` if you intend to use :mod:`ixmp` from R, rather than from Python.

Install :mod:`ixmp` **from source**, per the section above.
Then:

8. `Install R <https://www.r-project.org>`_.
   Ensure that your ``PATH`` environment variable is configured correctly so that the ``Rscript`` executable is available.

   .. warning::
      Ensure the the R version installed is either 32- *or* 64-bit (and >= 3.5.0), consistently with GAMS and Java.
      Having both 32- and 64-bit versions of R, or mixed 32- and 64-bit versions of different packages, can cause errors.

9. Open a command prompt in the :file:`ixmp/` directory.
   Type the following commands to build, then install, ``rixmp`` and its dependencies, including reticulate_::

    $ R CMD build rixmp

10. Check that there is only one :file:`*.tar.gz` or :file:`.zip` file in the folder, then run::

      # On Windows
      $ R CMD INSTALL rixmp_*.zip

      # Other operating systems
      $ R CMD INSTALL rixmp_*.tar.gz

11. (Optional) Install `IRKernel`_, which allows running R code in Jupyter notebooks (see the link for instructions).

12. (Optional) Check that the R interface works by using the built-in test suite to run the R tutorial notebooks::

    $ pytest -m rixmp


Troubleshooting
===============

Run ``ixmp show-versions`` on the command line to check that you have all dependencies installed, or when reporting issues.

For Anaconda users experiencing problems during installation of ixmp, check that the following paths are part of the ``PATH`` environment variable, and add them if missing::

    C:\[YOUR ANACONDA LOCATION]\Anaconda3;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Scripts;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Library\bin;


Install development tools
=========================

Developers making changes to the :mod:`ixmp` source **may** need one or more of the following tools.
Users developing models using existing functionality **should not** need these tools.

Git
   Use one of:

   - https://git-scm.com/downloads
   - https://desktop.github.com
   - https://www.gitkraken.com

Java Development Kit (JDK)
   - Install the Java Development Kit (JDK) for Java SE version 8 from https://www.oracle.com/technetwork/java/javase/downloads/index.html

     .. note:: At this point, ixmp is not compatible with JAVA SE 9.

   - Follow the `JDK website instructions`_ to set the ``JAVA_HOME`` environment variable; if ``JAVA_HOME`` does not exist, add it as a new system variable.

   - Update your ``PATH`` environment variable to point to the JRE binaries and server installation (e.g., :file:`C:\\Program Files\\Java\\jdk[YOUR JDK VERSION]\\jre\\bin\\`, :file:`C:\\Program Files\\Java\\jdk[YOUR JDK VERSION]\\jre\\bin\\server`).

     .. warning:: Do not overwrite the existing ``PATH`` environment variable, but add to the list of existing paths.

Rtools
   https://cran.r-project.org/bin/windows/Rtools/

   For installing or modifying some R packages on Windows.


.. _`installing MESSAGEix`: https://docs.messageix.org/en/latest/getting_started.html
.. _`Anaconda`: https://www.continuum.io/downloads
.. _`GAMS`: http://www.gams.com
.. _`latest version`: https://www.gams.com/download/
.. _`version 29`: https://www.gams.com/29/
.. _Graphviz: https://www.graphviz.org
.. _`its conda-forge package`: https://anaconda.org/conda-forge/graphviz
.. _Graphviz download page: https://www.graphviz.org/download/
.. _Miniconda: https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html
.. _conda glossary: https://docs.conda.io/projects/conda/en/latest/glossary.html
.. _Anaconda Navigator documentation: https://docs.anaconda.com/anaconda/navigator/
.. _`Github Desktop`: https://desktop.github.com
.. _reticulate: https://rstudio.github.io/reticulate/
.. _IRkernel: https://irkernel.github.io/installation/
.. _JDK website instructions: https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/
