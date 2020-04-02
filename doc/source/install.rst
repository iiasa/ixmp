Installation
============

Most users will have the |ixmp| installed automatically when `installing MESSAGEix`_.

The sections below cover other use cases:

- Installing *ixmp* to be used alone (i.e., with models or frameworks other than
  |MESSAGEix|):

  - see the section `Install GAMS`_,
  - then `Install ixmp via Anaconda`_.

- Installing *ixmp* from source, for development purposes: see
  `Install ixmp from source`_.

- Installing the R API to *ixmp*:

  - Start with `Install GAMS`_.
  - Then install *ixmp* either via Anaconda, or from source.
  - Finally, see `Install rixmp`_.

**Contents:**

.. contents::
   :local:


Technical requirements
----------------------

A high-quality desktop computer or laptop is sufficient for most purposes
using the |ixmp|.


Install GAMS
------------

*ixmp* requires `GAMS`_.

1. Download the latest version of `GAMS`_ for your operating system; run the
   installer.

2. Add GAMS to the ``PATH`` environment variable:

   - on Windows, in the GAMS installer…
      - Check the box labeled “Use advanced installation mode.”
      - Check the box labeled “Add GAMS directory to PATH environment variable”
        on the Advanced Options page.
   - on macOS or Linux, add the following line to your ``.bash_profile`` (Mac) or ``.bashrc`` (Linux)::

          export PATH=$PATH:/path/to/gams-directory-with-gams-binary

.. note::
   For using `GAMS`_ to solve numerical optimisation problems, you need to
   install the latest version of GAMS (in particular 24.8 or higher). If you
   only have a license for an older version, install both the older and the
   latest version of GAMS.


Install *ixmp* via Anaconda
---------------------------

After installing GAMS, we recommend that new users install Anaconda, and then
use it to install *ixmp*. Advanced users may choose to install *ixmp* from
source code (next section).

3. Install Python via `Anaconda`_. We recommend the latest version, i.e.,
   Python 3.6+.

4. Open a command prompt. We recommend Windows users use the “Anaconda Prompt”
   to avoid permissions issues when installing and using *ixmp*. This program
   is available in the Windows Start menu after installing Anaconda.

5. Install the ``ixmp`` package::

    $ conda install -c conda-forge ixmp


Install *ixmp* from source
--------------------------

3. (Optional) If you intend to contribute changes to *ixmp*, first register
   a Github account, and fork the `ixmp repository <https://github.com/iiasa/ixmp>`_. This will create a new repository ``<user>/ixmp``.

4. Clone either the main repository, or your fork; using the `Github Desktop`_
   client, or the command line::

    $ git clone git@github.com:iiasa/ixmp.git

    # or:
    $ git clone git@github.com:USER/ixmp.git

5. Open a command prompt in the ``ixmp`` directory and type::

    $ pip install --editable .

   The ``--editable`` flag ensures that changes to the source code are picked up every time ``import ixmp`` is used in Python code.

6. (Optional) Run the built-in test suite to check that *ixmp* functions
   correctly on your system::

    $ pip install --editable .[tests]
    $ py.test


Install ``rixmp``
-----------------

See also the :ref:`rixmp documentation <rixmp>`.

1. `Install R <https://www.r-project.org>`_.

   .. warning::
      Ensure the the R version installed is either 32- *or* 64-bit (and >= 3.5.0), consistently with GAMS and Java.
      Having both 32- and 64-bit versions of R, or mixed 32- and 64-bit versions of different packages, can cause errors.

2. Enter the directory ``rixmp/`` and use R to build and install the package and its dependencies, including reticulate_::

   $ cd rixmp
   $ Rscript -e "install.packages(c('knitr', 'reticulate'), repos='http://cran.rstudio.com/')"
   $ R CMD build .

3. Check that there is only one ``*tar.gz`` in the folder::

   $ R CMD INSTALL rixmp_*

4. (Optional) Run the built-in test suite to check that *ixmp* and *rixmp* functions, as in *Install ixmp from source 6.* (installing
   the R ``devtools`` package might be a pre-requisite). In the ``ixmp`` directory type::

    $ py.test --test-r

5. (Optional) For working with Jupyter notebooks using R, install the `IR kernel <https://irkernel.github.io>`_.

6. (Optional) Install `Rtools <https://cran.r-project.org/bin/windows/Rtools/>`_ and add the path to the environment variables.

.. _reticulate: https://rstudio.github.io/reticulate/


Install development tools
-------------------------

Developers making changes to the *ixmp* source may need one or more of the following tools.
Users developing models using existing *ixmp* functionality **should not** need these tools.

- **Java Development Kit (JDK).**

  - Install the Java Development Kit (JDK) for Java SE version 8 from
    https://www.oracle.com/technetwork/java/javase/downloads/index.html

    .. note:: At this point, ixmp is not compatible with JAVA SE 9.

  - Follow the `JDK website instructions
    <https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/>`_
    to set the ``JAVA_HOME`` environment variable; if ``JAVA_HOME`` does not
    exist, add as new system variable.

  - Update your `PATH` environment variable to point to the JRE binaries and
    server installation (e.g., ``C:\Program Files\Java\jdk[YOUR JDK
    VERSION]\jre\bin\``, ``C:\Program Files\Java\jdk[YOUR JDK
    VERSION]\jre\bin\server``).

    .. warning:: Do not overwrite the existing `PATH` environment variable, but
       add to the list of existing paths.

- **Git.** Use one of:

  - https://git-scm.com/downloads
  - https://desktop.github.com
  - https://www.gitkraken.com

  In addition, set up an account at https://github.com, and familiarize
  yourself with forking and cloning repositories, as well as pulling,
  committing and pushing changes.


Troubleshooting
---------------

For Anaconda users experiencing problems during installation of ixmp,
Anaconda might not have been added to the PATH system variable properly.
So, if ``install.bat`` fails, check if::

    C:\[YOUR ANACONDA LOCATION]\Anaconda3;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Scripts;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Library\bin;

are all part of the PATH system variable. If they are not there, add them.


.. _`installing MESSAGEix`: https://message.iiasa.ac.at/en/latest/getting_started.html
.. _`Anaconda`: https://www.continuum.io/downloads
.. _`GAMS`: http://www.gams.com
.. _`Github Desktop`: https://desktop.github.com
