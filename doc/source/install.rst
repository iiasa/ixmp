Installation
============

Most users will have |ixmp| installed automatically when `installing MESSAGEix`_.

The sections below cover other use cases:

- Installing *ixmp* to be used alone, i.e. with models or frameworks other than
  |MESSAGEix|.
- Installing *ixmp* from source, for development purposes.
- Installing the R API to *ixmp*.


.. contents::
   :local:


Technical requirements
----------------------

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

6. (Optional) Run the built-in test suite to check that *ixmp* functions
   correctly on your system::

    $ pip install .[tests]
    $ py.test


Install ``rixmp``, the R API
----------------------------

- Make sure the R version installed is either 32 OR 64 bit (and >= 3.3.0),
  consistently with GAMS and Java. Having both 32 and 64 bit generates error.

1. Install packages, `devtools` and `optparse` via the R package manager

2. Two alternative options are available:

    - use the package developed in Python, requires the `reticulate` R package (it will allow to install/use `rixmp`)

    - use specific R packages (less functionality), requires the `rJava` R package (it will allow to install/use `rixmp.legacy`)

3. Install Rtools and add the path to the environment variables

4. For working with Jupyter notebooks using R, install the
   [IRkernel](https://irkernel.github.io).


Install development tools
-------------------------


1. Install the Java Development Kit (Java SE 8) and set the environment variable
   `JAVA_HOME` per the [JDK website
   instructions](https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/);
   if `JAVA_HOME` does not exist, add as new system variable.  *At this point,
   ixmp is not compatible with JAVA SE 9.*

2. Update your `PATH` environment variable to point to the JRE binaries and
   server installation (e.g., `C:\Program Files\Java\jdk[YOUR JDK
   VERSION]\jre\bin\`, `C:\Program Files\Java\jdk[YOUR JDK
   VERSION]\jre\bin\server`).

   <aside class="warning">
   Do not overwrite the existing `PATH` environment variable, but add to the list of existing paths.
   </aside>

3. Install the latest version of GAMS, otherwise, the export to GDX may not work
   properly (visit [www.gams.com](http://www.gams.com)).  If you only have a
   license for an older verson of GAMS, install both versions.  Note that for
   using the integrated MESSAGEix-MACRO model, it is important to install a GAMS
   version >= 24.8.1.

4. Update your `PATH` environment variable to point to the GAMS installation
   (e.g. `C:\GAMS\win64\24.8`); again do not overwrite existing `PATH` but
   rather add to end.

5. Install some version of Python (2.7 is supported, but 3.6 or higher is
   recommended).  [Anaconda](https://www.continuum.io/downloads) is a good
   choice for users not yet familiar with the language (during installation
   select add anaconda to PATH system variable)

6. Install a Windows C++ Compiler

   - For Python 3: [link](http://landinghub.visualstudio.com/visual-cpp-build-tools)
   - For Python 2: [link](https://www.microsoft.com/en-us/download/details.aspx?id=44266)

7. Install a version of `git`, (see, e.g., the [website](https://git-scm.com/downloads))


Troubleshooting

For **Anaconda** users experiencing problems during installation of ixmp,
Anaconda might not have been added to the PATH system variable properly.
So, if ``install.bat`` just opens and collapses again, check if::

    C:\[YOUR ANACONDA LOCATION]\Anaconda3;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Scripts;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Library\bin;

are all part of the PATH system variable. If they are not there, add them.

.. technical_requirements:

Technical requirements
----------------------

A high-quality desktop computer or laptop is sufficient for most purposes
using the |ixmp|.

- | *For new users:*
  | *Please set up a* **GitHub account** (`github.com/join`_) *and get familiar
    with forking and cloning repositories, as well as pulling, committing and pushing changes.*

- For using `GAMS`_ to solve numerical optimisation problems,
  you need to install the latest version of GAMS (in particular 24.8 or higher).
  If you do only have a license for an older version,
  install both the older and the latest version of GAMS.


Scientific programming interface
--------------------------------

The scientific programming interface can be used either through Python or R:

- | **R**: visit the Comprehensive R Archive Network (`cran.r-project.org`_)
  | and install the R editor of your choice.
  | The following package is required: ``rJava``.


.. _`Anaconda`: https://www.continuum.io/downloads
.. _`GAMS`: http://www.gams.com
.. _`Github Desktop`: https://desktop.github.com

.. _`github.com/join` : https://github.com/join
.. _`gitkraken.com` : https://www.gitkraken.com/
.. _`cran.r-project.org` : https://cran.r-project.org/
.. _`README` : https://github.com/iiasa/ixmp/blob/master/README.md
