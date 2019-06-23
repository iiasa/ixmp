# The ix modeling platform (ixmp)

## Overview

The ix modeling platform (ixmp) is a data warehouse for high-powered scenario
analysis, with interfaces to Python and R for efficient scientific workflows and
effective data pre- and post-processing, and a structured database backend for
version-controlled data management.

This repository contains the core and application programming interfaces (API)
for the ix modeling platform (ixmp), as well as a number of tutorials and
examples for a generic model instance based on Dantzig's transport problem.


## License

Copyright © 2017–2019 IIASA Energy Program

The platform package is licensed under the Apache License, Version 2.0 (the
"License"); you may not use the files in this repository except in compliance
with the License. You may obtain a copy of the License at
<http://www.apache.org/licenses/LICENSE-2.0>.

Please refer to the [NOTICE](NOTICE.rst) for details and user guidelines.


## Getting started

### Documentation

Documentation of ixmp and the MESSAGEix framework is available in two forms:

- The [MESSAGEix framework documentation](https://message.iiasa.ac.at/)
  includes documentation of the
  [ixmp Python API](http://message.iiasa.ac.at/en/stable/api/ixmp.html), which
  is extended by the framework.
- The [stand-alone ixmp
  documentation](https://message.iiasa.ac.at/projects/ixmp/) contains
  additional information on installing ixmp from source, the R API, etc.

The online documentation is built automatically from the contents of the
[ixmp Github repository](https://github.com/iiasa/ixmp).

For offline use, the documentation can be built from the source code.
See [`doc/README.rst`](doc/README.rst) for further details.


### Installation

Most users will have ixmp installed automatically as a dependency when
[installing MESSAGEix](https://message.iiasa.ac.at/en/stable/getting_started.html).

To install the ixmp R API, or to install ixmp from source code, see
[‘Installation’ in the documentation](https://message.iiasa.ac.at/projects/ixmp/en/stable/).


### Tutorials

Introductory tutorials are provided in both Python and R.
See `tutorial/README.md`.


## Scientific reference

Please cite the following manuscript when using the MESSAGEix framework and/or
the ix modeling platform for scientific publications or technical reports:

> Daniel Huppmann, Matthew Gidden, Oliver Fricko, Peter Kolp, Clara Orthofer,
  Michael Pimmer, Nikolay Kushin, Adriano Vinca, Alessio Mastrucci,
  Keywan Riahi, and Volker Krey.
  "The |MESSAGEix| Integrated Assessment Model and the ix modeling platform".
  *Environmental Modelling & Software* 112:143-156, 2019.
  doi: [10.1016/j.envsoft.2018.11.012](https://doi.org/10.1016/j.envsoft.2018.11.012)
  electronic pre-print available at
  [pure.iiasa.ac.at/15157/](https://pure.iiasa.ac.at/15157/)

## Install from Conda (New Users)

1. Install Python via [Anaconda](https://www.continuum.io/downloads). We
   recommend the latest version, e.g., Python 3.6+.

2. Install [GAMS](https://www.gams.com/download/). **Importantly**:

   - Check the box labeled `Use advanced installation mode`
   - Check the box labeled `Add GAMS directory to PATH environment variable` on
     the Advanced Options page.

3. Open a command prompt and type

    ```
    conda install -c conda-forge ixmp
    ```

## Install from Source (Advanced Users)

### Dependencies

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


#### Additional dependencies for R users

1. Make sure the R version installed is either 32 OR 64 bit (and >= 3.3.0),
   consistently with GAMS and Java. Having both 32 and 64 bit generates error.

1. Install packages, `devtools` and `optparse` via the R package manager

1. Two alternative options are available:

    - use the package developed in Python, requires the `reticulate` R package (it will allow to install/use `rixmp`)

    - use specific R packages (less functionality), requires the `rJava` R package (it will allow to install/use `rixmp.legacy`)

1. Install Rtools and add the path to the environment variables

1. For working with Jupyter notebooks using R, install the
   [IRkernel](https://irkernel.github.io).


### Installing the ix modeling platform

1. Fork this repository and clone the forked repository (`<user>/ixmp`)
   to your machine.  To fork the repository, look for the fork button
   in the top right at [iiasa/ixmp](https://github.com/iiasa/ixmp).
   Add `iiasa/ixmp` as `upstream` to your clone.

   *We recommend* [GitKraken](https://www.gitkraken.com/) *for users who prefer
   a graphical user interface application to work with Github (as opposed to
   the command line).*

2. Open a command prompt in the new `ixmp` directory and type:

       $ pip install .

3. (Optional) Run tests to check that `ixmp` works on your system:

       $ pip install .[tests]
       $ py.test tests

### Notes and Warnings

1. For **Anaconda** users experiencing problems during installation of ixmp,
   Anaconda might not have been added to the PATH system variable properly.
   So, if ``install.bat`` just opens and collapses again, check if:

    ```
    C:\[YOUR ANACONDA LOCATION]\Anaconda3;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Scripts;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Library\bin;
    ```   

   are all part of the PATH system variable. If they are not there, add them.
