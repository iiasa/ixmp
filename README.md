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

Copyright 2017-18 IIASA Energy Program

The platform package is licensed under the Apache License, Version 2.0 (the
"License"); you may not use the files in this repository except in compliance
with the License.  You may obtain a copy of the License at
<http://www.apache.org/licenses/LICENSE-2.0>.

Please refer to the [NOTICE](NOTICE.rst) for details and the user guidelines.


## Documentation and tutorial

A [documentation the ix modeling platform and the MESSAGEix
framework](http://MESSAGEix.iiasa.ac.at/) is automatically created from the
documentation of the Python and R API packages.  The online documentation is
synchronyzed with the contents of the master branch of the repositories
[www.github.com/iiasa/ixmp](http://www.github.com/iiasa/ixmp) and
[www.github.com/iiasa/message_ix](http://www.github.com/iiasa/message_ix).

There are a number of tutorials to get started with ixmp.  You may want to try
the [tutorial/transport](tutorial/transport/README.md)...

Follow the instructions in [doc/README](doc/README.md) for building the ixmp
documentation including the scientific programming API manuals on your local
machine.


## Scientific reference

Please cite the following manuscript when using the MESSAGEix framework and/or
the ix modeling platform for scientific publications or technical reports:

  Daniel Huppmann, Matthew Gidden, Oliver Fricko, Peter Kolp, Clara Orthofer,
  Michael Pimmer, Adriano Vinca, Alessio Mastrucci, Keywan Riahi, and Volker
  Krey.  "The |MESSAGEix| Integrated Assessment Model and the ix modeling
  platform". 2018, submitted.  Electronic pre-print available at
  [pure.iiasa.ac.at/15157/](https://pure.iiasa.ac.at/15157/).

## Install from Conda (New Users)

1. Install Python via [Anaconda](https://www.continuum.io/downloads). We
   recommend the latest version, e.g., Python 3.6+.
   
2. Install [GAMS](https://www.gams.com/download/). **Importantly**:

   - Check the box labeled `Use advanced installation mode`
   - Check the box labeled `Add GAMS directory to PATH environment variable` on
     the Advanced Options page.
   
3. Open a command prompt and type

    ```
    conda install ixmp
    ```

## Install from Conda (Advanced Users)

### Dependencies

0. Install the Java Development Kit (Java SE 8) and set the environment variable
   `JAVA_HOME` per the [JDK website
   instructions](https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/);
   if `JAVA_HOME` does not exist, add as new system variable.  *At this point,
   ixmp is not compatible with JAVA SE 9.*
   
0. Update your `PATH` environment variable to point to the JRE binaries and
   server installation (e.g., `C:\Program Files\Java\jdk[YOUR JDK
   VERSION]\jre\bin\`, `C:\Program Files\Java\jdk[YOUR JDK
   VERSION]\jre\bin\server`).
   
   <aside class="warning">
   Do not overwrite the existing `PATH` environment variable, but add to the list of existing paths.
   </aside>

0. Install the latest version of GAMS, otherwise, the export to GDX may not work
   properly (visit [www.gams.com](http://www.gams.com)).  If you only have a
   license for an older verson of GAMS, install both versions.  Note that for
   using the integrated MESSAGEix-MACRO model, it is important to install a GAMS
   version >= 24.8.1.

0. Update your `PATH` environment variable to point to the GAMS installation
   (e.g. `C:\GAMS\win64\24.8`); again do not overwrite existing `PATH` but
   rather add to end.

0. Install some version of Python (2.7 is supported, but 3.6 or higher is
   recommended).  [Anaconda](https://www.continuum.io/downloads) is a good
   choice for users not yet familiar with the language (during installation
   select add anaconda to PATH system variable)

0. Install a Windows C++ Compiler

   - For Python 3: [link](http://landinghub.visualstudio.com/visual-cpp-build-tools)
   - For Python 2: [link](https://www.microsoft.com/en-us/download/details.aspx?id=44266)

0. Install a version of `git`, (see, e.g., the [website](https://git-scm.com/downloads))

0. In a command prompt, execute the following two lines to install all Python
   dependencies for running the unit tests and building the auto-documentation
   for the Python interface and the MESSAGEix GAMS code:

    ```
    pip install cython numpy pandas "pytest>=3.0.6" "JPype1>=0.6.2" 
    pip install sphinx sphinxcontrib.bibtex sphinxcontrib-fulltoc numpydoc cloud_sptheme
    ```

#### Additional dependencies for R users

0. Make sure the R version installed is either 32 OR 64 bit (and >= 3.3.0), consistently with GAMS and Java. Having both 32 and 64 bit generates error.

0. Install packages `rJava` , `devtools` and `optparse` via the R package manager

0. Install Rtools and add the path to the environment variables

0. For working with Jupyter notebooks using R, install the [IRkernel](https://irkernel.github.io)


### Installing the ix modeling platform

0. Fork this repository and clone the forked repository (`<user>/ixmp`)
   to your machine.  To fork the repository, look for the fork button 
   in the top right at [iiasa/ixmp](https://github.com/iiasa/ixmp).
   Add `iiasa/ixmp` as `upstream` to your clone.

   *We recommend* [GitKraken](https://www.gitkraken.com/) *for users who prefer a graphical user 
    interface application to work with Github (as opposed to the command line).*

0. Open a command prompt in the new `ixmp` directory and type

   ```
   pip install -r requirements.txt
   python setup.py install
   py.test tests
   ```

### Notes and Warnings 

0. For **Anaconda** users experiencing problems during installation of ixmp,
   Anaconda might not have been added to the PATH system variable properly. 
   So, if ``install.bat`` just opens and collapses again, check if:

    ```
    C:\[YOUR ANACONDA LOCATION]\Anaconda3;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Scripts;
    C:\[YOUR ANACONDA LOCATION]\Anaconda3\Library\bin;
    ```   

   are all part of the PATH system variable. If they are not there, add them.

### Building Documentation

Navigate to the `doc` folder and in a command prompt type

   ```
   make doc
   ```

