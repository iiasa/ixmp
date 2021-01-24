# The ix modeling platform (ixmp)

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4005665.svg)](https://doi.org/10.5281/zenodo.4005665)
[![PyPI version](https://img.shields.io/pypi/v/ixmp.svg)](https://pypi.python.org/pypi/ixmp/)
[![Anaconda version](https://img.shields.io/conda/vn/conda-forge/ixmp)](https://anaconda.org/conda-forge/ixmp)
[![Documentation build](https://readthedocs.com/projects/iiasa-energy-program-ixmp/badge/?version=master)](https://docs.messageix.org/projects/ixmp/en/master/)
[![Build status](https://github.com/iiasa/ixmp/workflows/pytest/badge.svg)](https://github.com/iiasa/ixmp/actions?query=workflow:pytest)
[![Test coverage](https://codecov.io/gh/iiasa/ixmp/branch/master/graph/badge.svg)](https://codecov.io/gh/iiasa/ixmp)

## Overview

The ix modeling platform (ixmp) is a data warehouse for high-powered scenario
analysis, with interfaces to Python and R for efficient scientific workflows and
effective data pre- and post-processing, and a structured database backend for
version-controlled data management.

This repository contains the core and application programming interfaces (API)
for the ix modeling platform (ixmp), as well as a number of tutorials and
examples for a generic model instance based on Dantzig's transport problem.


## License

Copyright © 2017–2021 IIASA Energy, Climate, and Environment (ECE) program

The platform package is licensed under the Apache License, Version 2.0 (the
"License"); you may not use the files in this repository except in compliance
with the License. You may obtain a copy of the License at
<https://www.apache.org/licenses/LICENSE-2.0>.

Please refer to the [NOTICE](NOTICE.rst) for details and user guidelines.


## Getting started

### Documentation

Documentation of ixmp and the MESSAGEix framework is available in two forms:

- The [MESSAGEix framework documentation](https://docs.messageix.org/)
  includes documentation of the
  [ixmp Python API](https://docs.messageix.org/en/stable/api.html), which
  is extended by the framework.
- The [stand-alone ixmp
  documentation](https://docs.messageix.org/projects/ixmp/) contains
  additional information on installing ixmp from source, the R API, etc.

The online documentation is built automatically from the contents of the
[ixmp Github repository](https://github.com/iiasa/ixmp).
For documentation of a specific release, e.g. `v3.2.0`, use the chooser in the sidebar.

For offline use, the documentation can be built from the source code.
See [`doc/README.rst`](doc/README.rst) for further details.


### Installation

Most users will have ixmp installed automatically as a dependency when
[installing MESSAGEix](https://docs.messageix.org/en/stable/getting_started.html).

To install the ixmp R API, or to install ixmp from source code, see
[‘Installation’ in the documentation](https://docs.messageix.org/projects/ixmp/en/stable/install.html).


### Tutorials

Introductory tutorials are provided in both Python and R.
See [‘Tutorials’ in the documentation](https://docs.messageix.org/projects/ixmp/en/stable/tutorials.html) or [`tutorial/README.rst`](tutorial/README.rst).


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
