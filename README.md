# `ixmp`: the ix modeling platform

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.4005665.svg)](https://doi.org/10.5281/zenodo.4005665)
[![PyPI version](https://img.shields.io/pypi/v/ixmp.svg)](https://pypi.python.org/pypi/ixmp/)
[![Anaconda version](https://img.shields.io/conda/vn/conda-forge/ixmp)](https://anaconda.org/conda-forge/ixmp)
[![Documentation build](https://readthedocs.com/projects/iiasa-energy-program-ixmp/badge/?version=latest)](https://docs.messageix.org/projects/ixmp/en/latest/)
[![Build status](https://github.com/iiasa/ixmp/actions/workflows/pytest.yaml/badge.svg)](https://github.com/iiasa/ixmp/actions/workflows/pytest.yaml)
[![Test coverage](https://codecov.io/gh/iiasa/ixmp/branch/master/graph/badge.svg)](https://codecov.io/gh/iiasa/ixmp)

The ix modeling platform (`ixmp`) is a data warehouse for high-powered scenario
analysis, with interfaces to Python and R for efficient scientific workflows and
effective data pre- and post-processing, and a structured database backend for
version-controlled data management.
In the name, “ix” stands for “integrated” and “cross (x) cutting”.

The [MESSAGEix modeling *framework*](https://docs.messageix.org) is built on top of the ix modeling *platform*.


This repository contains the core and application programming interfaces (API)
for the ix modeling platform (ixmp), as well as a number of tutorials and
examples for a generic model instance based on Dantzig's transport problem.

## Documentation

Complete documentation of the ixmp API is available for current and past versions at: https://docs.messageix.org/ixmp/.
This includes:

- **Installation.**
  Most users will have ixmp installed automatically as a dependency when
[installing MESSAGEix](https://docs.messageix.org/en/stable/#getting-started).
  To install ixmp from source code, or to use ixmp from R, see
[‘Installation’](https://docs.messageix.org/projects/ixmp/en/stable/install.html) or [`INSTALL.rst`](INSTALL.rst).
- **Tutorials.** Introductory tutorials are provided in both Python and R; see [‘Tutorials’](https://docs.messageix.org/projects/ixmp/en/stable/tutorials.html) or [`tutorial/README.rst`](tutorial/README.rst).
- How to **cite `ixmp` when using it in published scientific work.** See [‘User guidelines and notice’](https://docs.messageix.org/en/stable/notice.html) or [`NOTICE.rst`](NOTICE.rst).
- **Changelog and migration notes** for each new release: see [“What's New”](https://docs.messageix.org/projects/ixmp/en/stable/whatsnew.html).

Other forms of documentation:

- The online documentation is built automatically from the contents of the
[ixmp GitHub repository](https://github.com/iiasa/ixmp).
- For documentation of a specific release, e.g. `v3.2.0`, use the chooser in the bottom sidebar.
- For offline use, the documentation can be built from the source code.
See [`doc/README.rst`](doc/README.rst) for further details.
- The [MESSAGEix API documentation](https://docs.messageix.org/en/stable/api.html) links to the ixmp documentation in many places, for convenience.

## License

Copyright © 2017–2025 IIASA Energy, Climate, and Environment (ECE) program

`ixmp` is licensed under the Apache License, Version 2.0 (the "License"); you
may not use the files in this repository except in compliance with the License.
You may obtain a copy of the License in [`LICENSE`](LICENSE) or at
<https://www.apache.org/licenses/LICENSE-2.0>.
