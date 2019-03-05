
# Next Release

- [#128](https://github.com/iiasa/ixmp/pull/128): New module `ixmp.testing` for reuse of testing utilities.
- [#125](https://github.com/iiasa/ixmp/pull/125): Add functions to view and add regions for IAMC-style timeseries data.
- [#123](https://github.com/iiasa/ixmp/pull/123): Return absolute path from `find_dbprops()`.
- [#118](https://github.com/iiasa/ixmp/pull/118): Switch to RTD Sphinx theme.
- [#116](https://github.com/iiasa/ixmp/pull/116): Bugfix and extend functionality for working with IAMC-style timeseries data
- [#111](https://github.com/iiasa/ixmp/pull/111): Add functions to check if a Scenario has an item (set, par, var, equ)
- [#110](https://github.com/iiasa/ixmp/pull/110): Generalize the internal functions to format index dimensions for mapping sets and parameters
- [#108](https://github.com/iiasa/ixmp/pull/105): Improve documentation.
- [#105](https://github.com/iiasa/ixmp/pull/105): Replace [deprecated](http://pandas.pydata.org/pandas-docs/stable/indexing.html#ix-indexer-is-deprecated) pandas `.ix` indexer with `.iloc`.
- [#103](https://github.com/iiasa/ixmp/pull/103): Specify dependencies in setup.py.

# v0.1.3

- [#88](https://github.com/iiasa/ixmp/pull/80): Connecting to multiple databases, updating MESSAGE-scheme scenario specifications to version 1.1
- [#80](https://github.com/iiasa/ixmp/pull/80): Can now set logging level which is harmonized between Java and Python
- [#79](https://github.com/iiasa/ixmp/pull/79): Adding a deprecated-warning for `ixmp.Scenario` with `scheme=='MESSAGE'`
- [#76](https://github.com/iiasa/ixmp/pull/76): Changing the API from `mp.Scenario(...)` to `ixmp.Scenario(mp, ...)`
- [#73](https://github.com/iiasa/ixmp/pull/73): Adding a function `has_solution()`, rename kwargs to `..._solution`
- [#69](https://github.com/iiasa/ixmp/pull/69) bring retixmp available to other users
- [#64](https://github.com/iiasa/ixmp/pull/64): Support writing multiple sheets to Excel in utils.pd_write
- [#61](https://github.com/iiasa/ixmp/pull/61): Now able to connect to multiple databases (Platforms)
- [#58](https://github.com/iiasa/ixmp/pull/58): Add MacOSX support in CI
- [#52](https://github.com/iiasa/ixmp/pull/52): Add ability to load all scenario data into memory for fast subsequent computation.
- [#120](https://github.com/iiasa/ixmp/pull/120): Fix cloning scenario between databases
