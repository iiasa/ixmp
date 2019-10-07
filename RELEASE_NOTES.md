
# Next Release

- [#188](https://github.com/iiasa/ixmp/pull/188),
  [#195](https://github.com/iiasa/ixmp/pull/195): Enhance reporting.
- [#177](https://github.com/iiasa/ixmp/pull/177): add ability to pass `gams_args` through `Scenario.solve()`
- [#175](https://github.com/iiasa/ixmp/pull/175): Drop support for Python 2.
- [#174](https://github.com/iiasa/ixmp/pull/174): Set `convertStrings=True` for JPype >= 0.7; see the [JPype changelog](https://jpype.readthedocs.io/en/latest/CHANGELOG.html).
- [#173](https://github.com/iiasa/ixmp/pull/173): Make AppVeyor CI more robust; support pandas 0.25.0.
- [#165](https://github.com/iiasa/ixmp/pull/165): Add support for handling geodata.

# v0.2.0

Release 0.2.0 provides full support for Scenario.clone() across platforms (database instances), e.g. from a remote database to a local HSQL database.
IAMC-style timeseries data is better supported, and can be used to store processed results, together with model variables and equations.

Other improvements include a new, dedicated `ixmp.testing` module, user-supplied
callbacks in Scenario.solve(). The `retixmp` package using reticulate to access
the ixmp API is renamed to `rixmp` and now has its own unit tests (the former
`rixmp` package can be accessed as `rixmp.legacy`).

Release 0.2.0 coincides with MESSAGEix release 1.2.0.

## All changes

- [#135](https://github.com/iiasa/ixmp/pull/135): Test `rixmp` (former `retixmp`) using the R `testthat` package.
- [#142](https://github.com/iiasa/ixmp/pull/142): Cloning across platforms, better support of IAMC_style timeseries data, preparations for MESSAGEix release 1.2 in Java core.
- [#115](https://github.com/iiasa/ixmp/pull/115): Support iterating with user-supplied callbacks.
- [#130](https://github.com/iiasa/ixmp/pull/130): Recognize `IXMP_DATA` environment variable for configuration and local databases.
- [#129](https://github.com/iiasa/ixmp/pull/129), [#132](https://github.com/iiasa/ixmp/pull/132): Fully implement `Scenario.clone()` across platforms (databases).
- [#128](https://github.com/iiasa/ixmp/pull/128),
  [#137](https://github.com/iiasa/ixmp/pull/137): New module `ixmp.testing` for reuse of testing utilities.
- [#125](https://github.com/iiasa/ixmp/pull/125): Add functions to view and add regions for IAMC-style timeseries data.
- [#123](https://github.com/iiasa/ixmp/pull/123): Return absolute path from `find_dbprops()`.
- [#118](https://github.com/iiasa/ixmp/pull/118): Switch to RTD Sphinx theme.
- [#116](https://github.com/iiasa/ixmp/pull/116): Bugfix and extend functionality for working with IAMC-style timeseries data.
- [#111](https://github.com/iiasa/ixmp/pull/111): Add functions to check if a Scenario has an item (set, par, var, equ).
- [#110](https://github.com/iiasa/ixmp/pull/110): Generalize the internal functions to format index dimensions for mapping sets and parameters.
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
