
# Next Release

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
