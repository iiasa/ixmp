Next release
============

All changes
-----------

- `#315 <https://github.com/iiasa/ixmp/pull/315>`_: Ensure :meth:`.Model.initialize` is always called for new *and* cloned objects.
- `#320 <https://github.com/iiasa/ixmp/pull/320>`_: Add CLI command `ixmp show-versions` to print ixmp and dependency versions for debugging.
- `#312 <https://github.com/iiasa/ixmp/pull/312>`_: Add :meth:`~.computations.apply_units`, :meth:`~computations.select` reporting calculations; expand :meth:`.Reporter.add`.
- `#310 <https://github.com/iiasa/ixmp/pull/310>`_: :meth:`.Reporter.add_product` accepts a :class:`.Key` with a tag; :func:`~.computations.aggregate` preserves :class:`.Quantity` attributes.
- `#304 <https://github.com/iiasa/ixmp/pull/304>`_: Add CLI command ``ixmp solve`` to run model solver.
- `#303 <https://github.com/iiasa/ixmp/pull/303>`_: Add `dims` and `units` arguments to :meth:`Reporter.add_file`; remove :meth:`Reporter.read_config` (redundant with :meth:`Reporter.configure`).
- `#295 <https://github.com/iiasa/ixmp/pull/295>`_: Add option to include `subannual` column in dataframe returned by :meth:`.TimeSeries.timeseries`.
- `#286 <https://github.com/iiasa/ixmp/pull/286>`_,
  `#297 <https://github.com/iiasa/ixmp/pull/297>`_,
  `#309 <https://github.com/iiasa/ixmp/pull/309>`_: Add :meth:`.Scenario.to_excel` and :meth:`.read_excel`; this functionality is transferred to ixmp from :mod:`message_ix` and enhanced for dealing with maximum row limits in Excel.
- `#270 <https://github.com/iiasa/ixmp/pull/270>`_: Include all tests in the ixmp package.
- `#212 <https://github.com/iiasa/ixmp/pull/212>`_: Add :meth:`Model.initialize` API to help populate new Scenarios according to a model scheme.
- `#267 <https://github.com/iiasa/ixmp/pull/267>`_: Apply units to reported quantities.
- `#254 <https://github.com/iiasa/ixmp/pull/254>`_: Remove deprecated items:

  - ~/.local/ixmp as a configuration location.
  - positional and ``dbtype=`` arguments to :class:`.Platform`/:class:`.JDBCBackend`.
  - ``first_model_year=``, ``keep_sol=``, and ``scen=`` arguments to :meth:`~.Scenario.clone`.
  - ``rixmp.legacy``, an earlier version of :ref:`the R interface <rixmp>` that did not use reticulate.
- `#261 <https://github.com/iiasa/ixmp/pull/261>`_: Increase minimum pandas
  version to 1.0; adjust for `API changes and deprecations <https://pandas.pydata.org/pandas-docs/version/1.0.0/whatsnew/v1.0.0.html#backwards-incompatible-api-changes>`_.
- `#243 <https://github.com/iiasa/ixmp/pull/243>`_: Add :meth:`.export_timeseries_data` to write data for multiple scenarios to CSV.
- `#264 <https://github.com/iiasa/ixmp/pull/264>`_: Implement methods to get and create new subannual timeslices.

v2.0.0 (2020-01-14)
===================

ixmp v2.0.0 coincides with message_ix v2.0.0.

Migration notes
---------------

Support for **Python 2.7 is dropped** as it has reached end-of-life, meaning no further releases will be made even to fix bugs.
See `PEP-0373 <https://www.python.org/dev/peps/pep-0373/>`_ and https://python3statement.org.
``ixmp`` users must upgrade to Python 3.

**Configuration** for ixmp and its storage backends has been streamlined.
See the ref:`Configuration` section of the documentation for complete details on how to use ``ixmp platform add`` register local and remote databases.
To migrate from pre-2.0 settings:

DB_CONFIG_PATH
   …pointed to a directory containing database properties (.properties) files.

   - All Platform configuration is stored in one ixmp configuration file, config.json, and manipulated using the ``ixmp platform`` command and subcommands.
   - The :class:`.Platform` constructor accepts the name of a stored platform configuration.
   - Different storage backends may accept relative or absolute paths to backend-specific configuration files.

DEFAULT_DBPROPS_FILE
   …gave a default backend via a file path.

   - On the command line, use ``ixmp platform add default NAME`` to set ``NAME`` as the default platform.
   - This platform is loaded when ``ixmp.Platform()`` is called without any arguments.

DEFAULT_LOCAL_DB_PATH
   …pointed to a default *local* database.

   - :obj:`.ixmp.config` always contains a platform named 'local' that is located below the configuration path, in the directory 'localdb/default'.
   - To change the location for this platform, use e.g.: ``ixmp platform add local jdbc hsqldb PATH``.

All changes
-----------

- `#240 <https://github.com/iiasa/ixmp/pull/240>`_: Add ``ixmp list`` command-line tool.
- `#225 <https://github.com/iiasa/ixmp/pull/225>`_: Ensure filters are always converted to string.
- `#189 <https://github.com/iiasa/ixmp/pull/189>`_: Identify and load Scenarios using URLs.
- `#182 <https://github.com/iiasa/ixmp/pull/182>`_,
  `#200 <https://github.com/iiasa/ixmp/pull/200>`_,
  `#213 <https://github.com/iiasa/ixmp/pull/213>`_,
  `#217 <https://github.com/iiasa/ixmp/pull/217>`_,
  `#230 <https://github.com/iiasa/ixmp/pull/230>`_,
  `#245 <https://github.com/iiasa/ixmp/pull/245>`_,
  `#246 <https://github.com/iiasa/ixmp/pull/246>`_: Add new Backend, Model APIs and CachingBackend, JDBCBackend, GAMSModel classes.
- `#188 <https://github.com/iiasa/ixmp/pull/188>`_,
  `#195 <https://github.com/iiasa/ixmp/pull/195>`_: Enhance reporting.
- `#177 <https://github.com/iiasa/ixmp/pull/177>`_: Add ability to pass `gams_args` through :meth:`.solve`.
- `#175 <https://github.com/iiasa/ixmp/pull/175>`_,
  `#239 <https://github.com/iiasa/ixmp/pull/239>`_: Drop support for Python 2.7.
- `#174 <https://github.com/iiasa/ixmp/pull/174>`_: Set `convertStrings=True` for JPype >= 0.7; see the `JPype changelog <https://jpype.readthedocs.io/en/latest/CHANGELOG.html>`_.
- `#173 <https://github.com/iiasa/ixmp/pull/173>`_: Make AppVeyor CI more robust; support pandas 0.25.0.
- `#165 <https://github.com/iiasa/ixmp/pull/165>`_: Add support for handling geodata.
- `#232 <https://github.com/iiasa/ixmp/pull/232>`_: Fix exposing whole config file to log output.

v0.2.0 (2019-06-25)
===================

ixmp 0.2.0 provides full support for :meth:`~.Scenario.clone` across platforms (database instances), e.g. from a remote database to a local HSQL database.
IAMC-style timeseries data is better supported, and can be used to store processed results, together with model variables and equations.

Other improvements include a new, dedicated :mod:`.ixmp.testing` module, and user-supplied callbacks in :meth:`.solve`.
The ``retixmp`` package using reticulate to access the ixmp API is renamed to ``rixmp`` and now has its own unit tests (the former ``rixmp`` package can be accessed as ``rixmp.legacy``).

Release 0.2.0 coincides with MESSAGEix release 1.2.0.

All changes
-----------

- `#135 <https://github.com/iiasa/ixmp/pull/135>`_: Test ``rixmp`` (former ``retixmp``) using the R ``testthat`` package.
- `#142 <https://github.com/iiasa/ixmp/pull/142>`_: Cloning across platforms, better support of IAMC_style timeseries data, preparations for MESSAGEix release 1.2 in Java core.
- `#115 <https://github.com/iiasa/ixmp/pull/115>`_: Support iterating with user-supplied callbacks.
- `#130 <https://github.com/iiasa/ixmp/pull/130>`_: Recognize ``IXMP_DATA`` environment variable for configuration and local databases.
- `#129 <https://github.com/iiasa/ixmp/pull/129>`_,
  `#132 <https://github.com/iiasa/ixmp/pull/132>`_: Fully implement :meth:`~.Scenario.clone` across platforms (databases).
- `#128 <https://github.com/iiasa/ixmp/pull/128>`_,
  `#137 <https://github.com/iiasa/ixmp/pull/137>`_: New module :mod:`ixmp.testing` for reuse of testing utilities.
- `#125 <https://github.com/iiasa/ixmp/pull/125>`_: Add functions to view and add regions for IAMC-style timeseries data.
- `#123 <https://github.com/iiasa/ixmp/pull/123>`_: Return absolute path from ``find_dbprops()``.
- `#118 <https://github.com/iiasa/ixmp/pull/118>`_: Switch to RTD Sphinx theme.
- `#116 <https://github.com/iiasa/ixmp/pull/116>`_: Bugfix and extend functionality for working with IAMC-style timeseries data.
- `#111 <https://github.com/iiasa/ixmp/pull/111>`_: Add functions to check if a Scenario has an item (set, par, var, equ).
- `#110 <https://github.com/iiasa/ixmp/pull/110>`_: Generalize the internal functions to format index dimensions for mapping sets and parameters.
- `#108 <https://github.com/iiasa/ixmp/pull/105>`_: Improve documentation.
- `#105 <https://github.com/iiasa/ixmp/pull/105>`_: Replace `deprecated <http://pandas.pydata.org/pandas-docs/stable/indexing.html#ix-indexer-is-deprecated>`_ pandas ``.ix`` indexer with ``.iloc``.
- `#103 <https://github.com/iiasa/ixmp/pull/103>`_: Specify dependencies in setup.py.

v0.1.3 (2018-11-21)
===================

- `#88 <https://github.com/iiasa/ixmp/pull/80>`_: Connecting to multiple databases, updating MESSAGE-scheme scenario specifications to version 1.1.
- `#80 <https://github.com/iiasa/ixmp/pull/80>`_: Can now set logging level which is harmonized between Java and Python.
- `#79 <https://github.com/iiasa/ixmp/pull/79>`_: Adding a deprecated-warning for `ixmp.Scenario` with `scheme=='MESSAGE'`.
- `#76 <https://github.com/iiasa/ixmp/pull/76>`_: Changing the API from ``mp.Scenario(...)`` to ``ixmp.Scenario(mp, ...)``.
- `#73 <https://github.com/iiasa/ixmp/pull/73>`_: Adding a function :meth:`~.Scenario.has_solution`, rename kwargs to `..._solution`.
- `#69 <https://github.com/iiasa/ixmp/pull/69>`_: Bring retixmp available to other users.
- `#64 <https://github.com/iiasa/ixmp/pull/64>`_: Support writing multiple sheets to Excel in utils.pd_write.
- `#61 <https://github.com/iiasa/ixmp/pull/61>`_: Now able to connect to multiple databases (Platforms).
- `#58 <https://github.com/iiasa/ixmp/pull/58>`_: Add MacOSX support in CI.
- `#52 <https://github.com/iiasa/ixmp/pull/52>`_: Add ability to load all scenario data into memory for fast subsequent computation.
