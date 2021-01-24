v3.2.0 (2021-01-24)
===================

All changes
-----------

- :pull:`394`: Increase JPype minimum version to 1.2.1.
- :pull:`391`: Adjust test suite for pandas v1.2.0.
- :pull:`374`: Raise clearer exceptions from :meth:`.add_par` for incorrect parameters; silently handle empty data.
- :pull:`389`: Depend on :mod:`openpyxl` instead of :mod:`xlrd` and :mod:`xlsxwriter` for Excel I/O; :mod:`xlrd` versions 2.0.0 and later do not support :file:`.xlsx`.
- :pull:`367`: Add a parameter for exporting all model+scenario run versions to :meth:`.Platform.export_timeseries_data`, and fix a bug where exporting all runs happens uninteneded.
- :pull:`378`: Silence noisy output from ignored exceptions on JDBCBackend/JVM shutdown.
- :pull:`376`: Add a utility method, :func:`.gams_version`, to check the installed version of GAMS.
  The result is displayed by the ``ixmp show-versions`` CLI command/:func:`.show_versions`.
- :pull:`376`: :meth:`.init_par` and related methods accept any sequence (not merely :class:`list`) of :class:`str` for the `idx_sets` and `idx_names` arguments.


v3.1.0 (2020-08-28)
===================

All changes
-----------

ixmp v3.1.0 coincides with message_ix v3.1.0.

- :pull:`345`: Fix a bug in :meth:`.read_excel` when parameter data is spread across multiple sheets.
- :pull:`363`: Expand documentation and revise installation instructions.
- :pull:`362`: Raise Python exceptions from :class:`.JDBCBackend`.
- :pull:`354`: Add :meth:`Scenario.items`, :func:`.utils.diff`, and allow using filters in CLI command ``ixmp export``.
- :pull:`353`: Add functionality for storing ‘meta’ (annotations of model names, scenario names, versions, and some combinations thereof).

  - Add :meth:`.Backend.add_model_name`, :meth:`~.Backend.add_scenario_name`, :meth:`~.Backend.get_model_names`, :meth:`~.Backend.get_scenario_names`, :meth:`~.Backend.get_meta`, :meth:`~.Backend.set_meta`, :meth:`~.Backend.remove_meta`.
  - Allow these to be called from :class:`.Platform` instances.
  - Remove :meth:`.Scenario.delete_meta`.

- :pull:`349`: Avoid modifying indexers dictionary in :meth:`.AttrSeries.sel`.
- :pull:`343`: Add region/unit parameters to :meth:`.Platform.export_timeseries_data`.
- :pull:`347`: Preserve dtypes of index columns in :func:`.data_for_quantity`.
- :pull:`339`: ``ixmp show-versions`` includes the path to the default JVM used by JDBCBackend/JPype.
- :pull:`317`: Make :class:`reporting.Quantity` classes interchangeable.
- :pull:`330`: Use GitHub Actions for continuous testing and integration.


v3.0.0 (2020-06-05)
===================

ixmp v3.0.0 coincides with message_ix v3.0.0.

Migration notes
---------------

Excel input/output (I/O)
   The file format used by :meth:`.Scenario.to_excel` and :meth:`.read_excel` is now fully specified; see :doc:`file-io`.

   ixmp writes and reads items with more elements than the ~10⁶ row maximum of the Excel data format, by splitting these across multiple sheets.

   The I/O code now explicitly checks for situations where the index *sets* and *names* for an item are ambiguous; see :ref:`this example <excel-ambiguous-dims>` for how to initialize and read these items.

Updated dependencies
   The minimum versions of the following dependencies are increased:

   - JPype1 0.7.5
   - pandas 1.0
   - dask 2.14 (for reporting)

Deprecations and deprecation policy
   The following items, marked as deprecated in ixmp 2.0, are removed (:pull:`254`):

   - :file:`$HOME/.local/ixmp/` as a configuration location.
     Configuration files are now placed in the standard :file:`$HOME/.local/share/ixmp/`.
   - positional and ``dbtype=`` arguments to :class:`.Platform`/:class:`.JDBCBackend`.
   - ``first_model_year=``, ``keep_sol=``, and ``scen=`` arguments to :meth:`~.Scenario.clone`.
     Use `shift_first_model_year`, `keep_solution`, and `scenario`, respectively.
   - ``rixmp.legacy``, an earlier version of :ref:`the R interface <rixmp>` that did not use reticulate.

   Newly deprecated is:

   - `cache` keyword argument to :class:`.Scenario`.
     Caching is controlled at the :class:`.Platform`/Backend level, using the same keyword argument.

   Starting with ixmp v3.0, arguments and other features marked as deprecated will follow a standard deprecation policy: they will be removed no sooner than the second major release following the release in which they are marked deprecated.
   For instance, a feature marked deprecated in ixmp version "10.5" would be retained in ixmp versions "11.x", and removed only in version "12.0" or later.


All changes
-----------

- :pull:`327`: Bump JPype dependency to 0.7.5.
- :pull:`298`: Improve memory management in :class:`.JDBCBackend`.
- :pull:`316`: Raise user-friendly exceptions from :meth:`.Reporter.get` in Jupyter notebooks and other read–evaluate–print loops (REPLs).
- :pull:`315`: Ensure :meth:`.Model.initialize` is always called for new *and* cloned objects.
- :pull:`320`: Add CLI command `ixmp show-versions` to print ixmp and dependency versions for debugging.
- :pull:`314`: Bulk saving for metadata and exposing documentation API
- :pull:`312`: Add :meth:`~.computations.apply_units`, :meth:`~computations.select` reporting calculations; expand :meth:`.Reporter.add`.
- :pull:`310`: :meth:`.Reporter.add_product` accepts a :class:`.Key` with a tag; :func:`~.computations.aggregate` preserves :class:`.Quantity` attributes.
- :pull:`304`: Add CLI command ``ixmp solve`` to run model solver.
- :pull:`303`: Add `dims` and `units` arguments to :meth:`Reporter.add_file`; remove :meth:`Reporter.read_config` (redundant with :meth:`Reporter.configure`).
- :pull:`295`: Add option to include `subannual` column in dataframe returned by :meth:`.TimeSeries.timeseries`.
- :pull:`286`,
  :pull:`297`,
  :pull:`309`: Add :meth:`.Scenario.to_excel` and :meth:`.read_excel`; this functionality is transferred to ixmp from :mod:`message_ix` and enhanced for dealing with maximum row limits in Excel.
- :pull:`270`: Include all tests in the ixmp package.
- :pull:`212`: Add :meth:`Model.initialize` API to help populate new Scenarios according to a model scheme.
- :pull:`267`: Apply units to reported quantities.
- :pull:`261`: Increase minimum pandas version to 1.0; adjust for `API changes and deprecations <https://pandas.pydata.org/pandas-docs/version/1.0.0/whatsnew/v1.0.0.html#backwards-incompatible-api-changes>`_.
- :pull:`243`: Add :meth:`.export_timeseries_data` to write data for multiple scenarios to CSV.
- :pull:`264`: Implement methods to get and create new subannual timeslices.


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

- :pull:`240`: Add ``ixmp list`` command-line tool.
- :pull:`225`: Ensure filters are always converted to string.
- :pull:`189`: Identify and load Scenarios using URLs.
- :pull:`182`,
  :pull:`200`,
  :pull:`213`,
  :pull:`217`,
  :pull:`230`,
  :pull:`245`,
  :pull:`246`: Add new Backend, Model APIs and CachingBackend, JDBCBackend, GAMSModel classes.
- :pull:`188`,
  :pull:`195`: Enhance reporting.
- :pull:`177`: Add ability to pass `gams_args` through :meth:`.solve`.
- :pull:`175`,
  :pull:`239`: Drop support for Python 2.7.
- :pull:`174`: Set `convertStrings=True` for JPype >= 0.7; see the `JPype changelog <https://jpype.readthedocs.io/en/latest/CHANGELOG.html>`_.
- :pull:`173`: Make AppVeyor CI more robust; support pandas 0.25.0.
- :pull:`165`: Add support for handling geodata.
- :pull:`232`: Fix exposing whole config file to log output.

v0.2.0 (2019-06-25)
===================

ixmp 0.2.0 provides full support for :meth:`~.Scenario.clone` across platforms (database instances), e.g. from a remote database to a local HSQL database.
IAMC-style timeseries data is better supported, and can be used to store processed results, together with model variables and equations.

Other improvements include a new, dedicated :mod:`.ixmp.testing` module, and user-supplied callbacks in :meth:`.solve`.
The ``retixmp`` package using reticulate to access the ixmp API is renamed to ``rixmp`` and now has its own unit tests (the former ``rixmp`` package can be accessed as ``rixmp.legacy``).

Release 0.2.0 coincides with MESSAGEix release 1.2.0.

All changes
-----------

- :pull:`135`: Test ``rixmp`` (former ``retixmp``) using the R ``testthat`` package.
- :pull:`142`: Cloning across platforms, better support of IAMC_style timeseries data, preparations for MESSAGEix release 1.2 in Java core.
- :pull:`115`: Support iterating with user-supplied callbacks.
- :pull:`130`: Recognize ``IXMP_DATA`` environment variable for configuration and local databases.
- :pull:`129`,
  :pull:`132`: Fully implement :meth:`~.Scenario.clone` across platforms (databases).
- :pull:`128`,
  :pull:`137`: New module :mod:`ixmp.testing` for reuse of testing utilities.
- :pull:`125`: Add functions to view and add regions for IAMC-style timeseries data.
- :pull:`123`: Return absolute path from ``find_dbprops()``.
- :pull:`118`: Switch to RTD Sphinx theme.
- :pull:`116`: Bugfix and extend functionality for working with IAMC-style timeseries data.
- :pull:`111`: Add functions to check if a Scenario has an item (set, par, var, equ).
- :pull:`110`: Generalize the internal functions to format index dimensions for mapping sets and parameters.
- :pull:`108`: Improve documentation.
- :pull:`105`: Replace `deprecated <http://pandas.pydata.org/pandas-docs/stable/indexing.html#ix-indexer-is-deprecated>`_ pandas ``.ix`` indexer with ``.iloc``.
- :pull:`103`: Specify dependencies in setup.py.

v0.1.3 (2018-11-21)
===================

- :pull:`88`: Connecting to multiple databases, updating MESSAGE-scheme scenario specifications to version 1.1.
- :pull:`80`: Can now set logging level which is harmonized between Java and Python.
- :pull:`79`: Adding a deprecated-warning for `ixmp.Scenario` with `scheme=='MESSAGE'`.
- :pull:`76`: Changing the API from ``mp.Scenario(...)`` to ``ixmp.Scenario(mp, ...)``.
- :pull:`73`: Adding a function :meth:`~.Scenario.has_solution`, rename kwargs to `..._solution`.
- :pull:`69`: Bring retixmp available to other users.
- :pull:`64`: Support writing multiple sheets to Excel in utils.pd_write.
- :pull:`61`: Now able to connect to multiple databases (Platforms).
- :pull:`58`: Add MacOSX support in CI.
- :pull:`52`: Add ability to load all scenario data into memory for fast subsequent computation.
