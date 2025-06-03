.. Next release
.. ============

.. All changes
.. -----------

.. _v3.11.1:

v3.11.1 (2025-06-03)
====================

- Quiet warnings occurring with ixmp4 v0.10.0 and pandera v0.24 (:pull:`579`).

.. _v3.11.0:

v3.11.0 (2025-05-26)
====================

- Add :class:`.IXMP4Backend` as an alternative to :class:`.JDBCBackend` (:pull:`552`, :pull:`568`. :pull:`570`).
  Please read the usage notes at :mod:`.backend.ixmp4` and :class:`.IXMP4Backend`,
  and the linked `support roadmap for ixmp4 <https://github.com/iiasa/message_ix/discussions/939>`_.

  - New optional dependencies set ``ixmp[ixmp4]`` including ixmp4 version 0.10 (:pull:`552`, :pull:`576`).
  - Improve the :program:`ixmp platform add` :doc:`command <cli>` to support adding :class:`.Platform` with :class:`.IXMP4Backend` (:pull:`575`).

- Refine the method of locating the GAMS :attr:`~.GAMSInfo.executable` (:pull:`564`, :issue:`456`, :issue:`523`, :issue:`563`).
- Update installation instructions to align with current |MESSAGEix| install documentation (:pull:`577`).

.. _v3.10.0:

v3.10.0 (2025-02-19)
====================

- :mod:`ixmp` is tested and compatible with `Python 3.13 <https://www.python.org/downloads/release/python-3130/>`__ (:pull:`544`).
- Support for Python 3.8 is dropped (:pull:`544`), as it has reached end-of-life.
- :mod:`ixmp` locates GAMS API libraries needed for the Java code underlying :class:`.JDBCBackend` based on the system GAMS installation (:pull:`532`).
  As a result:

  - :class:`.JDBCBackend` is usable on MacOS with newer, ``arm64``-architecture processors and Python/GAMS compiled for ``arm64`` (:issue:`473`, :issue:`531`).
  - GAMS API libraries are no longer (re-)packaged with ixmp in the directory :file:`ixmp/backend/jdbc/`.

.. _v3.9.0:

v3.9.0 (2024-06-04)
===================

- Increase minimum required version of genno dependency to 1.20 (:pull:`514`).
- To aid debugging when execution fails, :class:`.GAMSModel` also displays the path to the GAMS log file (:pull:`513`).

.. _v3.8.0:

v3.8.0 (2024-01-12)
===================

Migration notes
---------------

Update code that imports from the following modules:

- :py:`ixmp.reporting` → use :mod:`ixmp.report`.
- :py:`ixmp.reporting.computations` → use :mod:`ixmp.report.operator`.
- :py:`ixmp.utils` → use :mod:`ixmp.util`.

Code that imports from the old locations will continue to work, but will raise :class:`DeprecationWarning`.

All changes
-----------

- :mod:`ixmp` is tested and compatible with `Python 3.12 <https://www.python.org/downloads/release/python-3120/>`__ (:pull:`504`).
- Support for Python 3.7 is dropped (:pull:`492`), as it has reached end-of-life.
- Rename :mod:`ixmp.report` and :mod:`ixmp.util` (:pull:`500`).
- New option :py:`record_version_packages` to :class:`.GAMSModel` (:pull:`502`).
  Versions of the named Python packages are recorded in a special set in GDX-format input and output files to help associate these files with generating code.
- New reporting operators :func:`.from_url`, :func:`.get_ts`, and :func:`.remove_ts` (:pull:`500`).
- New CLI command :program:`ixmp platform copy` and :doc:`CLI documentation <cli>` (:pull:`500`).
- New argument :py:`indexed_by=...` to :meth:`.Scenario.items` (thus :meth:`.Scenario.par_list` and similar methods) to iterate over (or list) only items that are indexed by a particular set (:issue:`402`, :pull:`500`).
- New :func:`.util.discard_on_error` and matching argument to :meth:`.TimeSeries.transact` to avoid locking :class:`.TimeSeries` / :class:`.Scenario` on failed operations with :class:`.JDBCBackend` (:pull:`488`).
- Work around limitations of :class:`.JDBCBackend` (:pull:`500`):

  - Unit :py:`""` cannot be added with the Oracle driver (:issue:`425`).
  - Certain items (variables) could not be initialized when providing :py:`idx_sets=...`, even if those match the sets fixed by the underlying Java code.
    With this fix, a matching list is silently accepted; a different list raises :class:`NotImplementedError`.
  - When a :class:`.GAMSModel` is solved with an LP status of 5 (optimal, but with infeasibilities after unscaling), :class:`.JDBCBackend` would attempt to read the output GDX file and fail, leading to an uninformative error message (:issue:`98`).
    Now :class:`.ModelError` is raised describing the situation.
- Improved type hinting for static typing of code that uses :mod:`ixmp` (:issue:`465`, :pull:`500`).
- :mod:`ixmp` requires JPype1 1.4.0 or earlier, for Python 3.10 and earlier (:pull:`504`).
  With JPype1 1.4.1 and later, memory management in :class:`.CachingBackend` may not function as intended (:issue:`463`), which could lead to high memory use where many, large :class:`.Scenario` objects are created and used in a single Python program.
  (For Python 3.11 and later, any version of JPype1 from the prior minimum (1.2.1) to the latest is supported.)

.. _v3.7.0:

v3.7.0 (2023-05-17)
===================

- :mod:`ixmp` is tested and compatible with `Python 3.11 <https://www.python.org/downloads/release/python-3110/>`__ (:pull:`481`).
- :mod:`ixmp` is tested and compatible with `pandas 2.0.0 <https://pandas.pydata.org/pandas-docs/version/2.0/whatsnew/v2.0.0.html>`__ (:pull:`471`).
  Note that `pandas 1.4.0 dropped support for Python 3.7 <https://pandas.pydata.org/docs/whatsnew/v1.4.0.html#increased-minimum-version-for-python>`__: thus while :mod:`ixmp` still supports Python 3.7 this is achieved with pandas 1.3.x, which may not receive further updates (the last patch release was in December 2021).
  Support for Python 3.7 will be dropped in a future version of :mod:`ixmp`, and users are encouraged to upgrade to a newer version of Python.
- Bugfix: `year` argument to :meth:`.TimeSeries.timeseries` accepts :class:`int` or :class:`list` of :class:`int` (:issue:`440`, :pull:`469`).
- Adjust to pandas 1.5.0 (:pull:`458`).
- New module :mod:`.util.sphinx_linkcode_github` to link documentation to source code on GitHub (:pull:`459`).

.. _v3.6.0:

v3.6.0 (2022-08-17)
===================

- Optionally tolerate failures to add individual items in :func:`.store_ts` reporting computation (:pull:`451`); use ``timeseries_only=True`` in check-out to function with :class:`.Scenario` with solution data stored.
- Bugfix: :class:`.Config` squashed configuration values read from :file:`config.json`, if the respective keys were registered in downstream packages, e.g. :mod:`message_ix`.
  Allow the values loaded from file to persist (:pull:`451`).
- Adjust to genno 1.12 and set this as the minimum required version for :mod:`ixmp.reporting <ixmp.report>` (:pull:`451`).
- Add :meth:`.enforce` to the :class:`~.base.Model` API for enforcing structure/data consistency before :meth:`.Model.run` (:pull:`450`).

.. _v3.5.0:

v3.5.0 (2022-05-06)
===================

- Add new logo and diagram to the documentation (:pull:`446`).
- Raise an informative :class:`ValueError` when adding infinite values with :meth:`.add_timeseries`; this is unsupported on :class:`.JDBCBackend` when connected to an Oracle database (:pull:`443`, :issue:`442`).
- New attribute :attr:`.url` for convenience in constructing :class:`.TimeSeries`/:class:`.Scenario` URLS (:pull:`444`).
- New :func:`.store_ts` reporting computation for storing time-series data on a :class:`.TimeSeries`/:class:`.Scenario` (:pull:`444`).
- Improve performance in :meth:`.add_par` (:pull:`441`).
- Minimum requirements are increased for dependencies (:pull:`435`):

  - Python 3.7 or greater. Python 3.6 reached end-of-life on 2021-12-31.
  - Pandas 1.2 (2020-12-26) or greater, the oldest version with a minimum Python version of 3.7.

- Improvements to configuration (:pull:`435`):

  - The `jvmargs` argument to :class:`.JDBCBackend` can be set via the command line (:program:`ixmp platform add …`) or :meth:`.Config.add_platform`; see :ref:`configuration` (:issue:`408`).
  - Bug fix: user config file values from downstream packages (e.g. :mod:`message_ix`) are respected (:issue:`415`).

- Security: upgrade Log4j to 2.17.1 in Java code underlying :class:`.JDBCBackend` to address `CVE-2021-44228 <https://nvd.nist.gov/  vuln/detail/CVE-2021-44228>`_, a.k.a. “Log4Shell” (:pull:`445`).

  The ixmp Python package is not network-facing *per se* (unless exposed as such by user code; we are not aware of any such applications), so remote code execution attacks are not a significant concern.
  However, users should still avoid running unknown or untrusted code provided by third parties with versions of ixmp prior to 3.5.0, as such code could be deliberately crafted to exploit the vulnerability.

.. _v3.4.0:

v3.4.0 (2022-01-24)
===================

Migration notes
---------------

:py:`ixmp.util.isscalar()` is deprecated.
Code should use :func:`numpy.isscalar`.

All changes
-----------

- Add :meth:`.TimeSeries.transact`, for wrapping data manipulations in :meth:`~.TimeSeries.check_out` and :meth:`~.TimeSeries.commit` operations (:pull:`422`).
- Add :doc:`data-model`, a documentation page giving a complete description of the :mod:`ixmp` data model (:pull:`422`).
- Add the :command:`pytest --user-config` command-line option, to use user's local configuration when testing (:pull:`422`).
- Adjust :func:`.format_scenario_list` for changes in :mod:`pandas` 1.3.0 (:pull:`421`).

.. _v3.3.0:

v3.3.0 (2021-05-28)
===================

Migration notes
---------------

``rixmp`` is deprecated, though not yet removed, as newer versions of the R `reticulate <https://rstudio.github.io/reticulate/>`_ package allow direct import and use of the Python modules with full functionality.
See the updated page for :doc:`api-r`.


All changes
-----------

- Add ``ixmp config show`` CLI command (:pull:`416`).
- Add :mod:`genno` and :mod:`message_ix_models` to the output of :func:`.show_versions` / ``ixmp show-versions`` (:pull:`416`).
- Clean up test suite, improve performance, increase coverage (:pull:`416`).
- Adjust documentation for deprecation of ``rixmp`` (:pull:`416`).
- Deprecate :func:`.util.logger` (:pull:`399`).
- Add a `quiet` option to :class:`.GAMSModel` and use in testing (:pull:`399`).
- Fix :class:`.GAMSModel` would try to write GDX data to filenames containing invalid characters on Windows (:pull:`398`).
- Format user-friendly exceptions when GAMSModel errors (:issue:`383`, :pull:`398`).
- Adjust :mod:`ixmp.reporting <ixmp.report>` to use :mod:`genno` (:pull:`397`).
- Fix two minor bugs in reporting (:pull:`396`).

.. _v3.2.0:

v3.2.0 (2021-01-24)
===================

- Increase JPype minimum version to 1.2.1 (:pull:`394`).
- Adjust test suite for pandas v1.2.0 (:pull:`391`).
- Raise clearer exceptions from :meth:`.add_par` for incorrect parameters; silently handle empty data (:pull:`374`).
- Depend on :mod:`openpyxl` instead of :py:`xlrd` and :py:`xlsxwriter` for Excel I/O; :py:`xlrd` versions 2.0.0 and later do not support :file:`.xlsx` (:pull:`389`).
- Add a parameter for exporting all model+scenario run versions to :meth:`.Platform.export_timeseries_data`, and fix a bug where exporting all runs happens uninteneded (:pull:`367`).
- Silence noisy output from ignored exceptions on JDBCBackend/JVM shutdown (:pull:`378`).
- Add a utility method, :func:`.gams_version`, to check the installed version of GAMS (:pull:`376`).
  The result is displayed by the ``ixmp show-versions`` CLI command/:func:`.show_versions`.
- :meth:`.init_par` and related methods accept any sequence (not merely :class:`list`) of :class:`str` for the `idx_sets` and `idx_names` arguments (:pull:`376`).

.. _v3.1.0:

v3.1.0 (2020-08-28)
===================

ixmp v3.1.0 coincides with message_ix v3.1.0.

- Fix a bug in :meth:`.read_excel` when parameter data is spread across multiple sheets (:pull:`345`).
- Expand documentation and revise installation instructions (:pull:`363`).
- Raise Python exceptions from :class:`.JDBCBackend` (:pull:`362`).
- Add :meth:`.Scenario.items`, :func:`.util.diff`, and allow using filters in CLI command ``ixmp export`` (:pull:`354`).
- Add functionality for storing ‘meta’ (annotations of model names, scenario names, versions, and some combinations thereof) (:pull:`353`).

  - Add :meth:`.Backend.add_model_name`, :meth:`~.Backend.add_scenario_name`, :meth:`~.Backend.get_model_names`, :meth:`~.Backend.get_scenario_names`, :meth:`~.Backend.get_meta`, :meth:`~.Backend.set_meta`, :meth:`~.Backend.remove_meta`.
  - Allow these to be called from :class:`.Platform` instances.
  - Remove :py:`Scenario.delete_meta()`.

- Avoid modifying indexers dictionary in :meth:`AttrSeries.sel <genno.core.attrseries.AttrSeries.sel>` (:pull:`349`).
- Add region/unit parameters to :meth:`.Platform.export_timeseries_data` (:pull:`343`).
- Preserve dtypes of index columns in :func:`.data_for_quantity` (:pull:`347`).
- ``ixmp show-versions`` includes the path to the default JVM used by JDBCBackend/JPype (:pull:`339`).
- Make :class:`reporting.Quantity <genno.Quantity>` classes interchangeable (:pull:`317`).
- Use GitHub Actions for continuous testing and integration (:pull:`330`).

.. _v3.0.0:

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

- Bump JPype dependency to 0.7.5 (:pull:`327`).
- Improve memory management in :class:`.JDBCBackend` (:pull:`298`).
- Raise user-friendly exceptions from :meth:`Reporter.get <genno.Computer.get>` in Jupyter notebooks and other read–evaluate–print loops (REPLs) (:pull:`316`).
- Ensure :meth:`.Model.initialize` is always called for new *and* cloned objects (:pull:`315`).
- Add CLI command `ixmp show-versions` to print ixmp and dependency versions for debugging (:pull:`320`).
- Bulk saving for metadata and exposing documentation AP (:pull:`314`)I
- Add :func:`~.genno.operator.apply_units`, :func:`~.genno.operator.select` reporting operators; expand :meth:`Reporter.add <genno.Computer.add>` (:pull:`312`).
- :func:`Reporter.add_product <genno.operator.mul>` accepts a :class:`~.genno.Key` with a tag; :func:`~.genno.operator.aggregate` preserves :class:`~.genno.Quantity` attributes (:pull:`310`).
- Add CLI command ``ixmp solve`` to run model solver (:pull:`304`).
- Add `dims` and `units` arguments to :func:`Reporter.add_file <genno.operator.load_file>`; remove :py:`Reporter.read_config()` (redundant with :meth:`Reporter.configure <genno.Computer.configure>`) (:pull:`303`).
- Add option to include `subannual` column in dataframe returned by :meth:`.TimeSeries.timeseries` (:pull:`295`).
- Add :meth:`.Scenario.to_excel` and :meth:`.read_excel`; this functionality is transferred to ixmp from :mod:`message_ix` and enhanced for dealing with maximum row limits in Excel (:pull:`286`, :pull:`297`, :pull:`309`).
- Include all tests in the ixmp package (:pull:`270`).
- Add :meth:`.Model.initialize` API to help populate new Scenarios according to a model scheme (:pull:`212`).
- Apply units to reported quantities (:pull:`267`).
- Increase minimum pandas version to 1.0; adjust for `API changes and deprecations <https://pandas.pydata.org/pandas-docs/version/1.0.0/whatsnew/v1.0.0.html#backwards-incompatible-api-changes>`_ (:pull:`261`).
- Add :meth:`.export_timeseries_data` to write data for multiple scenarios to CSV (:pull:`243`).
- Implement methods to get and create new subannual timeslices (:pull:`264`).

.. _v2.0.0:

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

- Add ``ixmp list`` command-line tool (:pull:`240`).
- Ensure filters are always converted to string (:pull:`225`).
- Identify and load Scenarios using URLs (:pull:`189`).
- Add new Backend, Model APIs and CachingBackend, JDBCBackend, GAMSModel classes (:pull:`182`, :pull:`200`, :pull:`213`, :pull:`217`, :pull:`230`, :pull:`245`, :pull:`246`).
- Enhance reporting (:pull:`188`, :pull:`195`).
- Add ability to pass `gams_args` through :meth:`.solve` (:pull:`177`).
- Drop support for Python 2.7 (:pull:`175`, :pull:`239`).
- Set `convertStrings=True` for JPype >= 0.7; see the `JPype changelog <https://jpype.readthedocs.io/en/latest/CHANGELOG.html>`_ (:pull:`174`).
- Make AppVeyor CI more robust; support pandas 0.25.0 (:pull:`173`).
- Add support for handling geodata (:pull:`165`).
- Fix exposing whole config file to log output (:pull:`232`).

.. _v0.2.0:

v0.2.0 (2019-06-25)
===================

ixmp 0.2.0 provides full support for :meth:`~.Scenario.clone` across platforms (database instances), e.g. from a remote database to a local HSQL database.
IAMC-style timeseries data is better supported, and can be used to store processed results, together with model variables and equations.

Other improvements include a new, dedicated :mod:`.ixmp.testing` module, and user-supplied callbacks in :meth:`.solve`.
The ``retixmp`` package using reticulate to access the ixmp API is renamed to ``rixmp`` and now has its own unit tests (the former ``rixmp`` package can be accessed as ``rixmp.legacy``).

Release 0.2.0 coincides with MESSAGEix release 1.2.0.

All changes
-----------

- Test ``rixmp`` (former ``retixmp``) using the R ``testthat`` package (:pull:`135`).
- Cloning across platforms, better support of IAMC_style timeseries data, preparations for MESSAGEix release 1.2 in Java core (:pull:`142`).
- Support iterating with user-supplied callbacks (:pull:`115`).
- Recognize ``IXMP_DATA`` environment variable for configuration and local databases (:pull:`130`).
- Fully implement :meth:`~.Scenario.clone` across platforms (databases) (:pull:`129`, :pull:`132`).
- New module :mod:`ixmp.testing` for reuse of testing utilities (:pull:`128`, :pull:`137`).
- Add functions to view and add regions for IAMC-style timeseries data (:pull:`125`).
- Return absolute path from ``find_dbprops()`` (:pull:`123`).
- Switch to RTD Sphinx theme (:pull:`118`).
- Bugfix and extend functionality for working with IAMC-style timeseries data (:pull:`116`).
- Add functions to check if a Scenario has an item (set, par, var, equ) (:pull:`111`).
- Generalize the internal functions to format index dimensions for mapping sets and parameters (:pull:`110`).
- Improve documentation (:pull:`108`).
- Replace `deprecated <http://pandas.pydata.org/pandas-docs/stable/indexing.html#ix-indexer-is-deprecated>`_ pandas ``.ix`` indexer with ``.iloc`` (:pull:`105`).
- Specify dependencies in setup.py (:pull:`103`).

.. _v0.1.3:

v0.1.3 (2018-11-21)
===================

- Connecting to multiple databases, updating MESSAGE-scheme scenario specifications to version 1.1 (:pull:`88`).
- Can now set logging level which is harmonized between Java and Python (:pull:`80`).
- Adding a deprecated-warning for `ixmp.Scenario` with `scheme=='MESSAGE'` (:pull:`79`).
- Changing the API from ``mp.Scenario(...)`` to ``ixmp.Scenario(mp, ...)`` (:pull:`76`).
- Adding a function :meth:`~.Scenario.has_solution`, rename kwargs to `..._solution` (:pull:`73`).
- Bring retixmp available to other users (:pull:`69`).
- Support writing multiple sheets to Excel in utils.pd_write (:pull:`64`).
- Now able to connect to multiple databases (Platforms) (:pull:`61`).
- Add MacOSX support in CI (:pull:`58`).
- Add ability to load all scenario data into memory for fast subsequent computation (:pull:`52`).
