.. currentmodule:: ixmp.backend

Storage back ends (:mod:`ixmp.backend`)
***************************************

:mod:`ixmp` includes two storage back ends:

- :class:`ixmp.backend.jdbc.JDBCBackend`,
  which can store data in many types of relational database management systems (RDBMS)
  that have Java DataBase Connector (JDBC) interfaces—hence its name.
- :class:`ixmp.backend.ixmp4.IXMP4Backend`,
  which uses the `ixmp4 <https://docs.ece.iiasa.ac.at/projects/ixmp4/>`_ package
  and in turn its storage options,
  including the SQLite and PostgreSQL RDBMS.

:mod:`ixmp` is extensible to support other methods of storing data:
in non-JDBC or -ixmp4 RDBMS, non-relational databases, local files, memory, or other ways.
Developers wishing to add such capabilities may subclass :class:`ixmp.backend.base.Backend` and implement its methods.

.. contents::
   :local:
   :backlinks: none

Provided backends
=================

.. autodata:: ixmp.backend.BACKENDS


.. currentmodule:: ixmp.backend.jdbc

JDBCBackend
-----------

.. autoclass:: ixmp.backend.jdbc.JDBCBackend
   :members: handle_config, read_file, write_file

   JDBCBackend supports:

   - Databases in local files (HyperSQL) using ``driver='hsqldb'`` and the *path* argument.
   - Remote, Oracle databases using ``driver='oracle'`` and the *url*, *username* and *password* arguments.
   - Temporary, in-memory databases using ``driver='hsqldb'`` and the *url* argument.
     Use the `url` parameter with the format ``jdbc:hsqldb:mem:[NAME]``, where [NAME] is any string::

       mp = ixmp.Platform(
           backend="jdbc",
           driver="hsqldb",
           url="jdbc:hsqldb:mem:temporary platform",
       )


   JDBCBackend caches values in memory to improve performance when repeatedly reading data from the same items with :meth:`.par`, :meth:`.equ`, or :meth:`.var`.

   .. tip:: If repeatedly accessing the same item with different *filters*:

      1. First, access the item by calling e.g. :meth:`.par` *without* any filters.
         This causes the full contents of the item to be loaded into cache.
      2. Then, access by making multiple :meth:`.par` calls with different *filters* arguments.
         The cache value is filtered and returned without further access to the database.

   .. tip:: Modifying an item by adding or deleting elements invalidates its cache.

   JDBCBackend has the following **limitations**:

   - The `comment` argument to :meth:`.Platform.add_unit` is limited to 64 characters.
   - Infinite floating-point values (:data:`numpy.inf`, :data:`math.inf`) cannot be stored using :meth:`.TimeSeries.add_timeseries` when using an Oracle database via ``driver='oracle'``.

   JDBCBackend's implementation allows the following kinds of file input and output:

   .. autosummary::

      read_file
      write_file

.. autofunction:: ixmp.backend.jdbc.start_jvm

.. currentmodule:: ixmp.backend.ixmp4

IXMP4Backend
------------

.. note::
   See :ref:`jdbc-vs-ixmp4`, below.

   As of :mod:`ixmp` version 3.11,
   IXMP4Backend uses :mod:`ixmp4` version 0.14.
   This is a development release;
   a 'stable' version 1.0 or later has not yet been released.
   For this reason:

   - The ixmp4 API is not yet finalized and may change at any time.
   - Complete documentation of the ixmp4 API itself is not yet available.

   The same holds for IXMP4Backend.
   Consequently you **may** but probably **should not** use it for 'production' scientific scenario work.

   As of version 0.14, :mod:`ixmp4` supports only Python 3.10 and above.

   - If you want to use IXMP4Backend,
     please ensure you are using a sufficiently recent Python version.
   - If you are restricted to Python 3.9 and below,
     please use JDBCBackend instead.

.. automodule:: ixmp.backend.ixmp4
   :members:
   :exclude-members: IXMP4Backend

.. autoclass:: ixmp.backend.ixmp4.IXMP4Backend
   :members:

.. automodule:: ixmp.util.ixmp4
   :members:

.. _jdbc-vs-ixmp4:

Differences between JDBCBackend and IXMP4Backend
================================================

The long-term goal is that :mod:`ixmp` used with IXMP4Backend
has **feature and performance parity** with JDBCBackend.
As of ixmp version 3.12, IXMP4Backend has support for *most* features.
This section lists known differences in behaviour when using IXMP4Backend,
compared to JDBCBackend.
This includes cases where:

1. Behaviour is the same, but performance is known to differ.
2. IXMP4Backend currently has different behaviour,
   but work is planned to address the difference.
3. IXMP4Backend has different behaviour, and this difference is permanent.

   This occurs where :mod:`ixmp4` intentionally omits certain functionality
   or implements different behaviour,
   IXMP4Backend does not cover the difference,
   and no work is planned to change this.
   In some cases, this is because inspection of publicly-visible :mod:`ixmp` user code
   shows no usage of certain features.
   The text “Not in visible use” is used.

For (1) and (2),
code that works with JDBCBackend will work or should work with IXMP4Backend.
For (3), code may need to be tested and adapted.

The differences are listed according to components of the :doc:`ixmp Python API <api>`,
with individual items and class attributes and methods in alphabetical order.
Behaviours that are identical are not mentioned.
For planned future work see the `support roadmap for ixmp4 <https://github.com/iiasa/message_ix/discussions/939>`_
and `issues labeled 'backend.ixmp4' <https://github.com/iiasa/ixmp/labels/backend.ixmp4>`_.

.. NOTE What do we do about major difference such as Run being the new Scenario, IndexSet and Table vs Set, etc?
.. Functionally, all things are equivalent, but these *are* changes we'd like people to adjust to eventually.

.. contents::
   :backlinks: none
   :local:

General behaviour
-----------------

Command-line interface (CLI)
   Some CLI tests do not pass yet with IXMP4Backend, but all should.
   In the meantime, use care with CLI commands including
   :program:`ixmp platform list` and :program:`ixmp solve`.

Default contents
   JDBCBackend pre-populates Platform and Scenario instances with certain contents.
   On IXMP4Backend, the configuration option :attr:`.ixmp4.Options.jdbc_compat`
   controls whether these default contents are created (:any:`True`) or not.
   See the sections below for further details.

   As of :mod:`ixmp` v3.12, these default contents only include items necessary
   for the test suite and tutorials.

Exceptions
   JDBCBackend raises exceptions from the Python standard exception hierarchy
   (:class:`RuntimeError`, :class:`ValueError`, :class:`KeyError`, etc.),
   or occasionally a generic :class:`IxException`.
   ixmp4 provides a large number of exception classes with specific meanings
   such as :class:`.RunLockRequired`, :class:`.NoDefaultRunFound`, or
   :class:`.OptimizationDataValidationError`
   that are not subclasses of the same standard exceptions.
   In cases covered by the ixmp test suite,
   IXMP4Backend intercepts ixmp4-specific exceptions
   and re-raises the same classes of exceptions as JDBCBackend.
   However:

   - Exception messages may differ.
   - In other cases, ixmp4 exceptions may be raised directly.

   Users **should** inspect code that uses Python :py:`try:` statements,
   :func:`isinstance` calls, or similar, to ensure it has the same behaviour
   when using IXMP4Backend.
   Code **may** be adapted by replacing single except types with tuples of types:

   .. code-block::

      try:
          ...
      # except RuntimeError:        # Old code: single exception type
      except (RuntimeError, ixmp4.exceptions.RunLockRequired):  # New
          ...

Platform class
--------------

Default contents
   If :attr:`.ixmp4.Options.jdbc_compat` is :any:`True`,
   regions and units are populated.
   See :issue:`608` for details.

:meth:`.Platform.add_region`
   - ixmp4 and IXMP4Backend do not and will not support the :py:`parent`
     argument.
     If passed, the argument is not stored, and warnings are logged.
   - IXMP4Backend supplies a default, meaningless :py:`hierarchy` value
     if no argument is given.

:meth:`.Platform.add_region_synonym`
   ixmp4 and IXMP4Backend do not and will not support 'synonyms' for region IDs.

:meth:`.Platform.add_timeslice`, :meth:`.Platform.timeslices`
   Not in visible use.
   ixmp4 does not and will not provide a dedicated way to handle :ref:`data-timeslice`
   for :ref:`data-tsdata`, so these methods are not supported on IXMP4Backend.

   When using ixmp4 directly,
   the :class:`ixmp4.data.db.iamc.datapoint.DataPoint` class
   and its :py:`type`, :py:`step_category`, :py:`step_year`, and
   :py:`step_datetime` methods can be used to define time slices.

:meth:`Platform.close_db <.base.Backend.close_db>` called twice
    JDBCBackend logs a warning if this method is called
    after the database connection has already been closed.
    IXMP4Backend does not;
    if the connection was already closed, the call has no effect.

    .. On IXMP4Backend there is no straightforward way to provide this:
       sqlalchemy does not provide a way to check if a database session
       or engine was closed.

:meth:`.Platform.export_timeseries_data`
   On JDBCBackend, the "meta" column (:py:`meta` parameter to :meth:`.TimeSeries.add_timeseries`)
   is exported as :type:`int` 0 or 1.
   On IXMP4Backend, it is exported as the string "False" or "True".

   .. NB see https://github.com/iiasa/ixmp_source/blob/889b51f7731b3fdfed2e241c3d6596723e83202e/src/main/resources/db/migration/postgresql/V1__postgresql_base_version.sql#L219

:meth:`Platform.get_doc <.Backend.get_doc>`, :meth:`set_doc <.Backend.set_doc>`
   On JDBCBackend, the argument :py:`domain="metadata"` is supported.
   On IXMP4Backend, it is not and will not be supported.

   Code that will use only IXMP4Backend **may** set values on :attr:`ixmp4.core.run.Run.meta`.

:meth:`Platform.get_meta <.Backend.get_meta>`, :meth:`remove_meta <.Backend.remove_meta>`, :meth:`set_meta <.Backend.set_meta>`
   On JDBCBackend, the :py:`meta=...` parameter can be a mapping of :class:`str` keys to arbitrary Python values,
   including :class:`list` and other collections.
   On IXMP4Backend, collections are not supported.

   On JDBCBackend, any one or two of the :py:`model=...`, :py:`scenario=...`, or :py:`version=...` parameters
   may be given.
   On IXMP4Backend, this is not supported; all 3 parameters are required.

:meth:`.Platform.set_log_level`
    On JDBCBackend, this sets the log level of the Python logger named :py:`"ixmp.backend.jdbc"`
    and the underlying Java code.
    On IXMP4Backend, only the log level of the logger named :py:`"ixmp.backend.ixmp4"` is changed,
    and not the log level of any underlying code.
    User code **may** use standard :mod:`logger` methods to access and
    change the level of loggers within :mod:`ixmp4` or its dependencies:

    .. code-block:: python

       import logging

       log = logging.getLogger("ixmp4")  # or other module
       log.setLevel(logging.DEBUG)  # or other level

TimeSeries class
----------------

:attr:`.TimeSeries.model`, :attr:`.TimeSeries.scenario`
   On ixmp4 and IXMP4Backend these names may be up to 255 characters long
   (see `here <https://github.com/iiasa/ixmp4/blob/main/ixmp4/db/migrations/versions/c71efc396d2b_initial_migration.py#L38>`__).
   On JDBCBackend the maximum is 1000 characters.

   .. NB see https://github.com/iiasa/ixmp_source/blob/889b51f7731b3fdfed2e241c3d6596723e83202e/src/main/resources/db/migration/postgresql/V1.31__model_scenario_names.sql

:attr:`.TimeSeries.version`
   On IXMP4Backend, a new, uncommitted Timeseries has :py:`version = 1`.
   (ixmp4 sets a value of 0, but IXMP4Backend always stores certain metadata,
   which increases the value to 1.)
   On JDBCBackend, the value is -1.

:meth:`.TimeSeries.add_geodata`, :meth:`.get_geodata`, and :meth:`.remove_geodata`
   Not in visible use.
   ixmp4 does not support storing and retrieving geodata,
   so these methods are and will not be supported on IXMP4Backend.

:meth:`.TimeSeries.add_timeseries`
   On IXMP4Backend, ‘variable’ column entries may be up to 255 characters long
   (see `here <https://github.com/iiasa/ixmp4/blob/main/ixmp4/db/migrations/versions/c71efc396d2b_initial_migration.py#L24>`__).
   On JDBCBackend, the maximum is 256 characters.

   .. NB see https://github.com/iiasa/ixmp_source/blob/889b51f7731b3fdfed2e241c3d6596723e83202e/src/main/resources/db/migration/postgresql/V1__postgresql_base_version.sql#L184

:meth:`.TimeSeries.last_update`
   On IXMP4Backend, the return value is never :any:`None`.
   For newly-created TimeSeries objects, it is the date-time of creation.

:meth:`.TimeSeries.transact`
   JDBCBackend logs certain warnings when exceptions occur within a :py:`with ts.transact(): ...` block.
   ixmp4/IXMP4Backend does not yet emit the same warnings.

Scenario class
--------------

Default contents 
   If :attr:`.ixmp4.Options.jdbc_compat` is :any:`True`,
   the sets ``technology`` and ``year`` are created,
   matching the behaviour of JDBCBackend.

:attr:`Scenario.version <.TimeSeries.version>`
   On IXMP4Backend, a new, empty Scenario has :py:`version = 1`.
   On JDBCBackend, the value is 0.

:meth:`.Scenario.par`
   On IXMP4Backend, data returned as :class:`pandas.DataFrame`
   have "unit" and "value" as the last 2 columns, in that order.
   On JDBCBackend, the columns are in the order "value", "unit".

   .. The former aligns with the common order in the IAMC data format.
      The latter “seems more aligned with natural language”.

   To accommodate this difference,
   user code **should** access the columns by name, rather than by index.

.. currentmodule:: ixmp.backend

Backend API
===========

.. autosummary::

   available
   get_class

.. currentmodule:: ixmp.backend.common

.. autosummary::

   ItemType
   FIELDS
   IAMC_IDX

.. currentmodule:: ixmp.backend.base

.. autosummary::

   Backend
   CachingBackend

- :class:`ixmp.Platform` implements a *user-friendly* API for scientific programming.
  This means its methods can take many types of arguments, check, and transform them—in a way that provides modeler-users with easy, intuitive workflows.
- In contrast, :class:`Backend` has a *very simple* API that accepts arguments and returns values in basic Python data types and structures.
- As a result:

  - :class:`Platform <ixmp.Platform>` code is not affected by where and how data is stored; it merely handles user arguments and then makes, usually, a single :class:`Backend` call.
  - :class:`Backend` code does not need to perform argument checking; merely store and retrieve data reliably.

- Additional Backends may inherit from :class:`Backend` or
  :class:`CachingBackend`.

.. autoclass:: ixmp.backend.base.Backend
   :members:

   In the following, the bold-face words **required**, **optional**, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

   Backend is an **abstract** class; this means it **must** be subclassed.
   Most of its methods are decorated with :any:`abc.abstractmethod`; this means they are **required** and **must** be overridden by subclasses.

   Others, marked below with "OPTIONAL:", are not so decorated.
   For these methods, the behaviour in the base Backend—often, nothing—is an acceptable default behaviour.
   Subclasses **may** extend or replace this behaviour as desired, so long as the methods still perform the actions described in the description.

   Backends:

   - **must** only raise standard Python exceptions.
   - **must** implement the :doc:`data model <data-model>` as described, or raise :class:`.NotImplementedError` for not implemented parts of the data model.

   Methods related to :class:`ixmp.Platform`:

   .. autosummary::
      :nosignatures:

      add_model_name
      add_scenario_name
      close_db
      get_auth
      get_doc
      get_log_level
      get_meta
      get_model_names
      get_nodes
      get_scenarios
      get_scenario_names
      get_units
      handle_config
      open_db
      read_file
      remove_meta
      set_doc
      set_log_level
      set_meta
      set_node
      set_unit
      write_file

   Methods related to :class:`ixmp.TimeSeries`:

   - Each method has an argument `ts`, a reference to the TimeSeries object being manipulated.
   - ‘Geodata’ is otherwise identical to regular timeseries data, except value are :class:`str` rather than :class:`float`.

   .. autosummary::
      :nosignatures:

      check_out
      commit
      delete
      delete_geo
      discard_changes
      get
      get_data
      get_geo
      init
      is_default
      last_update
      preload
      run_id
      set_data
      set_as_default
      set_geo

   Methods related to :class:`ixmp.Scenario`:

   - Each method has an argument `s`, a reference to the Scenario object being manipulated.

   .. autosummary::
      :nosignatures:

      clone
      delete_item
      get_meta
      has_solution
      init_item
      item_delete_elements
      item_get_elements
      item_set_elements
      item_index
      list_items
      remove_meta
      set_meta

   Methods related to :class:`message_ix.Scenario`:

   - Each method has an argument `ms`, a reference to the Scenario object being manipulated.

   .. warning:: These methods may be moved to ixmp in a future release.

   .. autosummary::
      :nosignatures:

      cat_get_elements
      cat_list
      cat_set_elements

.. autoclass:: ixmp.backend.base.CachingBackend
   :members:
   :private-members:

   CachingBackend stores cache values for multiple :class:`.TimeSeries`/:class:`.Scenario` objects, and for multiple values of a *filters* argument.

   Subclasses **must** call :meth:`cache`, :meth:`cache_get`, and :meth:`cache_invalidate` as appropriate to manage the cache; CachingBackend does not enforce any such logic.

.. automodule:: ixmp.backend
   :members:
   :exclude-members: ItemType

.. automodule:: ixmp.backend.common
   :members:
   :exclude-members: ItemType

.. autoclass:: ixmp.backend.ItemType
   :members:
   :undoc-members:
   :member-order: bysource

.. currentmodule:: ixmp.backend.io

Common input/output routines for backends
-----------------------------------------

.. automodule:: ixmp.backend.io
   :members:
