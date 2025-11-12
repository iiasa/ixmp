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
   As of version 0.10, `ixmp4 <https://github.com/iiasa/ixmp4/>`__ supports only Python 3.10 and above.

   - If you want to use IXMP4Backend,
     please ensure you are using a sufficiently recent Python version.
   - If you are restricted to Python 3.9 and below,
     please use JDBCBackend instead.

.. automodule:: ixmp.backend.ixmp4
   :members:
   :exclude-members: IXMP4Backend

.. autoclass:: ixmp.backend.ixmp4.IXMP4Backend
   :members:

   .. note::
      As of ixmp version 3.11,
      IXMP4Backend has only *partial* support for the APIs of :class:`ixmp.Platform`,
      :class:`ixmp.TimeSeries`, :class:`ixmp.Scenario`, and :py:`message_ix.Scenario`,
      and may not be as performant as JDBCBackend.
      See `Support roadmap for ixmp4 <https://github.com/iiasa/message_ix/discussions/939>`_
      and `issues labeled 'backend.ixmp4' <https://github.com/iiasa/ixmp/labels/backend.ixmp4>`_
      for details of future work to expand support and improve performance.

      Because IXMP4Backend uses ixmp4 version 0.10 (that is, prior to a 1.0 'stable' release):

      - The ixmp4 API is not yet finalized and may change at any time.
      - Complete documentation of the ixmp4 API itself is not yet available.

      Consequently you **may** but probably **should not** use it for 'production' scientific scenario work.

   IXMP4Backend supports storage in local, SQLite databases.

.. automodule:: ixmp.util.ixmp4
   :members:

Differences between JDBCBackend and IXMP4Backend
------------------------------------------------

.. NOTE What do we do about major difference such as Run being the new Scenario, IndexSet and Table vs Set, etc?
.. Functionally, all things are equivalent, but these *are* changes we'd like people to adjust to eventually.

While we strive to make IXMP4Backend as functionally compatible to JDBCBackend as possible,
there are several differences remaining.
Some of them are intentional,
so are not expected to change,
while others may still be addressed.

CLI integration
^^^^^^^^^^^^^^^

Some CLI tests do not work on ixmp4 yet, but all are supposed to.
Be careful when using ``ixmp platform list`` 
and ``ixmp solve``
on the command line.

Regions
^^^^^^^

On JDBCBackend, each region can have a "parent" and a "hierarchy"
and may have a "synonym" defined for itself.
Notably, when a region is part of a "hierarchy", a "parent" must be defined.

On ixmp4, each region must be part of a "hierarchy", 
but does not have either "parent" nor "synonym".

For backward compatibility, IXMP4Backend accepts the same parameters for region-related function as JDBCBackend.
However, "parent" and "synonym" are unused and will trigger logger warnings,
and if "hierarchy" is missing, a meaningless default will be used.

Meta data
^^^^^^^^^

Meta data refers to two things (on JDBCBackend, at least): 

1. TimeSeries data can be "marked as meta", which means the data will not be cleared by :func:`ixmp.backend.base.Backend.clear_solution` 
and will be kept during :func:`ixmp.Scenario.clone` regardless of the ``keep_solution`` parameter.
2. Meta data can also refer to a key:value storage of additional information about an :class:`ixmp.Scenario` or :class:`ixmp4.Run`.

In regards to the first point, 
the behaviour between ixmp and ixmp4 is almost identical:

- In ixmp4, the marker is called ``is_input`` instead of ``meta`` in order to distinguish the use cases.
- On JDBCBackend, `meta <https://github.com/iiasa/ixmp_source/blob/889b51f7731b3fdfed2e241c3d6596723e83202e/src/main/resources/db/migration/postgresql/V1__postgresql_base_version.sql#L219>`__ is stored as ``Integer``, whereas ixmp4 stores it as a ``Boolean``. 

This is recognized correctly by pandas,
and comparing dataframe types will yield differing types.

For the second point,
the main behaviour is identical, too:
users can store arbitrary single values (i.e., no lists or other collections) with string keys for :class:`ixmp4.Run`.
However, JDBCBackend also allows storing such mappings for a single model name or scenario name, which is not possible with ixmp4's current database model,
or when not providing a :class:`ixmp.Scenario` version, which ixmp4 requires.
Thus, ixmp4 does not permit the same combinations of parameters and requires that all of ``model``, ``scenario``, and ``version`` are present when using :func:`ixmp.backend.ixmp4.IXMP4Backend.set_meta`. 

Documenting Meta data
"""""""""""""""""""""

On JDBCBackend, documentation strings can be attached to Meta data (in the second sense above),
but on ixmp4, the current database model does not allow this.
As a workaround, one could add documentation to the :class:`ixmp4.Run` that the Meta data must be linked to.

Pre-defined Meta data
"""""""""""""""""""""

JDBCBackend stores an annotation and a scheme with every :class:`ixmp.Scenario`, which ixmp4 does not.
In order to keep the same information available, 
ixmp4 uses the Meta data of a :class:`ixmp4.Run` to store they values under "_ixmp_annotation" and "_ixmp_scheme", respectively, 
upon :class:`ixmp.Scenario` creation.

This also means that :func:`ixmp.TimeSeries.last_update` will never be :obj:`None` on IXMP4Backend.

Handling timeslices
^^^^^^^^^^^^^^^^^^^

In contrast to JDBCBackend, ixmp4 does not provide a dedicated way to handle timeslices.
This should be fine in practice since according to a GitHub search of our most important repositories/branches, 
no user code relies on these dedicated timeslice objects/methods.
In ixmp4, if one wants to use different timeslices,
one can use the ``type``, ``step_category``, ``step_year``, and ``step_datetime`` fields of the :class:`ixmp4.data.db.iamc.datapoint.DataPoint` model
to register them correctly.

Tests expecting specific behaviour of the dedicated functions will likely be impossible to satisfy with ixmp4.

Pre-defined items
^^^^^^^^^^^^^^^^^

At two levels, JDBCBackend pre-defines several items: on :class:`ixmp.Platform` s and on :class:`ixmp.Scenario` s with "MESSAGE" scheme. 
In contrast, ixmp4 consciously decides not to presuppose any kind of expected data, so it is left to users to define everything they desire themselves.

Platform-level pre-defined data (such as regions and units, among others) are detailed `on GitHub <https://github.com/iiasa/ixmp/issues/608>`__. 

Scenario-level data includes :class:`ixmp4.IndexSet` s like "technology" and "year".

Currently, both levels are only partially set up when using ixmp4 by providing the parameter ``jdbc_compat=True`` to :func:`ixmp.backend.ixmp4.IXMP4Backend.__init__`.
All items are set up that are required for the test suite and the tutorials, but non beyond that.

Raising errors
^^^^^^^^^^^^^^

In many cases if something goes awry, JDBCBackend raises a generic ``IxException`` (though with a speficic message).
Often, these errors are intercepted here and reraised as their closest related Python exception, e.g. ``RuntimeError``, ``ValueError``, etc.
On the other hand, ixmp4 provides lots of dedicated exceptions auch as ``RunLockRequired``, ``NoDefaultRunFound``, ``OptimizationDataValidationError``, and many others. 
In order to make the test suite pass, IXMP4Backend intercepts some of them and replaces them with the expected generic errors,
but in the end, I think our goal should be to make use of and rely on the dedicated ixmp4 exceptions.

Warnings during :func:`ixmp.TimeSeries.transact`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

JDBCBackend expects to see certain logger warnigns when running into errors in the transact context manager.
ixmp4 does not emit the same warnings right now.

Handling log levels
^^^^^^^^^^^^^^^^^^^

JDBCBackend allows setting and getting the log level of itself and the underlying Java code.
ixmp4 does not (yet) allow the same.

Handling Geodata
^^^^^^^^^^^^^^^^

In contrast to JDBCBackend, ixmp4 does not provide a way to handle Geodata.
This should be fine in practice since according to a GitHub search of our most important repositories/branches, 
no user code relies on Geodata objects/methods.
There is no plan to support Geodata, so all tests relying on related functions are expected to fail.

Sorting parameter data columns when reading from the database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

After all columns linked to :class:`ixmp4.IndexSet` s (or their equivalent on JDBCBackend),
JDBCBackend sorts "value" before "unit",
whereas ixmp4 sorts "unit" before "value".

The former choice seems more aligned to natural language,
while the latter is more aligned with the IAMC data format.
This should not make a difference since no user code should rely on a fixed order of these columns.

Closing a database connection twice
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

JDBCBackend is tested to log a warning that the database connection was already closed (and thus could not be closed again) if the logger settings are correct.
On, IXMP4Backend, there is no straightforward way to provide this:
sqlalchemy does not provide a way to check if a database session or engine was closed,
and closing one again after it had been closed before is simply a no-op. 

Miscellaneous database model differences
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- JDBCBackend allows `model and scenario names of up to 1000 characters <https://github.com/iiasa/ixmp_source/blob/889b51f7731b3fdfed2e241c3d6596723e83202e/src/main/resources/db/migration/postgresql/V1.31__model_scenario_names.sql>`__ in length, whereas ixmp4 only allows `255 characters <https://github.com/iiasa/ixmp4/blob/main/ixmp4/db/migrations/versions/c71efc396d2b_initial_migration.py#L38>`__ at maximum.
- JDBCBackend allows `variable names of up to 256 characters <https://github.com/iiasa/ixmp_source/blob/889b51f7731b3fdfed2e241c3d6596723e83202e/src/main/resources/db/migration/postgresql/V1__postgresql_base_version.sql#L184>`__, whereas ixmp4 only allows `255 characters <https://github.com/iiasa/ixmp4/blob/main/ixmp4/db/migrations/versions/c71efc396d2b_initial_migration.py#L24>`__.
- JDBCBackend starts new, empty :class:`ixmp.Scenario` s at version 0, whereas ixmp4 uses version 1.
- JDBCBackend starts new, uncommitted :class:`ixmp.TimeSeries` at version -1, whereas ixmp4 starts at version 0, but stores some Meta data upon creation, so effectively starts at 1.


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
