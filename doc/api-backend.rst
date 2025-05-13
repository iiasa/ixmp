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
   As of version 0.10, `ixmp4 <https://github.com/iiasa/ixmp4/>`_ supports only Python 3.10 and above.

   - If you want to use IXMP4Backend,
     please ensure you are using a sufficiently recent Python version.
   - If you are restricted to Python 3.9 and below,
     please use JDBCBackend instead.

.. automodule:: ixmp.backend.ixmp4
   :members:
   :exclude-members: IXMP4Backend

.. autoclass:: ixmp.backend.ixmp4.IXMP4Backend

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
