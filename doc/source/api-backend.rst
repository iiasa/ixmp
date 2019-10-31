.. currentmodule:: ixmp.backend

Storage back ends (:mod:`ixmp.backend`)
=======================================

By default, the |ixmp| is installed with :class:`ixmp.backend.jdbc.JDBCBackend`, which can store data in many types of relational database management systems (RDBMS) that have Java DataBase Connector (JDBC) interfaces—hence its name.

However, |ixmp| is extensible to support other methods of storing data: in non-JDBC RDBMS, non-relational databases, local files, memory, or other ways.
Developers wishing to add such capabilities may subclass :class:`ixmp.backend.base.Backend` and implement its methods.


Provided backends
-----------------

.. automodule:: ixmp.backend
   :members: BACKENDS

.. currentmodule:: ixmp.backend.jdbc

.. autoclass:: ixmp.backend.jdbc.JDBCBackend
   :members: read_gdx, write_gdx

   JDBCBackend supports:

   - ``dbtype='HSQLDB'``: HyperSQL databases in local files.
   - Remote databases. This is accomplished by creating a :class:`ixmp.Platform` with the ``dbprops`` argument pointing a file that specifies JDBC information. For instance::

       jdbc.driver = oracle.jdbc.driver.OracleDriver
       jdbc.url = jdbc:oracle:thin:@database-server.example.com:1234:SCHEMA
       jdbc.user = USER
       jdbc.pwd = PASSWORD

   It has the following methods that are not part of the overall :class:`Backend` API:

   .. autosummary::
      :nosignatures:

      read_gdx
      write_gdx

.. automethod:: ixmp.backend.jdbc.start_jvm

Backend API
-----------

- :class:`ixmp.Platform` implements a *user-friendly* API for scientific programming.
  This means its methods can take many types of arguments, check, and transform them—in a way that provides modeler-users with easy, intuitive workflows.
- In contrast, :class:`Backend` has a *very simple* API that accepts arguments and returns values in basic Python data types and structures.
- As a result:

  - :class:`Platform <ixmp.Platform>` code is not affected by where and how data is stored; it merely handles user arguments and then makes, usually, a single :class:`Backend` call.
  - :class:`Backend` code does not need to perform argument checking; merely store and retrieve data reliably.

.. autodata:: ixmp.backend.FIELDS

.. currentmodule:: ixmp.backend.base

.. autoclass:: ixmp.backend.base.Backend
   :members:

   In the following, the bold-face words **required**, **optional**, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

   Backend is an **abstract** class; this means it **must** be subclassed.
   Most of its methods are decorated with :meth:`abc.abstractmethod`; this means they are **required** and **must** be overridden by subclasses.

   Others, marked below with "OPTIONAL:", are not so decorated.
   For these methods, the behaviour in the base Backend—often, nothing—is an acceptable default behaviour.
   Subclasses **may** extend or replace this behaviour as desired, so long as the methods still perform the actions described in the description.

   Backends:

   - **must** only raise standard Python exceptions.

   Methods related to :class:`ixmp.Platform`:

   .. autosummary::
      :nosignatures:

      close_db
      get_auth
      get_nodes
      get_scenarios
      get_units
      open_db
      set_log_level
      set_node
      set_unit

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
      init_ts
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
      init_s
      init_item
      item_delete_elements
      item_get_elements
      item_set_elements
      item_index
      list_items
      set_meta

   Methods related to :class:`message_ix.Scenario`:

   - Each method has an argument `ms`, a reference to the Scenario object being manipulated.

   .. warning:: These methods may be moved to ixmp in a future release.

   .. autosummary::
      :nosignatures:

      cat_get_elements
      cat_list
      cat_set_elements
