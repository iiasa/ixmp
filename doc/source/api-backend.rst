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
   :members: s_write_gdx, s_read_gdx

   JDBCBackend supports:

   - ``dbtype='HSQLDB'``: HyperSQL databases in local files.
   - Remote databases. This is accomplished by creating a :class:`ixmp.Platform` with the ``dbprops`` argument pointing a file that specifies JDBC information. For instance::

       jdbc.driver = oracle.jdbc.driver.OracleDriver
       jdbc.url = jdbc:oracle:thin:@database-server.example.com:1234:SCHEMA
       jdbc.user = USER
       jdbc.pwd = PASSWORD

   It has the following methods that are not part of the overall :class:`Backend` API:

   .. autosummary::
      s_write_gdx
      s_read_gdx

.. automethod:: ixmp.backend.jdbc.start_jvm

Backend API
-----------

- :class:`ixmp.Platform` implements a *user-friendly* API for scientific programming.
  This means its methods can take many types of arguments, check, and transform them—in a way that provides modeler-users with easy, intuitive workflows.
- In contrast, :class:`Backend` has a *very simple* API that accepts arguments and returns values in basic Python data types and structures.
- As a result:

  - :class:`Platform <ixmp.Platform>` code does is not affected by where and how data is stored; it merely handles user arguments and then makes, usually, a single :class:`Backend` call.
  - :class:`Backend` code does not need to perform argument checking; merely store and retrieve data reliably.

.. currentmodule:: ixmp.backend.base

.. autoclass:: ixmp.backend.base.Backend

   In the following, the words REQUIRED, OPTIONAL, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

   Backend is an **abstract** class; this means it MUST be subclassed.
   Most of its methods are decorated with :meth:`abc.abstractmethod`; this means they are REQUIRED and MUST be overridden by subclasses.

   Others, marked below with “(OPTIONAL)”, are not so decorated.
   For these methods, the behaviour in the base Backend—often, nothing—is an acceptable default behaviour.
   Subclasses MAY extend or replace this behaviour as desired, so long as the methods still perform the actions described in the description.

   Methods related to :class:`ixmp.Platform`:

   .. autosummary::
      close_db
      get_auth
      get_nodes
      get_scenarios
      get_units
      open_db
      set_log_level
      set_nodes
      set_unit

   Methods related to :class:`ixmp.TimeSeries`:

   - ‘Geodata’ is otherwise identical to regular timeseries data, except value are :class:`str` rather than :class:`float`.

   .. autosummary::
      ts_check_out
      ts_commit
      ts_delete
      ts_delete_geo
      ts_discard_changes
      ts_get
      ts_get_geo
      ts_init
      ts_is_default
      ts_last_update
      ts_preload
      ts_run_id
      ts_set
      ts_set_as_default
      ts_set_geo

   Methods related to :class:`ixmp.Scenario`:

   .. autosummary::
      s_add_par_values
      s_add_set_elements
      s_clone
      s_delete_item
      s_get_meta
      s_has_solution
      s_init
      s_init_item
      s_item_delete_elements
      s_item_elements
      s_item_index
      s_list_items
      s_set_meta

   Methods related to :class:`message_ix.Scenario`:

   .. autosummary::
      ms_cat_get_elements
      ms_cat_list
      ms_cat_set_elements
      ms_year_first_model
      ms_years_active
