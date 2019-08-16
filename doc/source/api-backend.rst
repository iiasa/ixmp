.. currentmodule:: ixmp.backend

Storage back ends (:mod:`ixmp.backend` package)
===============================================

By default, the |ixmp| is installed with :class:`ixmp.backend.jdbc.JDBCBackend`, which can store data many types of relational database management systems (RDBMS) that have Java DataBase Connector (JDBC) interfaces—hence its name.
These include:

- ``dbtype='HSQLDB'``: databases in local files.
- Remote databases. This is accomplished by creating a :class:`ixmp.Platform` with the ``dbprops`` argument pointing a file that specifies JDBC information. For instance::

    jdbc.driver = oracle.jdbc.driver.OracleDriver
    jdbc.url = jdbc:oracle:thin:@database-server.example.com:1234:SCHEMA
    jdbc.user = USER
    jdbc.pwd = PASSWORD

However, |ixmp| is extensible to support other methods of storing data: in non-JDBC RDBMS, non-relational databases, local files, memory, or other ways.
Developers wishing to add such capabilities may subclass :class:`ixmp.backend.base.Backend` and implement its methods.

Implementing custom backends
----------------------------

In the following, the words MUST, MAY, etc. have specific meanings as described in RFC ____.

- :class:`ixmp.Platform` implements a *user-friendly* API for scientific programming.
  This means its methods can take many types of arguments, check, and transform them in a way that provides modeler-users with an easy, intuitive workflow.
- In contrast, :class:`Backend` has a very simple API that accepts and returns
  arguments in basic Python data types and structures.
  Custom backends need not to perform argument checking: merely store and retrieve data reliably.
- Some methods below are decorated as :meth:`abc.abstractmethod`; this means
  they MUST be overridden by a subclass of Backend.
- Others that are not so decorated and have “(optional)” in their signature are not required. The behaviour in base.Backend—often, nothing—is an acceptable default behaviour.
  Subclasses MAY extend or replace this behaviour as desired, so long as the methods still perform the actions described in the description.

.. automodule:: ixmp.backend
   :members: BACKENDS

.. autoclass:: ixmp.backend.jdbc.JDBCBackend

.. autoclass:: ixmp.backend.base.Backend
   :members:

   Methods related to :class:`ixmp.Platform`:

   .. autosummary::
      set_log_level
      open_db
      close_db
      get_nodes
      get_scenarios
      get_units

   Methods related to :class:`ixmp.TimeSeries`:

   .. autosummary::
      ts_init
      ts_check_out
      ts_commit
      ts_set
      ts_get
      ts_delete

   Methods related to :class:`ixmp.Scenario`:

   .. autosummary::
      s_init
      s_has_solution
      s_list_items
      s_init_item
      s_item_index
      s_item_elements
      s_add_set_elements
      s_add_par_values
