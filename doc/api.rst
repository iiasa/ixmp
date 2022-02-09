.. currentmodule:: ixmp

Python (:mod:`ixmp` package)
============================

The |ixmp| application programming interface (API) is organized around three classes:

.. autosummary::

   Platform
   TimeSeries
   Scenario

Platform
--------

.. autoclass:: Platform
   :members:

   Platforms have the following methods:

   .. autosummary::
      add_region
      add_region_synonym
      add_unit
      check_access
      regions
      scenario_list
      set_log_level
      units

   The following backend methods are available via Platform too:

   .. autosummary::
      backend.base.Backend.add_model_name
      backend.base.Backend.add_scenario_name
      backend.base.Backend.close_db
      backend.base.Backend.get_doc
      backend.base.Backend.get_meta
      backend.base.Backend.get_model_names
      backend.base.Backend.get_scenario_names
      backend.base.Backend.open_db
      backend.base.Backend.remove_meta
      backend.base.Backend.set_doc
      backend.base.Backend.set_meta

   These methods can be called like normal Platform methods, e.g.::

     $ platform_instance.close_db()


TimeSeries
----------

.. autoclass:: TimeSeries
   :members:

   A TimeSeries is uniquely identified on its :class:`Platform` by its :attr:`~.TimeSeries.model`, :attr:`~.TimeSeries.scenario`, and :attr:`~.TimeSeries.version` attributes.
   For more details, see the :ref:`data model documentation <data-timeseries>`.

   A new :attr:`.version` is created by:

   - Instantiating a new TimeSeries with the same `model` and `scenario` as an existing TimeSeries.
   - Calling :meth:`Scenario.clone`.

   TimeSeries objects have the following methods:

   .. autosummary::
      add_geodata
      add_timeseries
      check_out
      commit
      discard_changes
      get_geodata
      is_default
      last_update
      preload_timeseries
      read_file
      remove_geodata
      remove_timeseries
      run_id
      set_as_default
      timeseries
      transact


Scenario
--------

.. autoclass:: Scenario
   :show-inheritance:
   :members:

   A Scenario is a :class:`TimeSeries` that also contains model data, including model solution data.
   See the :ref:`data model documentation <data-model-data>`.

   The Scenario class provides methods to manipulate model data items:

   - Set: :meth:`init_set`, :meth:`add_set`, :meth:`set`, :meth:`remove_set`, :meth:`has_set`
   - Parameter:

     - ≥1-dimensional: :meth:`init_par`, :meth:`add_par`, :meth:`par`, :meth:`remove_par`, :meth:`par_list`, and :meth:`has_par`.
     - 0-dimensional: :meth:`init_scalar`, :meth:`change_scalar`, and :meth:`scalar`.

   - Variable: :meth:`init_var`, :meth:`var`, :meth:`var_list`, and :meth:`has_var`.
   - Equation: :meth:`init_equ`, :meth:`equ`, :meth:`equ_list`, and :meth:`has_equ`.

   .. autosummary::
      add_par
      add_set
      change_scalar
      clone
      equ
      equ_list
      get_meta
      has_equ
      has_par
      has_set
      has_solution
      has_var
      idx_names
      idx_sets
      init_equ
      init_par
      init_scalar
      init_set
      init_var
      load_scenario_data
      par
      par_list
      read_excel
      remove_par
      remove_set
      remove_solution
      scalar
      set
      set_list
      set_meta
      solve
      to_excel
      var
      var_list


.. currentmodule:: ixmp.backend.io

.. automodule:: ixmp.backend.io
   :members: EXCEL_MAX_ROWS


.. _configuration:

Configuration
-------------

When imported, :mod:`ixmp` reads configuration from the first file named
``config.json`` found in one of the following directories:

1. The directory given by the environment variable ``IXMP_DATA``, if
   defined,
2. ``${XDG_DATA_HOME}/ixmp``, if the environment variable is defined, or
3. ``$HOME/.local/share/ixmp``.

.. tip::
   For most users, #2 or #3 is a sensible default; platform information for many local and remote databases can be stored in ``config.json`` and retrieved by name.

   Advanced users wishing to use a project-specific ``config.json`` can set ``IXMP_DATA`` to the path for any directory containing a file with this name.

To manipulate the configuration file, use the ``platform`` command in the ixmp command-line interface::

  # Add a platform named 'p1' backed by a local HSQL database
  $ ixmp platform add p1 jdbc hsqldb /path/to/database/files

  # Add a platform named 'p2' backed by a remote Oracle database
  $ ixmp platform add p2 jdbc oracle \
         database.server.example.com:PORT:SCHEMA username password

  # Add a platform named 'p3' with specific JVM arguments
  $ ixmp platform add p3 jdbc hsqldb /path/to/database/files -Xmx12G

  # Make 'p2' the default Platform
  $ ixmp platform add default p2

…or, use the methods of :data:`.ixmp.config`.

.. currentmodule:: ixmp

.. data:: ixmp.config

   An instance of :class:`~.Config`.

.. autoclass:: ixmp._config.Config
   :members:


Utilities
---------

.. currentmodule:: ixmp.utils

.. automodule:: ixmp.utils
   :members:
   :exclude-members: as_str_list, check_year, isscalar, year_list, filtered

   .. autosummary::
      diff
      format_scenario_list
      maybe_check_out
      maybe_commit
      parse_url
      show_versions
      update_par

Testing utilities
-----------------

.. currentmodule:: ixmp.testing

.. automodule:: ixmp.testing
   :members:
   :exclude-members: pytest_report_header, pytest_sessionstart
