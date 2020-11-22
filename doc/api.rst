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

   A TimeSeries is uniquely identified on its :class:`Platform` by three
   values:

   1. `model`: the name of a model used to perform calculations between input
      and output data.

      - In TimeSeries storing non-model data, arbitrary strings can be used.
      - In a :class:`Scenario`, the `model` is a reference to a GAMS program
        registered to the :class:`Platform` that can be solved with
        :meth:`Scenario.solve`. See :attr:`ixmp.model.MODELS`.

   2. `scenario`: the name of a specific, coherent description of the real-
      world system being modeled. Any `model` may be used to represent multiple
      alternate, or 'counter-factual', `scenarios`.
   3. `version`: an integer identifying a specific iteration of a
      (`model`, `scenario`). A new `version` is created by:

      - Instantiating a new TimeSeries with the same `model` and `scenario` as
        an existing TimeSeries.
      - Calling :meth:`Scenario.clone`.

      Optionally, one `version` may be set as a **default version**. See
      :meth:`set_as_default`.

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


Scenario
--------

.. autoclass:: Scenario
   :show-inheritance:
   :members:

   A Scenario is a :class:`TimeSeries` associated with a particular model that
   can be run on the current :class:`Platform` by calling :meth:`solve`. The
   Scenario also stores the output, or 'solution' of a model run; this
   includes the 'level' and 'marginal' values of GAMS equations and variables.

   Data in a Scenario are closely related to different types in the GAMS data
   model:

   - A **set** is a named collection of labels. See :meth:`init_set`,
     :meth:`add_set`, and :meth:`set`. There are two types of sets:

     1. Sets that are lists of labels.
     2. Sets that are 'indexed' by one or more other set(s). For this type of
        set, each member is an ordered tuple of the labels in the index sets.

   - A **scalar** is a named, single, numerical value. See
     :meth:`init_scalar`, :meth:`change_scalar`, and :meth:`scalar`.

   - **Parameters**, **variables**, and **equations** are multi-dimensional
     arrays of values that are indexed by one or more sets (i.e. with
     dimension 1 or greater). The Scenario methods for handling these types
     are very similar; they mainly differ in how they are used within GAMS
     models registered with ixmp:

     - **Parameters** are generic data that can be defined before a model run.
       They may be altered by the model solution. See :meth:`init_par`,
       :meth:`remove_par`, :meth:`par_list`, :meth:`add_par`, and :meth:`par`.
     - **Variables** are calculated during or after a model run by GAMS code,
       so they cannot be modified by a Scenario. See :meth:`init_var`,
       :meth:`var_list`, and :meth:`var`.
     - **Equations** describe fundamental relationships between other types
       (parameters, variables, and scalars) in a model. They are defined in
       GAMS code, so cannot be modified by a Scenario. See :meth:`init_equ`,
       :meth:`equ_list`, and :meth:`equ`.

   .. deprecated:: 3.0
      The `cache` keyword argument to :class:`Scenario` has no effect and raises a warning.
      Use `cache` as one of the `backend_args` to :class:`Platform` to disable/enable caching for storage backends that support it.
      Use :meth:`load_scenario_data` to load all data in the Scenario into an in-memory cache.

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

   Advanced users wishing to use a project-specific ``config.json`` can set ``IXMP_DATA`` to the directory containing this file.

To manipulate the configuration file, use the ``platform`` command in the ixmp command-line interface::

  # Add a platform named 'p1' backed by a local HSQL database
  $ ixmp platform add p1 jdbc hsqldb /path/to/database/files

  # Add a platform named 'p2' backed by a remote Oracle database
  $ ixmp platform add p2 jdbc oracle \
         database.server.example.com:PORT:SCHEMA username password

  # Make 'p2' the default Platform
  $ ixmp platform add default p2

…or, use the methods of :obj:`ixmp.config`.

.. currentmodule:: ixmp

.. data:: ixmp.config

   An instance of :class:`~.Config`.

.. autoclass:: ixmp._config.Config
   :members:


Utilities
---------

.. currentmodule:: ixmp.utils

.. automodule:: ixmp.utils
   :members: format_scenario_list, maybe_check_out, maybe_commit, parse_url, show_versions, update_par


Testing utilities
-----------------

.. currentmodule:: ixmp.testing

.. automodule:: ixmp.testing
   :members:
   :exclude-members: pytest_report_header, pytest_sessionstart
