Reporting / postprocessing
**************************

.. currentmodule:: ixmp.report

:mod:`ixmp.report` provides features for computing derived values from the contents of a :class:`ixmp.Scenario`, *after* it has been solved using a model and the solution data has been stored.
It is built on the :mod:`genno` package, which has its own, separate documentation.
This page provides only API documentation.

- For an introduction and basic concepts, see :doc:`genno:usage` in the :mod:`genno` documentation.
- For automatic reporting of :class:`message_ix.Scenario`, see :doc:`message_ix:reporting` in the |MESSAGEix| documentation.

.. contents::
   :local:
   :depth: 3

Top-level classes and functions
===============================

.. automodule:: ixmp.report

The following top-level objects from :mod:`genno` may also be imported from
:mod:`ixmp.report`.

.. currentmodule:: genno
.. autosummary::

   ComputationError
   Key
   KeyExistsError
   MissingKeyError
   Quantity
   configure

:mod:`ixmp.report` additionally defines:

.. currentmodule:: ixmp.report
.. autosummary::

   Reporter

.. autoclass:: ixmp.report.Reporter
   :members:
   :exclude-members: add_load_file

   A Reporter extends a :class:`genno.Computer` to postprocess data from one or more :class:`ixmp.Scenario` objects.

   Using the :meth:`.from_scenario`, a Reporter is automatically populated with:

   - :class:`Keys <.genno.Key>` that retrieve the data for every :mod:`ixmp` item (parameter, variable, equation, or scalar) available in the Scenario.

   .. autosummary::

      finalize
      from_scenario
      set_filters

   The Reporter class inherits from Computer the following methods:

   .. currentmodule:: genno
   .. autosummary::

      ~Computer.add
      ~Computer.add_queue
      ~Computer.add_single
      ~Computer.apply
      ~Computer.check_keys
      ~Computer.configure
      ~Computer.describe
      ~Computer.full_key
      ~Computer.get
      ~Computer.infer_keys
      ~Computer.keys
      ~Computer.visualize
      ~Computer.write

   The following methods are deprecated; equivalent or better functionality is available through :meth:`Reporter.add <genno.Computer.add>`.
   See the genno documentation for each method for suggested changes/migrations.

   .. autosummary::

      ~Computer.add_file
      ~Computer.add_product
      ~Computer.aggregate
      ~Computer.convert_pyam
      ~Computer.disaggregate

.. _reporting-config:

Configuration
=============

:mod:`ixmp.report` adds handlers for two configuration sections, and modifies the behaviour of one from :mod:`genno`

.. autofunction:: ixmp.report.filters

   Reporter-specific configuration.

   Affects data loaded from a Scenario using :func:`.data_for_quantity`, which filters the data before any other computation takes place.
   Filters are stored at ``Reporter.graph["config"]["filters"]``.

   If no arguments are provided, *all* filters are cleared.
   Otherwise, `filters` is a mapping of :class:`str` → (:class:`list` of :class:`str` or :obj:`None`.
   Keys are dimension IDs.
   Values are either lists of allowable labels along the respective dimension or :obj:`None` to clear any existing filters for that dimension.

   This configuration can be applied through :meth:`.Reporter.set_filters`; :meth:`Reporter.configure <genno.Computer.configure>`, or in a configuration file:

   .. code-block:: yaml

      filters:
        # Exclude a label "x2" on the "x" dimension, etc.
        x: [x1, x3, x4]
        technology: [coal_ppl, wind_ppl]
        # Clear existing filters for the "commodity" dimension
        commodity: null

.. automethod:: ixmp.report.rename_dims

   Reporter-specific configuration.

   Affects data loaded from a Scenario using :func:`.data_for_quantity`.
   Native dimension names are mapped; in the example below, the dimension "i" is present in the Reporter as "i_renamed" on all quantities/keys in which it appears.

   .. code-block:: yaml

       rename_dims:
         i: i_renamed

.. automethod:: ixmp.report.units

   The only difference from :func:`genno.config.units` is that this handler keeps the configuration values stored in ``Reporter.graph["config"]``.
   This is so that :func:`.data_for_quantity` can make use of ``["units"]["apply"]``

.. automethod:: ixmp.report.configure

   This is the same as :func:`genno.configure`.


Operators
=========

.. automodule:: ixmp.report.operator
   :members:

   More than 30 operators are defined by :mod:`genno.operator` and its compatibility modules including :mod:`genno.compat.plotnine` and :mod:`genno.compat.sdmx`.
   See the genno documentation for details.

   :mod:`ixmp.report` defines these additional operators:

   .. autosummary::

      data_for_quantity
      from_url
      get_ts
      map_as_qty
      remove_ts
      store_ts
      update_scenario

Utilities
=========

.. currentmodule:: ixmp.report.common

.. autodata:: RENAME_DIMS

   User code **should** avoid directly manipulating :data:`RENAME_DIMS`.
   Instead, call :func:`~genno.configure`:

   .. code-block:: python

      # Rename dimension "long_dimension_name" to "ldn"
      configure(rename_dims={"long_dimension_name": "ldn"})

   As well, importing the variable into the global namespace of another module creates a copy of the dictionary that may become out of sync with other changes.
   Thus, instead of:

   .. code-block:: python

      from ixmp.report import RENAME_DIMS

      def my_operator(...):
          # Code that references RENAME_DIMS

   Do this:

   .. code-block:: python

      def my_operator(...):
          from ixmp.report import common

          # Code that references common.RENAME_DIMS

.. automodule:: ixmp.report.util
   :members:
