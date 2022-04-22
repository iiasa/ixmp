Reporting / postprocessing
**************************

.. currentmodule:: ixmp.reporting

:mod:`ixmp.reporting` provides features for computing derived values from the contents of a :class:`ixmp.Scenario`, *after* it has been solved using a model and the solution data has been stored.
It is built on the :mod:`genno` package, which has its own, separate documentation.
This page provides only API documentation.

- For an introduction and basic concepts, see :doc:`genno:usage` in the :mod:`genno` documentation.
- For automatic reporting of :class:`message_ix.Scenario`, see :doc:`message_ix:reporting` in the |MESSAGEix| documentation.

.. contents::
   :local:
   :depth: 3

Top-level classes and functions
===============================

.. automodule:: ixmp.reporting

The following top-level objects from :mod:`genno` may also be imported from
:mod:`ixmp.reporting`.

.. autosummary::

   ~genno.core.exceptions.ComputationError
   ~genno.core.key.Key
   ~genno.core.exceptions.KeyExistsError
   ~genno.core.exceptions.MissingKeyError
   ~genno.core.quantity.Quantity
   ~genno.config.configure

:mod:`ixmp.reporting` additionally defines:

.. autosummary::

   Reporter

.. autoclass:: ixmp.reporting.Reporter
   :members:
   :exclude-members: add_load_file

   A Reporter extends a :class:`genno.Computer` to postprocess data from one or more :class:`ixmp.Scenario` objects.

   Using the :meth:`.from_scenario`, a Reporter is automatically populated with:

   - :class:`Keys <.Key>` that retrieve the data for every :mod:`ixmp` item (parameter, variable, equation, or scalar) available in the Scenario.

   .. autosummary::
      finalize
      from_scenario
      set_filters

   The Computer class provides the following methods:

   .. autosummary::
      ~genno.core.computer.Computer.add
      ~genno.core.computer.Computer.add_file
      ~genno.core.computer.Computer.add_product
      ~genno.core.computer.Computer.add_queue
      ~genno.core.computer.Computer.add_single
      ~genno.core.computer.Computer.aggregate
      ~genno.core.computer.Computer.apply
      ~genno.core.computer.Computer.check_keys
      ~genno.core.computer.Computer.configure
      ~genno.core.computer.Computer.convert_pyam
      ~genno.core.computer.Computer.describe
      ~genno.core.computer.Computer.disaggregate
      ~genno.core.computer.Computer.full_key
      ~genno.core.computer.Computer.get
      ~genno.core.computer.Computer.infer_keys
      ~genno.core.computer.Computer.keys
      ~genno.core.computer.Computer.visualize
      ~genno.core.computer.Computer.write

.. _reporting-config:

Configuration
=============

:mod:`ixmp.reporting` adds handlers for two configuration sections, and modifies the behaviour of one from :mod:`genno`

.. automethod:: ixmp.reporting.filters

   Reporter-specific configuration.

   Affects data loaded from a Scenario using :func:`.data_for_quantity`, which filters the data before any other computation takes place.
   Filters are stored at ``Reporter.graph["config"]["filters"]``.

   If no arguments are provided, *all* filters are cleared.
   Otherwise, `filters` is a mapping of :class:`str` → (:class:`list` of :class:`str` or :obj:`None`.
   Keys are dimension IDs.
   Values are either lists of allowable labels along the respective dimension or :obj:`None` to clear any existing filters for that dimension.

   This configuration can be applied through :meth:`.Reporter.set_filters`; :meth:`.Reporter.configure`, or in a configuration file:

   .. code-block:: yaml

      filters:
        # Exclude a label "x2" on the "x" dimension, etc.
        x: [x1, x3, x4]
        technology: [coal_ppl, wind_ppl]
        # Clear existing filters for the "commodity" dimension
        commodity: null

.. automethod:: ixmp.reporting.rename_dims

   Reporter-specific configuration.

   Affects data loaded from a Scenario using :func:`.data_for_quantity`.
   Native dimension names are mapped; in the example below, the dimension "i" is present in the Reporter as "i_renamed" on all quantities/keys in which it appears.

   .. code-block:: yaml

       rename_dims:
         i: i_renamed

.. automethod:: ixmp.reporting.units

   The only difference from :func:`genno.config.units` is that this handler keeps the configuration values stored in ``Reporter.graph["config"]``.
   This is so that :func:`.data_for_quantity` can make use of ``["units"]["apply"]``


Computations
============

.. automodule:: ixmp.reporting.computations
   :members:

   :mod:`ixmp.reporting` defines these computations:

   .. autosummary::
      data_for_quantity
      map_as_qty
      update_scenario
      store_ts

   Basic computations are defined by :mod:`genno.computation`; and its compatibility modules; see there for details:

   .. autosummary::
      ~genno.compat.plotnine.Plot
      ~genno.computations.add
      ~genno.computations.aggregate
      ~genno.computations.apply_units
      ~genno.compat.pyam.computations.as_pyam
      ~genno.computations.broadcast_map
      ~genno.computations.combine
      ~genno.computations.concat
      ~genno.computations.disaggregate_shares
      ~genno.computations.div
      ~genno.computations.group_sum
      ~genno.computations.interpolate
      ~genno.computations.load_file
      ~genno.computations.mul
      ~genno.computations.pow
      ~genno.computations.product
      ~genno.computations.relabel
      ~genno.computations.rename_dims
      ~genno.computations.ratio
      ~genno.computations.select
      ~genno.computations.sum
      ~genno.computations.write_report

Utilities
=========

.. automodule:: ixmp.reporting.util
   :members:
