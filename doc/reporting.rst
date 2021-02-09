.. currentmodule:: ixmp.reporting

Reporting
*********

.. contents::
   :local:
   :depth: 3

Top-level classes and functions
===============================

:mod:`ixmp.reporting` is built on the :mod:`genno` package.
The following top-level objects from :mod:`genno` may also be imported from
:mod:`ixmp.reporting`.

.. autosummary::

   ~genno.config.configure
   ~genno.core.key.Key
   ~genno.core.quantity.Quantity

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

   .. autoattribute:: graph

Configuration
=============

:mod:`ixmp.reporting` adds a ``rename_dims:`` configuration file section.

.. automethod:: ixmp.reporting.rename_dims

Computer-specific configuration.

Affects data loaded from a Scenario using :func:`.data_for_quantity`.
Native dimension names are mapped; in the example below, the dimension "i" is present in the Reporter as "i_renamed" on all quantities/keys in which it appears.

.. code-block:: yaml

    rename_dims:
      i: i_renamed

Computations
============

.. automodule:: ixmp.reporting.computations
   :members:

   :mod:`ixmp.reporting` defines these computations:

   .. autosummary::
      data_for_quantity
      map_as_qty
      update_scenario

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
      ~genno.computations.group_sum
      ~genno.computations.load_file
      ~genno.computations.product
      ~genno.computations.ratio
      ~genno.computations.select
      ~genno.computations.sum
      ~genno.computations.write_report

Utilities
=========

.. automodule:: ixmp.reporting.util
   :members:
