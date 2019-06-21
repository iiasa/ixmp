.. currentmodule:: ixmp.reporting

Reporting
=========

.. warning::

   :mod:`ixmp.reporting` is **experimental** in ixmp 0.2, and only supports
   Python 3. The API and functionality may change without advance notice or a
   deprecation period in subsequent releases.

.. automethod:: ixmp.reporting.configure

.. autoclass:: ixmp.reporting.Reporter
   :members:
   :exclude-members: graph, add

   A Reporter is used to postprocess data from from one or more
   :class:`ixmp.Scenario` objects. The :meth:`get` method can be used to:

   - Retrieve individual **quantities**. A quantity has zero or more
     dimensions and optional units. Quantities include the ‘parameters’,
     ‘variables’, ‘equations’, and ‘scalars’ available in an
     :class:`ixmp.Scenario`.

   - Generate an entire **report** composed of multiple quantities. A report
     may:

       - Read in non-model or exogenous data,
       - Trigger output to files(s) or a database, or
       - Execute user-defined methods.

   Every report and quantity (including the results of intermediate steps) is
   identified by a :class:`utils.Key`; all the keys in a Reporter can be
   listed with :meth:`keys`.

   Reporter uses a :doc:`graph <graphs>` data structure to keep track of
   **computations**, the atomic steps in postprocessing: for example, a single
   calculation that multiplies two quantities to create a third. The graph
   allows :meth:`get` to perform *only* the requested computations. Advanced
   users may manipulate the graph directly; but common reporting tasks can be
   handled by using Reporter methods:

   .. autosummary::
      add
      add_file
      aggregate
      apply
      configure
      describe
      disaggregate
      finalize
      full_key
      get
      read_config
      visualize
      write

   .. autoattribute:: graph

   .. automethod:: add

      :meth:`add` may be used to:

      - Provide an alias from one *key* to another:

        >>> r.add('aliased name', 'original name')

      - Define an arbitrarily complex computation in a Python function that
        operates directly on the :class:`ixmp.Scenario`:

        >>> def my_report(scenario):
        >>>     # many lines of code
        >>>     return 'foo'
        >>> r.add('my report', (my_report, 'scenario'))
        >>> r.finalize(scenario)
        >>> r.get('my report')
        foo

      .. note::
         Use care when adding literal :class:`str` values (2); these may
         conflict with keys that identify the results of other
         computations.


Computations
------------

.. automodule:: ixmp.reporting.computations
   :members:

   Calculations:

   .. autosummary::
      aggregate
      disaggregate_shares
      product
      ratio
      sum

   Input and output:

   .. autosummary::
      load_file
      write_report

   Conversion:

   .. autosummary::
      make_dataframe


Utilities
---------

.. autoclass:: ixmp.reporting.utils.Key
   :members:

   Quantities in a :class:`Scenario` can be indexed by one or more dimensions.
   For example, a parameter with three dimensions can be initialized with:

   >>> scenario.init_par('foo', ['a', 'b', 'c'], ['apple', 'bird', 'car'])

   Computations for this scenario might use the quantity ``foo`` in different
   ways:

   1. in its full resolution, i.e. indexed by a, b, and c;
   2. aggregated (e.g. summed) over any one dimension, e.g. aggregated over c
      and thus indexed by a and b;
   3. aggregated over any two dimensions; etc.

   A Key for (1) will hash, display, and evaluate as equal to ``'foo:a-b-c'``.
   A Key for (2) corresponds to ``'foo:a-b'``, and so forth.

   Keys may be generated concisely by defining a convenience method:

   >>> def foo(dims):
   >>>     return Key('foo', dims.split(''))
   >>> foo('a b')
   foo:a-b

.. autoclass:: ixmp.reporting.utils.AttrSeries

.. automodule:: ixmp.reporting.utils
   :members:
   :exclude-members: AttrSeries, Key, combo_partition
