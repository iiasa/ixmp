.. currentmodule:: ixmp.reporting

Reporting
=========

.. warning::

   :mod:`ixmp.reporting` is **experimental** in ixmp 0.2, and only supports
   Python 3. The API and functionality may change without advance notice or a
   deprecation period in subsequent releases.

Top-level methods and classes:

.. autosummary::

   configure
   Reporter
   Key

Others:

.. contents::
   :local:
   :depth: 3

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
      add_product
      aggregate
      apply
      check_keys
      configure
      describe
      disaggregate
      finalize
      full_key
      get
      keys
      read_config
      set_filters
      visualize
      write

   .. autoattribute:: graph

   .. automethod:: add

      :meth:`add` may be used to:

      - Provide an alias from one *key* to another:

        >>> from message_ix.reporting import Reporter
        >>> rep = Reporter()  # Create a new Reporter object
        >>> rep.add('aliased name', 'original name')

      - Define an arbitrarily complex computation in a Python function that
        operates directly on the :class:`ixmp.Scenario`:

        >>> def my_report(scenario):
        >>>     # many lines of code
        >>>     return 'foo'
        >>> rep.add('my report', (my_report, 'scenario'))
        >>> rep.finalize(scenario)
        >>> rep.get('my report')
        foo

      .. note::
         Use care when adding literal ``str()`` values as a *computation*
         argument for :meth:`add`; these may conflict with keys that
         identify the results of other computations.


.. autoclass:: ixmp.reporting.Key
   :members:

   Quantities in a :class:`Scenario` can be indexed by one or more dimensions.
   A Key refers to a quantity using three components:

   1. a string :attr:`name`,
   2. zero or more ordered :attr:`dims`, and
   3. an optional :attr:`tag`.

   For example, an ixmp parameter with three dimensions can be initialized
   with:

   >>> scenario.init_par('foo', ['a', 'b', 'c'], ['apple', 'bird', 'car'])

   Key allows a specific, explicit reference to various forms of “foo”:

   - in its full resolution, i.e. indexed by a, b, and c:

     >>> k1 = Key('foo', ['a', 'b', 'c'])
     >>> k1 == 'foo:a-b-c'
     True

     Notice that a Key has the same hash, and compares equal (`==`) to its ``str()``.

   - in a partial sum over one dimension, e.g. summed along c with dimensions
     a and b:

     >>> k2 = k1.drop('c')
     >>> k2 == 'foo:a-b'
     True

   - in a partial sum over multiple dimensions, etc.:

     >>> k1.drop('a', 'c') == k2.drop('a') == 'foo:b'
     True

   .. note::
        Some remarks:

        - ``repr(key)`` prints the Key in angle brackets ('<>') to signify it is a Key object.

          >>> repr(k1)
          <foo:a-b-c>

        - Keys are *immutable*: the properties :attr:`name`, :attr:`dims`, and :attr:`tag` are read-only, and the methods :meth:`append`, :meth:`drop`, and :meth:`add_tag` return *new* Key objects.

        - Keys may be generated concisely by defining a convenience method:

          >>> def foo(dims):
          >>>     return Key('foo', dims.split())
          >>> foo('a b c')
          foo:a-b-c


Computations
------------

.. automodule:: ixmp.reporting.computations
   :members:

   Unless otherwise specified, these methods accept and return
   :class:`Quantity <ixmp.reporting.utils.Quantity>` objects for data
   arguments/return values.

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

   Data manipulation:

   .. autosummary::
      concat


Utilities
---------

.. autoclass:: ixmp.reporting.attrseries.AttrSeries

.. automodule:: ixmp.reporting.utils
   :members:
   :exclude-members: AttrSeries
