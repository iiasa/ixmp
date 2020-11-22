.. currentmodule:: ixmp.reporting

Reporting
*********

Top-level methods and classes:

.. autosummary::

   configure
   Reporter
   Key
   Quantity

Others:

.. contents::
   :local:
   :depth: 3

.. automethod:: ixmp.reporting.configure

.. autoclass:: ixmp.reporting.Reporter
   :members:
   :exclude-members: graph, add, add_load_file, apply

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
   identified by a :class:`.Key`; all the keys in a Reporter can be listed with
   :meth:`keys`.

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
      add_queue
      add_single
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
      set_filters
      visualize
      write

   .. autoattribute:: graph

   .. automethod:: add

      :meth:`add` may be called with:

      - :class:`list` : `data` is a list of computations like ``[(list(args1), dict(kwargs1)), (list(args2), dict(kwargs2)), ...]`` that are added one-by-one.
      - the name of a function in :mod:`.computations` (e.g. 'select'): A computation is added with key ``args[0]``, applying the named function to ``args[1:]`` and `kwargs`.
      - :class:`str`, the name of a :class:`Reporter` method (e.g. 'apply'): the corresponding method (e.g. :meth:`apply`) is called with the `args` and `kwargs`.
      - Any other :class:`str` or :class:`.Key`: the arguments are passed to :meth:`add_single`.

      :meth:`add` may also be used to:

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

   .. automethod:: apply

      The `generator` may have a type annotation for Reporter on its first positional argument.
      In this case, a reference to the Reporter is supplied, and `generator` may use the Reporter methods to add computations:

      .. code-block:: python

         def gen0(r: ixmp.Reporter, **kwargs):
             r.load_file('file0.txt', **kwargs)
             r.load_file('file1.txt', **kwargs)

         # Use the generator to add several computations
         rep.apply(my_gen, units='kg')

      Or, `generator` may ``yield`` a sequence (0 or more) of (`key`, `computation`), which are added to the :attr:`graph`:

      .. code-block:: python

         def gen1(**kwargs):
             op = partial(computations.load_file, **kwargs)
             yield from (f'file:{i}', op, 'file{i}.txt') for i in range(2)

         rep.apply(my_gen, units='kg')

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

.. autodata:: ixmp.reporting.Quantity(data, *args, **kwargs)
   :annotation:

The :data:`.Quantity` constructor converts its arguments to an internal, :class:`xarray.DataArray`-like data format:

.. code-block:: python

   # Existing data
   data = pd.Series(...)

   # Convert to a Quantity for use in reporting calculations
   qty = Quantity(data, name="Quantity name", units="kg")
   rep.add("new_qty", qty)

Common :mod:`ixmp.reporting` usage, e.g. in :mod:`message_ix`, creates large, sparse data frames (billions of possible elements, but <1% populated); :class:`~xarray.DataArray`'s default, 'dense' storage format would be too large for available memory.

- Currently, Quantity is :class:`.AttrSeries`, a wrapped :class:`pandas.Series` that behaves like a :class:`~xarray.DataArray`.
- In the future, :mod:`ixmp.reporting` will use :class:`.SparseDataArray`, and eventually :class:`~xarray.DataArray` backed by sparse data, directly.

The goal is that reporting code, including built-in and user computations, can treat quantity arguments as if they were :class:`~xarray.DataArray`.


Computations
============

.. automodule:: ixmp.reporting.computations
   :members:

   Unless otherwise specified, these methods accept and return
   :class:`Quantity <ixmp.reporting.utils.Quantity>` objects for data
   arguments/return values.

   Calculations:

   .. autosummary::
      add
      aggregate
      apply_units
      disaggregate_shares
      product
      ratio
      select
      sum

   Input and output:

   .. autosummary::
      load_file
      write_report

   Data manipulation:

   .. autosummary::
      concat


Internal format for reporting quantities
========================================

.. currentmodule:: ixmp.reporting.quantity

.. automodule:: ixmp.reporting.quantity
   :members: assert_quantity

.. currentmodule:: ixmp.reporting.attrseries

.. automodule:: ixmp.reporting.attrseries
   :members:

.. currentmodule:: ixmp.reporting.sparsedataarray

.. automodule:: ixmp.reporting.sparsedataarray
   :members: SparseDataArray, SparseAccessor


Utilities
=========

.. automodule:: ixmp.reporting.utils
   :members:
