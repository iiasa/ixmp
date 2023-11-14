File formats and input/output
*****************************

In addition to the data management features provided by :doc:`api-backend`, ixmp is able to write and read :class:`.TimeSeries` and :class:`.Scenario` data to and from files.
This page describes those options and formats.

Time series data
================

Time series data can be:

- Read using :meth:`.TimeSeries.read_file`, or the :doc:`CLI command <cli>` ``ixmp import timeseries FILE`` for a single TimeSeries object.
- Written using :meth:`.export_timeseries_data` for multiple TimeSeries objects at once.

Both CSV and Excel files in the IAMC time-series format are supported.

.. _excel-data-format:

Scenario/model data
===================

Scenario data can be read from/written to Microsoft Excel files using :meth:`.Scenario.read_excel` and :meth:`.to_excel`, and the CLI commands ``ixmp import scenario FILE`` and ``ixmp export FILE``.
The files have the following structure:

- One sheet named 'ix_type_mapping' with two columns:

  - 'item': the name of an ixmp item.
  - 'ix_type': the item's type as a length-3 string: 'set', 'par', 'var', or 'equ'.

- One or more sheet per item. If the length of data is greater than the maximum number of rows per sheet supported by the Excel file format (:data:`.EXCEL_MAX_ROWS`), the item is split across multiple sheets named, e.g., 'foo', 'foo(2)', 'foo(3)'.

- Sets:

  - Sheets for one-dimensional indexed sets have one column, with a header cell that is the index set name.
  - Sheets for multi-dimensional indexed sets have multiple columns.
  - Sets with no elements are represented by empty sheets.

- Parameters, variables, and equations:

  - Sheets have zero (for scalar items) or more columns with headers that are the index *names* (not necessarily sets; see below) for those dimensions.
  - Parameter sheets have 'value' and 'unit' columns.
  - Variable and equation sheets have 'lvl' and 'mrg' columns.
  - Items with no elements are not included in the file.

Limitations
-----------

Reading variables and equations
   The ixmp API provides no way to set the data of variables and equations, because these are considered model solution data.

   Thus, while :meth:`.to_excel` will write files containing variable and equation data, :meth:`.read_excel` can not add these to a Scenario, and only emits log messages indicating that they are ignored.

.. _excel-ambiguous-dims:

Multiple dimensions indexed by the same set
   :meth:`.read_excel` provides the `init_items` argument to create new sets and parameters when reading a file.
   However, the file format does not capture information needed to reconstruct the original data in all cases.

   For example::

      scenario.init_set('foo')
      scenario.add_set('foo', ['a', 'b', 'c'])
      scenario.init_par(name='bar', idx_sets=['foo'])
      scenario.init_par(
          name='baz',
          idx_sets=['foo', 'foo'],
          idx_names=['foo', 'another_dimension'])
      scenario.to_excel('file.xlsx')

   :file:`file.xlsx` will contain sheets named 'bar' and 'baz'.
   The sheet 'bar' will have column headers 'foo', 'value', and 'unit', which are adequate to reconstruct the parameter.
   However, the sheet 'baz' will have column headers 'foo' and 'another_dimension'; this information does not allow ixmp to infer that 'another_dimension' is indexed by 'foo'.

   To work around this limitation, initialize 'baz' with the correct dimensions before reading its data::

      new_scenario.init_par(
          name='baz',
          idx_sets=['foo', 'foo'],
          idx_names=['foo', 'another_dimension'])
      new_scenario.read_excel('file.xlsx', init_items=True)

.. _excel-formats:

File formats other than :file:`.xlsx`
   The :file:`.xlsx` (Office Open XML) file format is preferred for input and output.
   :mod:`ixmp` uses `openpyxl <https://openpyxl.readthedocs.io>`_ and :mod:`pandas` in order to read and write this format.
   For other Excel file formats, including :file:`.xls` and :file:`.xlsb`, see the :ref:`Pandas documentation <pandas:io.excel>`.
