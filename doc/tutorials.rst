.. include:: ../tutorial/README.rst

Adapting GAMS models for :mod:`ixmp`
------------------------------------

The common example optimization from Dantzig :cite:`dantzig-1963` is available in a GAMS implementation `from the GAMS website <https://www.gams.com/mccarl/trnsport.gms>`_.
The file :file:`tutorial/transport/transport_ixmp.gms` illustrates how an existing GAMS model can be adapted to work with the |ixmp|.
The same, simple procedure can be applied to any GAMS code.

The steps are:

1. Modify the definitions of GAMS sets (``i`` and ``j``) and parameters (``a``, ``b``, ``d``, and ``f``) to **remove explicit values**.
2. Add lines to **read the model input data passed by ixmp**.

   The following lines are added *before* the code that defines and solves the model::

    * These two lines let the model code be run outside of ixmp, if needed
    $if not set in  $setglobal in  'ix_transport_data.gdx'
    $if not set out $setglobal out 'ix_transport_results.gdx'

    $gdxin '%in%'
    $load i, j, a, b, d, f
    $gdxin


3. Add a line to **write the model output data**.

   The following line is added *after* the model's ``solve ...;`` statement::

    execute_unload '%out%';

*ixmp*'s :class:`~.GAMSModel` class uses command-line options to pass the values of the variables ``in`` and ``out``.
This causes the model to read its input data from a GDX-format file created by *ixmp*, and write its output data to a GDX file specified by *ixmp*.
*ixmp* then automatically retrieves the model solution and other information from the output file, updating the :class:`~.Scenario` and storage :class:`~.Backend`.
