Tutorials for the  |ixmp|
=========================

The tutorials provided in the ``tutorial`` directory of the *ixmp* repository walk through the first steps of working with :mod:`ixmp`.
You will learn how to create a :class:`ixmp.Scenario` as a structured data collection for sets, parameters and the numerical solution to the associated optimization problem.

Dantzig's transportation problem
--------------------------------

We use Dantzig's transport problem, which is also used as the standard GAMS tutorial.
This problem finds a least cost shipping schedule that meets demand requirements at markets and supply capacity constraints at multiple factories.

For reference of the transport problem, see :cite:`dantzig-1963`.
This formulation is described in detail in :cite:`rosenthal-1988`.

> see http://www.gams.com/mccarl/trnsport.gms

The tutorials are provided as Jupyter notebooks for both Python and R,
and they are identical as far as possible.

- Tutorial 1:
  `Python <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/py_transport.ipynb>`__
  `R <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/R_transport.ipynb>`__

  Solve Dantzig's Transport Problem

- Tutorial 2:
  `Python <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/py_transport_scenario.ipynb>`__
  `R <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/R_transport_scenario.ipynb>`__

  Create an alternate or ‘counterfactual’ scenario of the transport problem; solve it; and compare the results to the original or reference scenario.

If you are not familiar with GAMS, please take a minute to look at the [transport.gms](transport.gms) code.
