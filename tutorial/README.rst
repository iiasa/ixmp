Tutorials
=========

The tutorials provided in the ``tutorial`` directory of the *ixmp* repository walk through the first steps of working with :mod:`ixmp`.
You will learn how to create a :class:`ixmp.Scenario` as a structured data collection for sets, parameters and the numerical solution to the associated optimization problem.

Dantzig's transportation problem
--------------------------------

We use Dantzig's transport problem :cite:`dantzig-1963`, which is also used as the standard GAMS tutorial :cite:`rosenthal-1988`.
This problem finds a least cost shipping schedule that meets demand requirements at markets and supply capacity constraints at multiple factories.

The tutorials are provided as Jupyter notebooks for both Python and R, and are identical as far as possible.

- Tutorial 1:
  in `Python <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/py_transport.ipynb>`__,
  or in `R <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/R_transport.ipynb>`__.

  This tutorial walks through the following steps:

  1. Launch an :class:`ixmp.Platform` instance and initialize a new :class:`ixmp.Scenario`.
  2. Define the sets and parameters in the scenario, and commit the data to the platform.
  3. Check out the scenario and initialize variables and equations (necessary for ixmp to import the solution).
  4. Solve the scenario (export to GAMS input gdx, execute, read solution from output gdx).
  5. Display the solution (variables and equation).

- Tutorial 2:
  in `Python <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/py_transport_scenario.ipynb>`__,
  or in `R <https://github.com/iiasa/ixmp/blob/master/tutorial/transport/R_transport_scenario.ipynb>`__.

  This tutorial creates an alternate or ‘counterfactual’ scenario of the transport problem; solves it; and compares the results to the original or reference scenario.
