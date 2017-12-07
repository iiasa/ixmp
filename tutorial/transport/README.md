The transport tutorial - getting started with the ``ixmp`` package
==================================================================

The tutorials provided in this folder take you through the first steps
of working with ``ixmp`` package. You will learn how to create an 
``ixmp.Scenario`` as a structured data collection for sets, parameters
and the numerical solution to the associated optimization problem.

We use Dantzig's transport problem, which is also used as the standard GAMS tutorial.
This problem finds a least cost shipping schedule that meets requirements at markets and supplies at factories.

If you are not familiar with GAMS, please take a minute to look at the [transport.gms](transport.gms) code.

For reference of the transport problem, see:
> Dantzig, G B, Chapter 3.3. In Linear Programming and Extensions.  
> Princeton University Press, Princeton, New Jersey, 1963.

> This formulation is described in detail in:  
> Rosenthal, R E, Chapter 2: A GAMS Tutorial.  
> In GAMS: A User's Guide. The Scientific Press, Redwood City, California, 1988.

> see http://www.gams.com/mccarl/trnsport.gms

The tutorials are provided as Jupyter notebooks for both Python and R, 
and they are identical as far as possible.

 - Tutorial 1: ``<..>_transport.ipynb`` - Solve Dantzig's Transport Problem
 - Tutorial 2: ``<..>_transport_scenario.ipynb`` - Make a scenario of Dantzig's Transport Problem
