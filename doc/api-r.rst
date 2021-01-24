.. _rixmp:

R (``rixmp`` package)
*********************

An R interface to the `ixmp` is provided by the ``rixmp`` package.
``rixmp`` uses the `reticulate <https://rstudio.github.io/reticulate/>`_ R-to-Python adapter to provide access to all features of the :mod:`ixmp` *Python* package

.. code-block:: R

   # Load the rixmp package
   library(rixmp)
   ixmp <- import('ixmp')

   # An 'ixmp' object is added to the global namespace.
   # It can be used in the same way as the Python ixmp package.
   mp <- ixmp$Platform(dbtype = 'HSQLDB')
   scen <- ixmp$Scenario(mp, 'model name', 'scenario name', version = 'new')

   # etc.

One additional method, :meth:`adapt_to_ret` is provided.
Access its documentation with

.. code-block:: R

   ?rixmp::adapt_to_ret

This function is useful when adding :class:`data.frames` objects to a Scenario:

.. code-block:: R

   scen$init_set("i")
   i.set = c("seattle", "san-diego")
   scen$add_set("i", i.set)
   # load dataframes
   scen$init_par("a", c("i"))
   a.df = data.frame(i = i.set, value = c(350, 600) , unit = 'cases')
   scen$add_par("a", adapt_to_ret(a.df))
