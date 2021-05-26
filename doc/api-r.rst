.. _rixmp:

Usage in R via ``reticulate``
*****************************

:mod:`ixmp` is fully usable in R via `reticulate`_, a package that allows nearly seamless access to the Python API.
No additional R packages are needed.

.. note:: The former ``rixmp`` package was removed in :mod:`ixmp` :ref:`v3.3.0`.

See :ref:`message_ix:install-r` for installing R and `reticulate`_ to use with :mod:`ixmp`.
Those instructions are suitable whether :mod:`message_ix` is also installed, or only :mod:`ixmp`.

Once installed, use reticulate to import the Python package:

.. code-block:: R

   library(reticulate)
   ixmp <- import("ixmp")

This creates a global variable, ``ixmp``, that can be used much like the Python module:

.. code-block:: R

   mp <- ixmp$Platform(name = 'default')
   scen <- ixmp$Scenario(mp, 'model name', 'scenario name', version = 'new')

Finally, see the R versions of the :doc:`tutorials`.

.. _reticulate: https://rstudio.github.io/reticulate/
