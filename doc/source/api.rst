Scientific Programming APIs
============================

The `ixmp` has application programming interfaces (API) for efficient scientific workflows and data processing.

.. toctree::
   :maxdepth: 2

   api-python

R (``retixmp`` package)
-----------------------

An R interface to the `ixmp` is provided by the ``retixmp`` package.
``retixmp`` uses the `reticulate <https://rstudio.github.io/reticulate/>`_ R-to-Python adapter to provide all the features of the :mod:`ixmp` *Python* package::

    # Load the retixmp package
    library(retixmp)

    # An 'ixmp' object is added to the global namespace.
    # It can be used in the same way as the Python ixmp package.
    mp <- ixmp$Platform(dbtype = 'HSQLDB')
    scen <- ixmp$Scenario(mp, 'model name', 'scenario name', version = 'new')

    # etc.

One additional method, ``adapt_to_ret()`` is provided. Access its documentation with::

    ?retixmp::adapt_to_ret

.. warning::
   The *ixmp* source also contains an older R package called ``rixmp`` that provided reduced-functionality versions of :class:`ixmp.Platform` and :class:`ixmp.Scenario`.
   This code is unmaintained and untested, and users are strongly advised to use or migrate to ``retixmp``.

Java
----

The `ixmp` is powered by a Java interface to connect a database instance
with the scientific programming interfaces and the web user interface.
The full documentation of the ixmp Java source will be available shortly.
