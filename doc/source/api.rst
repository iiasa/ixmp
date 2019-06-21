Scientific Programming APIs
============================

The `ixmp` has application programming interfaces (API) for efficient scientific workflows and data processing.

.. toctree::
   :maxdepth: 2

   api-python
   reporting

R (``rixmp`` package)
-----------------------

An R interface to the `ixmp` is provided by the ``rixmp`` package.
``rixmp`` uses the `reticulate <https://rstudio.github.io/reticulate/>`_ R-to-Python adapter to provide all the features of the :mod:`ixmp` *Python* package::

    # Load the rixmp package
    library(rixmp)

    # An 'ixmp' object is added to the global namespace.
    # It can be used in the same way as the Python ixmp package.
    mp <- ixmp$Platform(dbtype = 'HSQLDB')
    scen <- ixmp$Scenario(mp, 'model name', 'scenario name', version = 'new')

    # etc.

One additional method, ``adapt_to_ret()`` is provided. Access its documentation with::

    ?rixmp::adapt_to_ret

.. warning::
   The *ixmp* source also contains an older R package, now called ``rixmp.legacy`` that provided reduced-functionality versions of :class:`ixmp.Platform` and :class:`ixmp.Scenario`.
   This code is unmaintained and untested, and users are strongly advised to use or migrate to ``rixmp``.

Major syntax differences between ``rixmp.legacy`` and the new ``rixmp`` (see tutorial for practical examples):

Initialization:

``rixmp.legacy``::

    library("rixmp.legacy")
    # launch the ix modeling platform using a local HSQL database instance
    mp <- ixmp.Platform(dbtype="HSQLDB")

``rixmp``::

    library("rixmp")
    ixmp <- import('ixmp')
    mp <- ixmp$Platform(dbtype="HSQLDB")

To load sets and parameter on the oracle database with ``rixmp.legacy`` the user need to load each data entry indipendently::

    scen$init_set("i")
    scen$add_set("i", "seattle")
    scen$add_set("i", "san-diego")

With ``rixmp`` the user can load entire sets of strings or dataframes, which require the additional function 'adapt_to_ret()'::

    scen$init_set("i")
    i.set = c("seattle","san-diego")
    scen$add_set("i", i.set )
    # load dataframes
    scen$init_par("a", c("i"))
    a.df = data.frame( i = i.set, value = c(350 , 600) , unit = 'cases')
    scen$add_par("a", adapt_to_ret(a.df))


Java
----

The `ixmp` is powered by a Java interface to connect a database instance
with the scientific programming interfaces and the web user interface.
The full documentation of the ixmp Java source will be available shortly.
