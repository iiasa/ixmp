.. currentmodule:: ixmp.model

Mathematical models (:mod:`ixmp.model`)
=======================================

By default, the |ixmp| is installed with :class:`ixmp.model.gams.GAMSModel`, which performs calculations by executing code stored in GAMS files.

However, |ixmp| is extensible to support other methods of performing calculations or optimization.
Developers wishing to add such capabilities may subclass :class:`ixmp.model.base.Model` and implement its methods.


Provided models
---------------

.. automodule:: ixmp.model
   :members: get_model, MODELS

.. currentmodule:: ixmp.model.gams

.. automodule:: ixmp.model.gams
   :members:

.. currentmodule:: ixmp.model.dantzig

.. autoclass:: ixmp.model.dantzig.DantzigModel
   :members:


Model API
---------

.. currentmodule:: ixmp.model.base

.. automodule:: ixmp.model.base
   :members: ModelError

.. autoclass:: ixmp.model.base.Model
   :members: name, __init__, run, initialize, initialize_items, enforce

   In the following, the words **required**, **optional**, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

   Model is an *abstract* class; this means it **must** be subclassed.
   It has two **required** methods that **must** be overridden by subclasses:

   .. autosummary::
      __init__
      run

   The following attributes and methods are **optional** in subclasses.
   The default implementations are either empty or implement reasonable default behaviour.

   .. autosummary::
      enforce
      initialize
      initialize_items
      name
