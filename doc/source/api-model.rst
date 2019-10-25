.. currentmodule:: ixmp.model

Model formulations (:mod:`ixmp.model`)
======================================

By default, the |ixmp| is installed with :class:`ixmp.model.gams.GAMSModel`, which performs calculations by executing code stored in GAMS files.

However, |ixmp| is extensible to support other methods of performing calculations or optimization.
Developers wishing to add such capabilities may subclass :class:`ixmp.model.base.Model` and implement its methods.


Provided models
---------------

.. automodule:: ixmp.model
   :members: get_model, MODELS

.. autoclass:: ixmp.model.gams.GAMSModel
   :members:


Model API
---------

.. autoclass:: ixmp.model.base.Model
   :members: name, __init__, run

   In the following, the words REQUIRED, OPTIONAL, etc. have specific meanings as described in `IETF RFC 2119 <https://tools.ietf.org/html/rfc2119>`_.

   Model is an **abstract** class; this means it MUST be subclassed.
   It has two REQURIED methods that MUST be overridden by subclasses:

   .. autosummary::
      name
      __init__
      run
