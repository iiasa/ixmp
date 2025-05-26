Installation
************

Most users of :mod:`ixmp` will have it installed automatically as a dependency of :mod:`message_ix`,
for instance by following the :ref:`message_ix:install-quick` instructions.

.. important:: If that is the case,
   you **do not** need to also follow these instructions.

The sections below cover *other* use cases, such as:

- Using :mod:`ixmp` *alone*
  —that is, *without* :mod:`message_ix`,
  perhaps with other models or frameworks.

- Installing :mod:`ixmp` from source for development purposes,
  for instance for use with a source install of :mod:`message_ix`.

.. |message-ix-adv| replace:: the |MESSAGEix| :doc:`message_ix:install-adv`

They correspond to sections of |message-ix-adv|
and reference its contents.
They omit extra background information
and discussion found in that guide.
Be sure that you have the :doc:`prerequisite skills and knowledge <message_ix:prereqs>`;
these include specific points of knowledge
that are necessary to understand these instructions
and choose among different installation options.

To use :mod:`ixmp` from R, see :ref:`message_ix:install-r` in the |MESSAGEix| documentation.

.. contents::
   :local:

.. _system-dependencies:

Install system dependencies
===========================

Read and follow **each** of these sections of |message-ix-adv|:

- :ref:`message_ix:install-python`.
- :ref:`message_ix:install-java`.
- :ref:`message_ix:install-gams`.
- :ref:`message_ix:install-graphviz`.

Install :mod:`ixmp`
===================

Choose :program:`pip` or :program:`conda`
-----------------------------------------

Read :ref:`message_ix:install-pip-or-conda` in the |message-ix-adv|.

Whichever option you choose,
please skip over the unrelated sections below.

Create and activate a virtual environment
-----------------------------------------

Read and follow :ref:`message_ix:install-venv`.

Then,
according to your choice above,
follow *either* “Use pip” or “Use conda” below.

Use :program:`pip`
------------------

.. _install-extras:

Choose optional dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Optional dependencies
(also called “extra requirements”)
are gathered in groups.
The example commands below include a string like ``[docs]``.
This implies four of the five available groups of extra requirements for :mod:`ixmp`:

- ``docs`` includes packages required to build this documentation locally,
  including ``ixmp[tests]`` and all *its* requirements,
- ``ixmp4`` includes packages required to use the :class:`.IXMP4Backend`,
- ``report`` includes packages required to use the built-in :doc:`reporting <reporting>` features of :mod:`ixmp`,
- ``tests`` includes packages required to run the test suite,
  including ``ixmp[ixmp4]``, ``ixmp[report]``, ``ixmp[tutorial]`` and all the requirements in those groups, and
- ``tutorial`` includes packages required to run the :doc:`tutorials <tutorials>`.

Install the latest release from PyPI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Install :mod:`ixmp` [1]_::

    pip install ixmp[docs]

.. [1] If using the (non-standard) :program:`zsh` shell,
   note or recall that ``[...]`` is a `glob operator`_,
   so the argument to pip must be quoted appropriately:
   ``pip install 'ixmp[docs]'``.

At this point, installation is complete.

Install from GitHub
~~~~~~~~~~~~~~~~~~~

1. Run the following.
   Replace ``<ref>`` with a specific Git reference such as a branch name
   (for instance, the ``main`` development branch, or a branch associated with a pull request),
   a tag, or a commit hash::

    pip install git+ssh://git@github.com:iiasa/ixmp.git@<ref>[docs]

   ``git+ssh://`` assumes that you `use SSH to authenticate to GitHub`_,
   which we recommend.
   If you instead use other methods,
   then run::

    pip install git+https://github.com/iiasa/ixmp.git@<ref>[docs]

At this point, installation is complete.

Install from a :program:`git` clone of the source code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See the corresponding section in |message-ix-adv| for further details about editable installs,
registering a GitHub account,
and using a fork.

1. Clone either the main repository, or your fork;
   using the `Github Desktop`_ client,
   or the command line::

    git clone git@github.com:iiasa/ixmp.git

    # or:
    git clone git@github.com:<user>/ixmp.git

2. Navigate to the ``ixmp`` directory created by :program:`git clone` in step (2).
   Run the following [1]_::

    pip install --editable .[docs]

At this point, installation is complete.

Use :program:`conda`
--------------------

1. Configure conda to install :mod:`ixmp` from the conda-forge ‘channel’::

    conda config --prepend channels conda-forge

2. Install and configure the `mamba solver`_,
   which is faster and more reliable than conda's default solver::

    conda install conda-libmamba-solver
    conda config --set solver libmamba

3. Create a new conda environment and activate it.
   This step is **required** if using Anaconda_, but *optional* if using Miniconda_.
   This example uses the name ``ixmp-env``, but you can use any name of your choice::

    conda create --name ixmp-env
    conda activate ixmp-env

4. Install the :mod:`ixmp` package into the current environment
   (either ``ixmp-env``, or another name from the previous step)::

    conda install ixmp

At this point, installation is complete.

Troubleshooting
===============

To check that you have all dependencies installed,
or when reporting issues,
run the following::

   ixmp show-versions

.. _`glob operator`: https://zsh.sourceforge.io/Doc/Release/Expansion.html#Glob-Operators
.. _`mamba solver`: https://conda.github.io/conda-libmamba-solver/
.. _`use SSH to authenticate to GitHub`: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent
.. _`Github Desktop`: https://desktop.github.com
.. _Anaconda: https://www.continuum.io/downloads
.. _Miniconda: https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html
