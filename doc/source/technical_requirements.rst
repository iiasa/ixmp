.. technical_requirements:

Technical requirements
======================

Getting started...
------------------

A high-quality desktop computer or laptop is sufficient for most purposes
using the |ixmp|.

- | *For new users:* 
  | *Please set up a* **GitHub account** (`github.com/join`_) *and get familiar 
    with forking and cloning repositories, as well as pulling, committing and pushing changes.*
  | *We recommend* **GitKraken** (`gitkraken.com`_) *for users who prefer a graphical user
    interface application to work with Github (as opposed to the command line).*

- | A **Python installation is required** for the installation scripts. For novice users,
    we recommend to install Anaconda and Spyder (`anaconda.org`_), version 3.6 or higher.
  | *No advanced knowledge of Python is required for getting started with the ix modeling platform.*

- For using `GAMS`_ to solve numerical optimisation problems,
  you need to install the latest version of GAMS (in particular 24.8 or higher).
  If you do only have a license for an older version,
  install both the older and the latest version of GAMS.


Scientific programming interface
--------------------------------

The scientific programming interface can be used either through Python or R:

- | **Python**: for novice users, we recommend to install Anaconda and Spyder
    (`anaconda.org`_). There are no known issues with the programming interface
    regarding Python 2.7 vs. 3.6 or higher.
  | The following package is required: ``jpype1``

- | **R**: visit the Comprehensive R Archive Network (`cran.r-project.org`_)
  | and install the R editor of your choice.
  | The following package is required: ``rJava``.


Installation and dependencies
-----------------------------

Please follow the instructions in the `README`_.


.. _`github.com/join` : https://github.com/join

.. _`gitkraken.com` : https://www.gitkraken.com/

.. _`anaconda.org` : https://anaconda.org/

.. _`GAMS` : http://www.gams.com

.. _`cran.r-project.org` : https://cran.r-project.org/

.. _`README` : https://github.com/iiasa/ixmp/blob/master/README.md
