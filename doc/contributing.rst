.. include:: ../CONTRIBUTING.rst

Performance tests
=================

The test suite contains performance tests, which are disabled by the default options given in :file:`setup.cfg`.
To run these, use::

    pytest … --benchmark-only …
