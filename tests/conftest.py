"""Test configuration.

Notes:

- For tests that fail strangely on Appveyor (Windows continuous integration),
  use a pattern like::

    import os

    @pytest.mark.xfail('APPVEYOR' in os.environ, strict=True
                       reason='Description of the issue.')
    def test_something(...):
        # etc.

"""
import os
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

import ixmp
import pytest


pytest_plugins = ['ixmp.testing']


# Hooks

def pytest_sessionstart(session):
    """Unset any configuration read from the user's directory."""
    ixmp.config._config.clear()


def pytest_report_header(config):
    """Add the ixmp import path to the pytest report header."""
    return 'ixmp location: {}'.format(os.path.dirname(ixmp.__file__))


# Fixtures

@pytest.fixture(scope='session')
def test_data_path(request):
    """Path to the directory containing test data."""
    return Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def tutorial_path(request):
    """Path to the directory containing the tutorials."""
    return Path(__file__).parent / '..' / 'tutorial'
