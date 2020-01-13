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
from pathlib import Path

import pytest


pytest_plugins = ['ixmp.testing']
# Disable the faulthandler plugin on Windows to prevent spurious console noise;
# see https://github.com/jpype-project/jpype/issues/561
# https://github.com/iiasa/ixmp/issues/229
# https://github.com/iiasa/ixmp/issues/247
pytest_plugins.extend(['no:faulthandler'] if os.name == 'nt' else [])


# Hooks

def pytest_addoption(parser):
    parser.addoption(
        '--test-r',
        action='store_true',
        help='also run tests of the ixmp R interface.',
    )


def pytest_runtest_setup(item):
    if 'rixmp' in item.keywords and \
       not item.config.getoption('--test-r'):
        pytest.skip('skipping rixmp test without --test-r flag')


# Fixtures

@pytest.fixture(scope='session')
def test_data_path():
    """Path to the directory containing test data."""
    return Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def tutorial_path():
    """Path to the directory containing the tutorials."""
    return Path(__file__).parents[1] / 'tutorial'
