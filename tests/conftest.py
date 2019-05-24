import os
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

import ixmp
import pytest

# pytest hooks

pytest_plugins = ['ixmp.testing']


def pytest_addoption(parser):
    parser.addoption(
        "--win_ci_skip", action="store_true", default=False,
        help="weird skips for windows ci"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--win_ci_skip"):
        skip_win_ci = pytest.mark.skip(reason="weird effects on windows ci")
        for item in items:
            if "skip_win_ci" in item.keywords:
                item.add_marker(skip_win_ci)


def pytest_report_header(config, startdir):
    """Add the ixmp import path to the pytest report header."""
    return 'ixmp location: {}'.format(os.path.dirname(ixmp.__file__))


def pytest_sessionstart(session):
    """Unset any configuration read from the user's directory."""
    ixmp.config._config.clear()
    print(ixmp.config._config.values)


@pytest.fixture(scope='session')
def test_data_path(request):
    """Path to the directory containing test data."""
    return Path(__file__).parent / 'data'


@pytest.fixture
def tutorial_path(request):
    """Path to the directory containing tutorials."""
    return Path(__file__).parent / '..' / 'tutorial'
