import pytest


pytest_plugins = ['ixmp.testing']


# Hooks

def pytest_addoption(parser):
    parser.addoption(
        '--test-r',
        action='store_true',
        help='also run tests of the ixmp R interface.',
    )
    parser.addoption(
        '--test-gc',
        action='store_true',
        default=False,
        help='also run GC tests.',
    )


def pytest_runtest_setup(item):
    if 'rixmp' in item.keywords and \
       not item.config.getoption('--test-r'):
        pytest.skip('skipping rixmp test without --test-r flag')
    test_gc = item.config.getoption('--test-gc')
    if bool('test_gc' in item.keywords) != bool(test_gc):
        pytest.skip('skipping gc test without --test-gc flag')
