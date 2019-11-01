try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

import pytest

from ixmp._config import Config


try:
    FileNotFoundError
except NameError:
    # Python 2.7
    FileNotFoundError = OSError


@pytest.fixture
def cfg():
    """Return a :class:`ixmp.config.Config` object that doesn't read a file."""
    yield Config(read=False)


def test_locate_nonexistent(cfg):
    with pytest.raises(FileNotFoundError):
        cfg._locate('nonexistent')


def test_get(cfg):
    # This key has a value even with no configuration provided
    assert cfg.get('DEFAULT_LOCAL_DB_PATH')
