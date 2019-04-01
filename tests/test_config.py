try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

import pytest

from ixmp.config import Config


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


def test_find_dbprops(cfg):
    # Returns an absolute path
    expected_abs_path = Path.cwd() / 'foo.properties'
    # u'' here is for python2 compatibility
    expected_abs_path.write_text(u'bar')

    assert cfg.find_dbprops('foo.properties') == Path(expected_abs_path)

    expected_abs_path.unlink()

    # Exception raised on missing file
    with pytest.raises(FileNotFoundError):
        cfg.find_dbprops('foo.properties')
