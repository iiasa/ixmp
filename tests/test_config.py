try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path

import pytest

from ixmp.default_paths import find_dbprops


try:
    FileNotFoundError
except NameError:
    # Python 2.7
    FileNotFoundError = OSError


def test_find_dbprops():
    # Returns an absolute path
    expected_abs_path = Path.cwd() / 'foo.properties'
    expected_abs_path.write_text('bar', encoding='utf-8')

    assert find_dbprops('foo.properties') == Path(expected_abs_path)

    expected_abs_path.unlink()

    # Exception raised on missing file
    with pytest.raises(FileNotFoundError):
        find_dbprops('foo.properties')
