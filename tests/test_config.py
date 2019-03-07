try:
    from pathlib import Path
    WindowsError = OSError
except ImportError:
    from pathlib2 import Path
import sys

import pytest

from ixmp.default_paths import find_dbprops


try:
    FileNotFoundError
except NameError:
    # Python 2.7
    FileNotFoundError = OSError


@pytest.mark.skipif(
    condition=sys.platform.startswith('win'),
    reason='https://github.com/mcmtroffaes/pathlib2/issues/45',
    raises=WindowsError)
def test_find_dbprops():
    # Returns an absolute path
    expected_abs_path = Path.cwd() / 'foo.properties'
    # u'' here is for python2 compatibility
    expected_abs_path.write_text(u'bar')

    assert find_dbprops('foo.properties') == Path(expected_abs_path)

    expected_abs_path.unlink()

    # Exception raised on missing file
    with pytest.raises(FileNotFoundError):
        find_dbprops('foo.properties')
