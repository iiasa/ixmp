import os
import os.path

import pytest

from ixmp.default_paths import find_dbprops


def test_find_dbprops():
    # Returns an absolute path
    with open('foo.properties', 'w') as f:
        f.write('bar')

    expected_abs_path = os.path.join(os.getcwd(), 'foo.properties')
    assert find_dbprops('foo.properties') == expected_abs_path

    os.remove('foo.properties')

    # Exception raised on missing file
    with pytest.raises(FileNotFoundError):
        find_dbprops('foo.properties')
