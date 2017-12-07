import os
from contextlib import contextmanager

import ixmp

dbpth = os.path.join(ixmp.default_paths.TEST_DIR, 'testdb')
prop_filename = os.path.join(dbpth, 'test.properties')
template_filename = os.path.join(dbpth, 'test.properties_template')


def write_props():
    # create properties file
    with open(template_filename, 'r') as f:
        lines = f.read()
    lines = lines.format(here=dbpth.replace("\\", "/"))
    with open(prop_filename, 'w') as f:
        f.write(lines)


@contextmanager
def test_props():
    write_props()
    yield
    os.remove(prop_filename)
