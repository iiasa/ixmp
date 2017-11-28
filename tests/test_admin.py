"""This file is a **NOT** used to test specific functions of IXMP, but rather
to print configuration information of the loaded package(s) to users!"""

import os
import ixmp


def test_config(capsys):
    with capsys.disabled():
        print('Running tests with the following configuration:')
        print('ixmp location: {}'.format(os.path.dirname(ixmp.__file__)))
