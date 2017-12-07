#!/usr/bin/env python
from __future__ import print_function

import os
import shutil
import sys
import glob

from setuptools import setup, Command, find_packages
from setuptools.command.install import install

INFO = {
    'version': '0.1.0',
}

paths = """
import os

fullpath = lambda *x: os.path.abspath(os.path.join(*x))

ROOT_DIR = fullpath(r'{here}')
TEST_DIR = fullpath(r'{here}', 'tests')
CONFIG_DIR = fullpath(r'{here}', 'config')

""".format(here=os.path.dirname(os.path.realpath(__file__)))


class Cmd(install):
    """Custom clean command to tidy up the project root."""

    def initialize_options(self):
        install.initialize_options(self)

    def finalize_options(self):
        install.finalize_options(self)

    def run(self):
        install.run(self)
        dirs = [
            'ixmp.egg-info',
            'build',
        ]
        for d in dirs:
            print('removing {}'.format(d))
            shutil.rmtree(d)


def main():
    packages = [
        'ixmp',
    ]
    pack_dir = {
        'ixmp': 'ixmp',
    }
    entry_points = {
        'console_scripts': [
            'import-timeseries=ixmp.cli:import_timeseries',
        ],
    }
    cmdclass = {
        'install': Cmd,
    }
    lib_files = [x.split('ixmp/')[-1] for x in glob.glob('ixmp/lib/*')]
    db_files = [x.split('ixmp/')[-1]
                for x in glob.glob('ixmp/db/migration/hsql/*')]
    pack_data = {
        'ixmp': [
            'ixmp.R',
            'ixmp.jar',
        ] + lib_files + db_files,
    }
    setup_kwargs = {
        "name": "ixmp",
        "version": INFO['version'],
        "description": 'ix modeling platform',
        "author": 'Daniel Huppmann, Matthew Gidden, Volker Krey, '
                  'Oliver Fricko, Peter Kolp',
        "author_email": 'message_ix@iiasa.ac.at',
        "url": 'http://github.com/iiasa/message_ix',
        "packages": packages,
        "package_dir": pack_dir,
        "package_data": pack_data,
        "entry_points": entry_points,
        "cmdclass": cmdclass,
    }
    print('Writing default_paths.py')
    pth = os.path.join('ixmp', 'default_paths.py')
    with open(pth, 'w') as f:
        f.write(paths)
    rtn = setup(**setup_kwargs)
    print('Removing default_paths.py')
    os.remove(pth)

if __name__ == "__main__":
    main()
