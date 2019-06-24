#!/usr/bin/env python
from __future__ import print_function

import glob

from setuptools import find_packages, setup
import versioneer


with open('README.md', 'r') as f:
    LONG_DESCRIPTION = f.read()


INSTALL_REQUIRES = [
    'JPype1>=0.6.2',
    'click',
    'dask[array]',
    'graphviz',
    'pandas',
    'pint',
    'PyYAML',
    'xarray<0.12',
    'xlsxwriter',
    'xlrd',
]

EXTRAS_REQUIRE = {
    'tests': ['pytest>=3.9', 'jupyter'],
    'docs': ['numpydoc', 'sphinx', 'sphinx_rtd_theme', 'sphinxcontrib-bibtex'],
    'tutorial': ['jupyter'],
}


def main():
    pack_dir = {
        'ixmp': 'ixmp',
    }
    entry_points = {
        'console_scripts': [
            'import-timeseries=ixmp.cli:import_timeseries',
            'ixmp-config=ixmp.cli:config',
            'ixmp=ixmp.cli:main',
        ],
    }
    lib_files = [x.split('ixmp/')[-1] for x in glob.glob('ixmp/lib/*')]
    db_files = [x.split('ixmp/')[-1]
                for x in glob.glob('ixmp/db/migration/*/*')]
    pack_data = {
        'ixmp': [
            'ixmp.jar',
        ] + lib_files + db_files,
    }
    setup_kwargs = {
        "name": "ixmp",
        "version": versioneer.get_version(),
        "cmdclass": versioneer.get_cmdclass(),
        "description": 'ix modeling platform',
        "long_description": LONG_DESCRIPTION,
        "long_description_content_type": 'text/markdown',
        "author": 'Daniel Huppmann, Matthew Gidden, Volker Krey, '
                  'Oliver Fricko, Peter Kolp',
        "author_email": 'message_ix@iiasa.ac.at',
        "url": 'https://github.com/iiasa/ixmp',
        "install_requires": INSTALL_REQUIRES,
        "extras_require": EXTRAS_REQUIRE,
        "packages": find_packages(),
        "package_dir": pack_dir,
        "package_data": pack_data,
        "entry_points": entry_points,
    }
    rtn = setup(**setup_kwargs)


if __name__ == "__main__":
    main()
