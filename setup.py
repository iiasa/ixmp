#!/usr/bin/env python
import glob

from setuptools import find_packages, setup
import versioneer


with open('README.md', 'r') as f:
    LONG_DESCRIPTION = f.read()

INSTALL_REQUIRES = [
    'JPype1>=0.7,!=0.7.2',
    'click',
    'dask[array]',
    'graphviz',
    'pandas>=1.0',
    'pint',
    'PyYAML',
    'xarray',
    'xlsxwriter',
    'xlrd',
]

EXTRAS_REQUIRE = {
    'tests': ['codecov', 'jupyter', 'pretenders>=1.4.4', 'pytest-cov',
              'pytest>=3.9'],
    'docs': ['numpydoc', 'sphinx', 'sphinx_rtd_theme', 'sphinxcontrib-bibtex'],
    'tutorial': ['jupyter'],
}

LIB_FILES = [x.split('ixmp/')[-1] for x in glob.glob('ixmp/lib/*')]

setup(
    name='ixmp',
    version=versioneer.get_version(),
    description='ix modeling platform',
    author='IIASA Energy Program',
    author_email='message_ix@iiasa.ac.at',
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://github.com/iiasa/ixmp',
    cmdclass=versioneer.get_cmdclass(),
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    packages=find_packages(),
    package_dir={'ixmp': 'ixmp'},
    package_data={'ixmp': ['ixmp.jar'] + LIB_FILES},
    entry_points={
        'console_scripts': [
            'ixmp=ixmp.cli:main',
        ],
    },
)
