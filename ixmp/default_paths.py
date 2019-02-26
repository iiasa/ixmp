import os
import sys

from ixmp import config


if sys.version[0] == 2:
    FileNotFoundError = OSError


def default_dbprops_file():
    return config.get('DEFAULT_DBPROPS_FILE')


def db_config_path():
    return config.get('DB_CONFIG_PATH')


def find_dbprops(fname):
    """Return the absolute path to a database properties file.

    Searches for a file named *fname*, first in the current working directory
    (`.`), then in the ixmp default location.

    Parameters
    ----------
    fname : str
        Name of a database properties file to locate.

    Returns
    -------
    str
        Absolute path to *fname*.

    Raises
    ------
    FileNotFoundError
        *fname* is not found in any of the search paths.
    """
    # Look in the current directory first, then the configured directory
    dirs = ['']

    try:
        # Catch exception raised by db_config_path() if no config file exists.
        # See TODO in config.get().
        dirs.append(db_config_path())
    except RuntimeError:
        pass

    for directory in dirs:
        path = os.path.abspath(os.path.join(directory, fname))
        if os.path.isfile(path):
            return path

    raise FileNotFoundError('Could not find {} in {!r}'.format(fname, dirs))
