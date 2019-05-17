from itertools import chain
import json
import os
try:
    from pathlib import Path
except ImportError:
    # Python 2.7 compatibility
    from pathlib2 import Path
    FileNotFoundError = OSError

from ixmp.utils import logger


class Config(object):
    """Configuration for ixmp.

    When imported, :mod:`ixmp` reads a configuration file `config.json` in the
    first of the following directories:

    1. `IXMP_DATA`, if defined.
    2. `${XDG_DATA_HOME}/ixmp`, if defined.
    3. `$HOME/.local/share/ixmp`.
    4. `$HOME/.local/ixmp` (used by ixmp <= 1.1).

    The file may define either or both of the following configuration keys, in
    JSON format:

    - `DB_CONFIG_PATH`: location for database properties files. A
      :class:`ixmp.Platform` instantiated with a relative path name for the
      `dbprops` argument will locate the file first in the current working
      directory, then in `DB_CONFIG_PATH`, then in the four directories above.
    - `DEFAULT_DBPROPS_FILE`: path to a default database properties file. A
      :class:`ixmp.Platform` instantiated with no arguments will use this file.
    - `DEFAULT_LOCAL_DB_PATH`: path to a directory where a local directory
      should be created. A :class:`ixmp.Platform` instantiated with
      `dbtype='HSQLDB'` will create or reuse a database in this path.

    Parameters
    ----------
    read : bool
        Read `config.json` on startup.

    """
    # User configuration keys
    _keys = [
        'DB_CONFIG_PATH',
        'DEFAULT_DBPROPS_FILE',
        'DEFAULT_LOCAL_DB_PATH',
    ]

    def __init__(self, read=True):
        # Default values
        self.clear()

        # Read configuration from file; store the path at which it was located
        if read:
            self.read()

    def _iter_paths(self):
        """Yield recognized paths, in order of priority."""
        print('Config._iter_paths', os.environ, Path.home())

        try:
            yield 'environment (IXMP_DATA)', Path(os.environ['IXMP_DATA'])
        except KeyError:
            pass

        try:
            yield 'environment (XDG_DATA_HOME)', \
                Path(os.environ['XDG_DATA_HOME'], 'ixmp')
        except KeyError:
            pass

        yield 'default', Path.home() / '.local' / 'share' / 'ixmp'
        yield 'default (ixmp<=1.1)', Path.home() / '.local' / 'ixmp'

    def _locate(self, filename=None, dirs=[]):
        """Locate an existing *filename* in the ixmp config directories.

        If *filename* is None (the default), only directories are located.
        If *dirs* are provided, they are tried in order before the ixmp config
        directories.
        """
        tried = []
        dirs = map(lambda d: ('arg', d), dirs)
        for label, directory in chain(dirs, self._iter_paths()):
            try:
                directory = Path(directory)
            except TypeError:
                # e.g. 'DB_CONFIG_PATH' via find_dbprops() is None
                continue

            if filename:
                # Locate a specific file
                if (directory / filename).exists():
                    return directory / filename
                else:
                    tried.append(str(directory))
            else:
                # Locate an existing directory
                if directory.exists():
                    return directory
                else:
                    tried.append(str(directory))

        if filename:
            raise FileNotFoundError('Could not find {} in {!r}'
                                    .format(filename, tried))
        else:
            raise FileNotFoundError('Could not find any of {!r}'.format(tried))

    def read(self):
        """Try to read configuration keys from file.

        If successful, the configuration key 'CONFIG_PATH' is set to the path
        of the file.
        """
        try:
            config_path = self._locate('config.json')
            contents = config_path.read_text()
            self.values.update(json.loads(contents))
            self.values['CONFIG_PATH'] = config_path
        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
            print(config_path, contents)
            raise

    # Public methods

    def get(self, key):
        """Return the value of a configuration *key*."""
        return self.values[key]

    def set(self, key, value):
        """Set configuration *key* to *value*."""
        assert key in self.values
        if value is None:
            return
        self.values[key] = value

    def clear(self):
        """Clear all configuration keys by setting their values to None."""
        self.values = {key: None for key in self._keys}

        # Set 'DEFAULT_LOCAL_DB_PATH'
        # Use the first identifiable path
        _, config_dir = next(self._iter_paths())
        self.values['DEFAULT_LOCAL_DB_PATH'] = (config_dir / 'localdb' /
                                                'default')

    def save(self):
        """Write configuration keys to file.

        `config.json` is created in the first of the ixmp configuration
        directories that exists. Only non-null values are written.
        """
        # Use the first identifiable path
        _, config_dir = next(self._iter_paths())
        path = config_dir / 'config.json'

        # TODO merge with existing configuration

        # Make the directory to contain the configuration file
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write the file
        logger().info('Updating configuration file: {}'.format(path))
        # str() here is for py2 compatibility
        with open(str(path), 'w') as f:
            json.dump({k: str(self.values[k]) for k in self._keys if
                       self.values[k] is not None}, f)

    def find_dbprops(self, fname):
        """Return the absolute path to a database properties file.

        Searches for a file named *fname*, first in the current working
        directory (`.`), then in the ixmp default location.

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
        if fname is None:
            # Use the default
            return self.get('DEFAULT_DBPROPS_FILE')
        else:
            # Look in the current directory first, then the config directories
            return self._locate(fname,
                                dirs=[Path.cwd(), self.get('DB_CONFIG_PATH')])


_config = Config()
