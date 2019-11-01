from copy import copy, deepcopy
import json
import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)


class _JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Path):
            return str(o)
        return json.JSONEncoder.default(self, o)


class Config:
    """Configuration for ixmp.

    Parameters
    ----------
    read : bool
        Read `config.json` on startup.
    """
    # User configuration keys
    _keys = {
        'platform': dict,
    }

    path = None

    def __init__(self, read=True):
        # Default values
        self.clear()

        # Read configuration from file; store the path at which it was located
        if read:
            self.read()

    def _iter_paths(self):
        """Yield recognized paths, in order of priority."""
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

    def _locate(self, filename=None):
        """Locate an existing *filename* in the ixmp config directories.

        If *filename* is None (the default), only directories are located.
        """
        tried = []
        for label, directory in self._iter_paths():
            if filename:
                # Locate a specific file
                if (directory / filename).exists():
                    return directory / filename
            else:
                # Locate an existing directory
                if directory.exists():
                    return directory
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
            self.path = config_path
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
        self.values = {key: val_type() for key, val_type in self._keys.items()}

        # Set the default local database path
        _, config_dir = next(self._iter_paths())
        self.values['platform'] = {
            'default': 'local',
            'local': {
                'class': 'jdbc',
                'driver': 'hsqldb',
                'path': config_dir / 'localdb' / 'default',
            }
        }

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

        values = deepcopy(self.values)
        for key, value_type in self._keys.items():
            if value_type is str and values[key] == '':
                values.pop(key)

        # Write the file
        log.info('Updating configuration file: {}'.format(path))
        path.write_text(_JSONEncoder(indent=2).encode(values))

    def add_platform(self, name, *args):
        args = list(args)
        if name == 'default':
            assert len(args) == 1
            info = args[0]
        else:
            cls = args.pop(0)
            info = {'class': cls}

            if cls == 'jdbc':
                info['driver'] = args.pop(0)
                assert info['driver'] in ('oracle', 'hsqldb'), info['driver']
                if info['driver'] == 'oracle':
                    info['url'] = args.pop(0)
                    info['user'] = args.pop(0)
                    info['password'] = args.pop(0)
                elif info['driver'] == 'hsqldb':
                    info['path'] = args.pop(0)
                assert len(args) == 0
            else:
                raise ValueError(cls)

        if name in self.values['platform']:
            log.warning('Overwriting existing config: {!r}'
                        .format(self.values['platform'][name]))

        self.values['platform'][name] = info

    def get_platform_info(self, name):
        if name == 'default':
            # The 'default' key stores the name of another config'd platform
            name = self.values['platform'].get(name, None)
        try:
            return name, copy(self.values['platform'][name])
        except KeyError:
            message = 'platform name {!r} not among {!r}\nfrom {}' \
                .format(name, sorted(self.values['platform'].keys()),
                        self.path)
            raise ValueError(message)

    def remove_platform(self, name):
        self.values['platform'].pop(name)


config = Config()
