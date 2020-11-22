import json
import logging
import os
from copy import copy, deepcopy
from pathlib import Path
from typing import Dict

log = logging.getLogger(__name__)


class _JSONEncoder(json.JSONEncoder):
    """Helper for writing config to file.

    The default JSONEncoder does not automatically convert pathlib.Path
    objects.
    """

    def default(self, o):
        if isinstance(o, Path):
            return str(o)
        return json.JSONEncoder.default(self, o)


def _iter_config_paths():
    """Yield recognized configuration paths, in order of priority."""
    try:
        yield "environment (IXMP_DATA)", Path(os.environ["IXMP_DATA"])
    except KeyError:
        pass

    try:
        yield "environment (XDG_DATA_HOME)", Path(os.environ["XDG_DATA_HOME"], "ixmp")
    except KeyError:
        pass

    yield "default", Path.home() / ".local" / "share" / "ixmp"


# Recognized configuration keys; name -> (type, default value)
KEYS = {
    "platform": (
        dict,
        {
            "default": "local",
            "local": {
                "class": "jdbc",
                "driver": "hsqldb",
                "path": next(_iter_config_paths())[1] / "localdb" / "default",
            },
        },
    ),
}


def _locate(filename=None):
    """Locate an existing *filename* in the ixmp config directories.

    If *filename* is None (the default), only directories are located.
    """
    tried = []
    for label, directory in _iter_config_paths():
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
        raise FileNotFoundError(f"Could not find {filename} in {repr(tried)}")
    else:
        raise FileNotFoundError(f"Could not find any of {repr(tried)}")


class Config:
    """Configuration for ixmp.

    Config stores two kinds of data: simple keys with a single value, and
    structured Platform information.

    ixmp has no built-in simple keys; however, it can store keys for other
    packages that build on ixmp, such as :mod:`message_ix`.

    Parameters
    ----------
    read : bool
        Read ``config.json`` on startup.
    """

    #: Full-resolved path of the ``config.json`` file.
    path = None

    # Configuration values
    values: Dict[str, object] = dict()

    def __init__(self, read=True):
        # Default values
        self.clear()

        # Read configuration from file; store the path at which it was located
        if read:
            self.read()

    def read(self):
        """Try to read configuration keys from file.

        If successful, the attribute :attr:`path` is set to the path of the
        file.
        """
        try:
            config_path = _locate("config.json")
            contents = config_path.read_text()
            self.values.update(json.loads(contents))
            self.path = config_path.resolve()
        except FileNotFoundError:
            pass
        except json.JSONDecodeError:
            print(config_path, contents)
            raise

    # Public methods

    def get(self, key):
        """Return the value of a configuration *key*."""
        return self.values[key]

    def register(self, name, type, default=None):
        """Register a new configuration key.

        Parameters
        ----------
        name : str
            Name of the new key; must not already exist.
        type : object
            Type of the key's value, such as :obj:`str` or
            :class:`pathlib.Path`.
        default : any, optional
            Default value for the key. If not supplied, the *type* is called
            to supply the default, value, e.g. ``str()``.
        """
        if name in KEYS:
            raise KeyError(f"configuration key {repr(name)} already defined")

        # Register the key for future clear()
        KEYS[name] = (type, default)

        # Also set on the current config object
        self.values[name] = default or type()

    def set(self, name, value):
        """Set configuration *key* to *value*."""
        if value is None:
            return

        type_, _ = KEYS[name]
        if not isinstance(value, type_):
            # Value is not of the expected type
            try:
                # Attempt to cast to the correct type
                value = type_(value)
            except Exception:
                raise TypeError(
                    f"expected {type_} for {repr(name)}; got {type(value)} "
                    f"{repr(value)}"
                )

        self.values[name] = value

    def clear(self):
        """Clear all configuration keys by setting empty or default values.

        :meth:`clear` also sets the default local platform::

          {
            "platform": {
              "default": "local",
              "local": {
                "class": "jdbc",
                "driver": "hsqldb",
                "path": "~/.local/share/ixmp/localdb/default"
              },
          }
        """
        self.values = dict()
        for name, (value_type, default) in KEYS.items():
            self.values[name] = default or value_type()

        # Set the default local database path; changed versus KEYS if IXMP_DATA
        # has been altered since the module was imported
        self.values["platform"]["local"]["path"] = (
            next(_iter_config_paths())[1] / "localdb" / "default"
        )

    def save(self):
        """Write configuration keys to file.

        ``config.json`` is created in the first of the ixmp configuration
        directories that exists. Only non-null values are written.
        """
        # Use the first identifiable path
        _, config_dir = next(_iter_config_paths())
        path = config_dir / "config.json"

        # TODO merge with existing configuration

        # Make the directory to contain the configuration file
        path.parent.mkdir(parents=True, exist_ok=True)

        values = deepcopy(self.values)
        for key, value_type in KEYS.items():
            if value_type is str and values[key] == "":
                values.pop(key)

        # Write the file
        log.info("Updating configuration file: {}".format(path))
        path.write_text(_JSONEncoder(indent=2).encode(values))

        # Update the path attribute to match the written file
        self.path = path

    def add_platform(self, name, *args, **kwargs):
        """Add or overwrite information about a platform.

        Parameters
        ----------
        name : str
            New or existing platform name.
        args
            Positional arguments. If *name* is 'default', *args* must be a
            single string: the name of an existing configured Platform.
            Otherwise, the first of *args* specifies one of the
            :obj:`~.BACKENDS`, and the remaining *args* are specific to that
            backend.
        kwargs
            Keyword arguments. These differ according to backend.
        """
        args = list(args)
        if name == "default":
            assert len(args) == 1
            info = args[0]

            if info not in self.values["platform"]:
                raise ValueError(f"Cannot set unknown {repr(info)} as default platform")
        else:
            cls = args.pop(0)
            info = {"class": cls}
            info.update(kwargs)

            if cls == "jdbc":
                info["driver"] = args.pop(0)
                assert info["driver"] in ("oracle", "hsqldb"), info["driver"]
                if info["driver"] == "oracle":
                    info["url"] = args.pop(0)
                    info["user"] = args.pop(0)
                    info["password"] = args.pop(0)
                elif info["driver"] == "hsqldb":
                    try:
                        info["path"] = Path(args.pop(0)).resolve()
                    except IndexError:
                        if "url" not in info:
                            raise ValueError(
                                "must supply either positional"
                                "path or url= keyword for "
                                "JDBCBackend with driver=hsqldb"
                            )
                assert len(args) == 0
            else:
                raise ValueError(cls)

        if name in self.values["platform"]:
            log.warning(
                "Overwriting existing config: " + repr(self.values["platform"][name])
            )

        self.values["platform"][name] = info

    def get_platform_info(self, name):
        """Return information on configured Platform *name*.

        Parameters
        ----------
        name : str
            Existing platform. If *name* is 'default', then the information for
            the default platform is returned.

        Returns
        -------
        dict
            The 'class' key specifies one of the :obj:`~.BACKENDS`.
            Other keys vary by backend class.

        Raises
        ------
        KeyError
            If *name* is not configured as a platform.
        """
        if name == "default":
            # The 'default' key stores the name of another config'd platform
            name = self.values["platform"].get(name, None)
        try:
            return name, copy(self.values["platform"][name])
        except KeyError:
            raise ValueError(
                f"platform name {repr(name)} not among "
                + repr(sorted(self.values["platform"].keys()))
                + f"\nfrom {self.path}"
            )

    def remove_platform(self, name):
        """Remove the configuration for platform *name*."""
        self.values["platform"].pop(name)


#: Default |ixmp| configuration object.
config = Config()
