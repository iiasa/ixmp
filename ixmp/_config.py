import json
import logging
import os
from copy import copy
from dataclasses import asdict, dataclass, field, fields, make_dataclass
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)


class _JSONEncoder(json.JSONEncoder):
    """Helper for writing config to file.

    The default JSONEncoder does not automatically convert pathlib.Path objects.
    """

    def default(self, o):
        if isinstance(o, Path):
            return str(o)
        return json.JSONEncoder.default(self, o)


def _iter_config_paths():
    """Yield recognized configuration paths, in order of priority."""
    try:
        yield "environment (IXMP_DATA)", Path(os.environ["IXMP_DATA"]).resolve()
    except KeyError:
        pass

    try:
        yield (
            "environment (XDG_DATA_HOME)",
            Path(os.environ["XDG_DATA_HOME"], "ixmp").resolve(),
        )
    except KeyError:
        pass

    yield "default", Path.home().joinpath(".local", "share", "ixmp")


def _locate(filename=None):
    """Locate an existing director or `filename` in the ixmp configuration directory.

    If `filename` is :obj:`None` (the default), only directories are located.
    """
    tried = []
    for label, directory in _iter_config_paths():
        # Locate either a specific file or an existing directory
        path = directory.joinpath(filename) if filename else directory

        if path.exists():
            return path.resolve()

        tried.append(str(directory))

    raise FileNotFoundError(
        "Could not find " + (f"{filename} in " if filename else "any of ") + repr(tried)
    )


def _platform_default():
    """Default values for the `platform` setting on BaseValues."""
    try:
        from ixmp.util.ixmp4 import configure_logging_and_warnings

        configure_logging_and_warnings()

        import ixmp4.conf

        # Use configured ixmp4 storage directory
        ixmp4_databases = ixmp4.conf.settings.storage_directory.joinpath("databases")
    except ImportError:
        # ixmp4 not installed or importable; construct a likely value
        from platformdirs import user_data_path

        ixmp4_databases = user_data_path("ixmp4").joinpath("databases")

    return {
        "default": "local",
        "local": {
            "class": "jdbc",
            "driver": "hsqldb",
            "path": next(_iter_config_paths())[1].joinpath("localdb", "default"),
        },
        "ixmp4-local": {
            "class": "ixmp4",
            "dsn": f"sqlite:///{ixmp4_databases.joinpath('local.sqlite3')}",
            "ixmp4_name": "local",
            "jdbc_compat": True,
        },
    }


@dataclass
class BaseValues:
    """Base class for storing configuration values."""

    platform: dict = field(default_factory=_platform_default)

    def __getitem__(self, name):
        return getattr(self, name.replace(" ", "_"))

    def __setitem__(self, name, value):
        setattr(self, name.replace(" ", "_"), value)

    def add_field(self, name, type_, default, **kwargs):
        # Check `name`
        name = name.replace(" ", "_")
        if (
            name in self.__dataclass_fields__
            and "auto" not in self.__dataclass_fields__[name].metadata
        ):
            raise ValueError(f"configuration key {repr(name)} already defined")

        # Create a new data class with an additional field
        new_cls = make_dataclass(
            "Values",
            [(name, type_, field(default=default, **kwargs))],
            bases=(self.__class__,),
        )

        # Re-use current values and any defaults for the new fields
        return new_cls, new_cls(**asdict(self))

    def delete_field(self, name):
        # Check `name`
        name = self.munge(name)
        if name in BaseValues.__dataclass_fields__:
            raise ValueError(f"cannot remove ixmp core configuration key {repr(name)}")

        # Create a new dataclass, removing `name`
        fields = []
        for f in self.__dataclass_fields__.values():
            if f.name == name or f in BaseValues.__dataclass_fields__:
                continue
            fields.append((f.name, f.type, f))
        new_cls = make_dataclass("Values", fields, bases=(BaseValues,))

        # Reuse current values, discarding the deleted field
        data = asdict(self)
        data.pop(name)
        return new_cls, new_cls(**data)

    def keys(self) -> tuple[str, ...]:
        return tuple(map(lambda f: f.name.replace("_", " "), fields(self)))

    def set(self, name: str, value: Any, strict: bool = True):
        f = self.get_field(name)
        if strict and f is None:
            raise KeyError(name)

        # Retrieve the type for `name`; or None if unregistered
        type_ = getattr(f, "type", None)

        try:
            # Attempt to cast to the correct type, if any
            value = type_(value) if type_ else value
        except Exception:
            raise TypeError(
                f"expected {type_} for {repr(name)}; got {repr(value)} ({type(value)})"
            )

        setattr(self, getattr(f, "name", name.replace(" ", "_")), value)

    # Utilities

    def get_field(self, name):
        """For `name` = "field name", retrieve a field "field_name", if any."""
        for f in fields(self):
            if f.name in (name, name.replace(" ", "_")):
                return f

    def munge(self, name):
        """Return a field name matching `name`."""
        return self.get_field(name).name or name


class Config:
    """Configuration for ixmp.

    For most purposes, there is only one instance of this class, available at
    :data:`.ixmp.config` and automatically :meth:`read` from the ixmp configuration
    file at the moment the package is imported. (:meth:`save` writes the current values
    to file.)

    Config is a key-value store. Key names are strings; each key has values of a fixed
    type. Individual keys can be accessed with :meth:`get` and :meth:`set`, or by
    accessing the :attr:`values` attribute.

    Spaces in names are automatically replaced with underscores, e.g. "my key" is
    stored as "my_key", but may be set and retrieved as "my key".

    Downstream packages (e.g. :mod:`message_ix`, :mod:`message_ix_models`) may
    :meth:`register` additional keys to be stored in and read from the ixmp
    configuration file.

    The default configuration (restored by :meth:`clear`) is:

    .. code-block:: json

       {
         "platform": {
           "default": "local",
           "local": {
             "class": "jdbc",
             "driver": "hsqldb",
             "path": "~/.local/share/ixmp/localdb/default"
           },
       }

    .. autosummary::
       clear
       get
       keys
       read
       save
       set
       register
       unregister
       add_platform
       get_platform_info
       remove_platform

    Parameters
    ----------
    read : bool
        Read ``config.json`` on startup.
    """

    #: Fully-resolved path of the ``config.json`` file.
    path: Optional[Path] = None

    #: Configuration values. These can be accessed using Python item access syntax, e.g.
    #: ``ixmp.config.values["platform"]["platform name"]…``.
    values: BaseValues

    _ValuesClass: type[BaseValues]

    def __init__(self, read: bool = True):
        self._ValuesClass = BaseValues

        # Default values
        self.values = self._ValuesClass()

        # Read configuration from file; store the path at which it was located
        if read:
            self.read()

    def read(self):
        """Try to read configuration keys from file.

        If successful, the attribute :attr:`path` is set to the path of the file.
        """
        try:
            # Locate and read the configuration file
            config_path = _locate("config.json")
        except FileNotFoundError:
            contents = "{}"
        else:
            self.path = config_path.resolve()
            contents = config_path.read_text()

        try:
            data = json.loads(contents)
        except json.JSONDecodeError:
            print(config_path, contents)
            raise

        # Parse JSON and set values
        for key, value in data.items():
            try:
                self.set(key, value)  # Cast type for registered keys
            except KeyError:
                # Automatically register new values
                self.register(key, type(value), default=None, metadata=dict(auto=True))
                self.set(key, value)

    # Public methods

    def get(self, name: str) -> Any:
        """Return the value of a configuration key `name`."""
        return self.values[name]

    def keys(self) -> tuple[str, ...]:
        """Return the names of all registered configuration keys."""
        return self.values.keys()

    def register(self, name: str, type_: type, default: Optional[Any] = None, **kwargs):
        """Register a new configuration key.

        Parameters
        ----------
        name : str
            Name of the new key.
        type_ : object
            Type of valid values for the key, e.g. :obj:`str` or :class:`pathlib.Path`.
        default : optional
            Default value for the key. If not supplied, the `type` is called to supply
            the default value, e.g. ``str()``.

        Raises
        ------
        ValueError
            if the key `name` is already registered.
        """
        self._ValuesClass, self.values = self.values.add_field(
            name, type_, default, **kwargs
        )

    def unregister(self, name: str) -> None:
        """Unregister and clear the configuration key `name`."""
        self._ValuesClass, self.values = self.values.delete_field(name)

    def set(self, name: str, value: Any, _strict: bool = True):
        """Set configuration key `name` to `value`.

        Parameters
        ----------
        value :
            Value to store. If :obj:`None`, :func:`set` has no effect.
        """
        if value is None:
            return

        self.values.set(name, value, _strict)

    def clear(self):
        """Clear all configuration keys by setting empty or default values."""
        self.values = self._ValuesClass()

    def save(self):
        """Write configuration keys to file.

        ``config.json`` is created in the first of the ixmp configuration directories
        that exists. Only non-null values are written.
        """
        # Use the first identifiable path
        _, config_dir = next(_iter_config_paths())
        path = config_dir / "config.json"

        # TODO merge with existing configuration

        # Make the directory to contain the configuration file
        path.parent.mkdir(parents=True, exist_ok=True)

        values = asdict(self.values)

        # Write the file
        log.info(f"Updating configuration file: {path}")
        path.write_text(_JSONEncoder(indent=2).encode(values))

        # Update the path attribute to match the written file
        self.path = path

    def add_platform(self, name: str, *args, **kwargs):
        """Add or overwrite information about a platform.

        Parameters
        ----------
        name : str
            New or existing platform name.
        args
            Positional arguments. If `name` is 'default', `args` must be a single
            string: the name of an existing configured Platform. Otherwise, the first
            of `args` specifies one of the :obj:`~.BACKENDS`, and the remaining `args`
            differ according to the backend.
        kwargs
            Keyword arguments. These differ according to backend.

        See also
        --------
        .Backend.handle_config
        .JDBCBackend.handle_config
        """
        if name == "default":
            assert len(args) == 1
            info = args[0]

            if info not in self.values["platform"]:
                raise ValueError(f"Cannot set unknown {repr(info)} as default platform")
        else:
            from ixmp.backend import get_class

            _args = list(args)

            try:
                # Get the backend class
                cls = _args.pop(0)
                backend_class = get_class(cls)
            except IndexError:
                raise ValueError("Must give at least 1 arg: backend class")

            # Use the backend class' method to handle the arguments
            info = backend_class.handle_config(_args, kwargs)
            info.setdefault("class", cls)

        if name in self.values["platform"]:
            log.warning(
                "Overwriting existing config: " + repr(self.values["platform"][name])
            )

        self.values["platform"][name] = info

    def get_platform_info(self, name: str) -> tuple[str, dict[str, Any]]:
        """Return information on configured Platform `name`.

        Parameters
        ----------
        name : str
            Existing platform. If `name` is "default", the information for the default
            platform is returned.

        Returns
        -------
        str
            The name of the platform. If `name` was "default", this will be the actual
            name of platform that is designated default.
        dict
            The "class" key specifies one of the :obj:`~.BACKENDS`. Other keys vary by
            backend class.

        Raises
        ------
        ValueError
            If `name` is not configured as a platform.
        """
        if name == "default":
            # The 'default' key stores the name of another config'd platform
            name = self.values["platform"].get(name, None)
        try:
            return name, copy(self.values["platform"][name])
        except KeyError:
            raise ValueError(
                f"platform name {name!r} not among {sorted(self.values['platform'])!r}"
                f"\nfrom {self.path}"
            ) from None

    def remove_platform(self, name: str):
        """Remove the configuration for platform `name`."""
        self.values["platform"].pop(name)


#: Default :mod:`ixmp` configuration object.
config = Config()
