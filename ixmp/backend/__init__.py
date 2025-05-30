"""Backend API."""

from typing import TYPE_CHECKING

from .common import ItemType

if TYPE_CHECKING:
    import ixmp.backend.base

__all__ = [
    "ItemType",
    "available",
    "get_class",
]

#: Mapping from names to available backends. To register additional backends, add
#: entries to this dictionary.
BACKENDS: dict[str, type["ixmp.backend.base.Backend"]] = {}


def get_class(name: str) -> type["ixmp.backend.base.Backend"]:
    """Return a reference to a :class:`~.base.Backend` subclass.

    Note that unlike :func:`.model.get_class`, this function does not create a new
    instance.
    """
    if name == "ixmp4":
        from ixmp.util.ixmp4 import configure_logging_and_warnings

        configure_logging_and_warnings()

        from . import ixmp4

        BACKENDS[name] = ixmp4.IXMP4Backend
    elif name == "jdbc":
        from . import jdbc

        BACKENDS[name] = jdbc.JDBCBackend

    try:
        return BACKENDS[name]
    except KeyError:
        names = set(BACKENDS.keys()) | {"ixmp4", "jdbc"}
        raise ValueError(f"backend class {name!r} not among {sorted(names)}")


def available() -> list[str]:
    """Return a list of available backend names."""
    for name in "ixmp4", "jdbc":
        try:
            get_class(name)
        except Exception:
            pass

    return sorted(BACKENDS)
