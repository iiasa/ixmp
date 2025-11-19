from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypeGuard

import pandas as pd

if TYPE_CHECKING:
    from ixmp4.data.backend.db import SqlAlchemyBackend

    from ixmp.backend.ixmp4 import IXMP4Backend


@dataclass
class ContainerData:
    name: str
    kind: Literal["IndexSet", "Table", "Scalar", "Parameter", "Equation", "Variable"]
    records: (
        float
        | list[int]
        | list[float]
        | list[str]
        | dict[str, list[float] | list[int] | list[str]]
        | pd.DataFrame
        | None
    )
    domain: list[str] | None = None
    docs: str | None = None


def configure_logging_and_warnings() -> None:
    """Quiet verbose log and warning messages from :mod:`ixmp4`.

    These include:

    1. Log messages with level WARNING on logger ixmp4.data.db.base: “Dispatching a
       versioned insert statement on an 'sqlite' backend. This might be very slow!”
    2. :py:`PydanticDeprecatedSince211` (a subclass of :class:`DeprecationWarning`) in
       :py:`ixmp4.db.filters`: “Accessing the 'model_fields' attribute on the instance
       is deprecated.”
    3. :class:`pandas.errors.SettingWithCopyWarning` in :py:`ixmp4.data.db.base` at
       L589, L590, L621.
    4. :class:`FutureWarning` for top-level imports from :py:`pandera`.
    5. :class:`DeprecationWarning` for calling :meth:`datetime.datetime.now`.
    """
    import logging
    import warnings

    from pandas.errors import SettingWithCopyWarning

    logging.getLogger("ixmp4.data.db.base").setLevel(logging.WARNING + 1)

    warnings.filterwarnings(
        "ignore",
        ".*Accessing the 'model_fields' attribute on the instance.*",
        DeprecationWarning,  # Actually pydantic.PydanticDeprecatedSince211
        "ixmp4.db.filters",
    )
    warnings.filterwarnings(
        "ignore",
        ".*A value is trying to be set on a copy of a slice from a DataFrame.*",
        SettingWithCopyWarning,
        "ixmp4.data.db.base",
    )
    warnings.filterwarnings(
        "ignore",
        r"Importing pandas-specific classes .* from the\s+top-level pandera module",
        FutureWarning,
        "pandera",
    )
    warnings.filterwarnings(
        "ignore", "datetime.datetime.now", DeprecationWarning, "sqlalchemy.sql.schema"
    )


def format_url(value: str, **replacements: str) -> str:
    """Format an :mod:`ixmp4` compatible database URL.

    - :mod:`ixmp4` depends on :mod:`psycopg`, whereas :mod:`sqlalchemy` uses "psycopg2"
      as the default driver for backend "postgresql". The
      :attr:`sqlalchemy.engine.URL.drivername` is forced to "postgresql+psycopg".
    - Any other `replacements` are applied using :meth:`sqlalchemy.engine.URL.set`.
    """
    from sqlalchemy import make_url

    url = make_url(value)
    if "postgresql" in url.drivername:
        replacements.update(drivername="postgresql+psycopg")
    if replacements:
        url = url.set(**replacements)  # type: ignore [arg-type]
    return url.render_as_string(hide_password=False)


def is_ixmp4backend(obj: Any) -> TypeGuard["IXMP4Backend"]:
    """Type guard to ensure that `obj` is an IXMP4Backend.

    Example
    -------
    >>> import message_ix
    >>> from ixmp import Platform
    >>> mp = Platform(...)
    >>> s = message_ix.Scenario(mp, ...)
    >>> assert is_ixmp4backend(mp._backend)
    >>> assert is_ixmp4backend(s.platform._backend)
    """
    import ixmp.backend

    if "ixmp4" not in ixmp.backend.available():
        return False

    from ixmp.backend.ixmp4 import IXMP4Backend

    return isinstance(obj, IXMP4Backend)


def is_sqlalchemybackend(obj: Any) -> TypeGuard["SqlAlchemyBackend"]:
    """Ensure that `obj` is :class:`ixmp4.data.backend.db.SqlAlchemyBackend`."""
    from ixmp4.data.backend.db import SqlAlchemyBackend

    return isinstance(obj, SqlAlchemyBackend)
