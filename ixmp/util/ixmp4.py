from dataclasses import dataclass
from typing import Literal, Optional, Union

import pandas as pd

# TODO Import this from typing when dropping Python 3.11

# These are based on existing calls within ixmp


@dataclass
class ContainerData:
    name: str
    kind: Literal["IndexSet", "Table", "Scalar", "Parameter", "Equation", "Variable"]
    records: Optional[
        Union[
            float,
            list[int],
            list[float],
            list[str],
            dict[str, Union[list[float], list[int], list[str]]],
            pd.DataFrame,
        ]
    ]
    domain: Optional[list[str]] = None
    docs: Optional[str] = None


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
