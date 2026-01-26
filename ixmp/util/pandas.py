"""Compatibility utilities for :mod:`pandas`.

These are used to allow code to work with pandas version 2.x and 3.x.
"""

from importlib.metadata import version
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "SettingWithCopyWarning",
    "STRING_DTYPE",
]

if version("pandas") >= "3.":
    SettingWithCopyWarning: type[Warning] = Warning

    #: Default dtype for string columns. :class:`pandas.StringDtype` is available in
    #: pandas 2.3.3, but is not the default, so cannot be used directly.
    STRING_DTYPE: Any = pd.StringDtype(na_value=np.nan)
else:
    import pandas.errors

    SettingWithCopyWarning = pandas.errors.SettingWithCopyWarning

    STRING_DTYPE = object
