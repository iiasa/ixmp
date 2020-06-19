from typing import Dict

import numpy as np
import xarray as xr

from .quantity import Quantity


def random_qty(shape: Dict[str, int], **kwargs):
    """Return a Quantity with *shape* and random contents.

    Parameters
    ----------
    shape : dict
        Mapping from dimension names to
    kwargs
        Other keyword arguments to :class:`Quantity`.

    Returns
    -------
    Quantity
        Keys in `shape`—e.g. "foo"—result in a dimension named "foo" with
        coords "foo0", "foo1", etc., with total length matching the value.
        Data is random.
    """
    return Quantity(
        xr.DataArray(
            np.random.rand(*shape.values()),
            coords={
                dim: [f"{dim}{i}" for i in range(length)]
                for dim, length in shape.items()
            },
            dims=shape.keys(),
        ),
        **kwargs,
    )
