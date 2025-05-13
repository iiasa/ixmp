"""Types for type hinting and checking of :mod:`ixmp` and downstream code."""

from typing import NotRequired, TypedDict, Union


class PlatformArgs(TypedDict, total=False):
    """Arguments to :class:`.Platform`."""

    name: NotRequired[Union[str, None]]
    # NB The class itself has Literal["ixmp4", "jdbc"] first as an aid to completion in
    #    IDE/interactive use, but for downstream code any str is valid, though may raise
    #    in get_class().
    backend: NotRequired[Union[str, None]]
