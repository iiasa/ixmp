from typing import TYPE_CHECKING, Optional, Union

# TODO Import from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from .dantzig import DantzigModel
from .gams import GAMSModel

if TYPE_CHECKING:
    from ixmp.types import GamsModelInitKwargs


#: Mapping from names to available models. To register additional models, add elements
#: to this variable.
MODELS: dict[
    str, Union[type[GAMSModel], type[DantzigModel]]
] = {  # type["ixmp.model.base.Model"]
    "default": GAMSModel,
    "gams": GAMSModel,
    "dantzig": DantzigModel,
}


def get_model(
    name: Optional[str], **model_options: Unpack["GamsModelInitKwargs"]
) -> Union[GAMSModel, DantzigModel]:
    """Return a model instance for `name` (or the default) with `model_options`.

    Note that unlike :func:`.backend.get_class`, this function creates a new instance.

    Parameters
    ----------
    name :
        Model class name; a key in :data:`MODELS`. If `name` is not in MODELS, then
        :py:`MODELS["default"]` is used, and `name` is passed as an additional keyword
        argument to the class constructor.
    """
    try:
        assert name is not None
        cls = MODELS[name]
    except (AssertionError, KeyError):
        cls = MODELS["default"]
        model_options.setdefault("name_", name)
    return cls(**model_options)
