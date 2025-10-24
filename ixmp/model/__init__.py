from typing import TYPE_CHECKING, Any

from .dantzig import DantzigModel
from .gams import GAMSModel

if TYPE_CHECKING:
    from .base import Model

#: Mapping from names to available models. To register additional models, add elements
#: to this variable.
MODELS: dict[str, type["Model"]] = {
    "default": GAMSModel,
    "gams": GAMSModel,
    "dantzig": DantzigModel,
}


def get_model(name: str | None, **model_options: Any) -> "Model":
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
