from .gams import GAMSModel


#: Mapping from names to available models. To register additional models,
#: add elements to this variable.
MODELS = {
    'default': GAMSModel,
    'gams': GAMSModel,
}


def get_model(name, **model_options):
    """Return a model for *name* (or the default) with *model_options*."""
    try:
        return MODELS[name](**model_options)
    except KeyError:
        return MODELS['default'](name=name, **model_options)
