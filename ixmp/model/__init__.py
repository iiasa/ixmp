from .gams import GAMSModel


#: Mapping from names to available backends
MODELS = {
    'default': GAMSModel,
    'gams': GAMSModel,
}


def get_model(name, **args):
    try:
        return MODELS[name](**args)
    except KeyError:
        return MODELS['default'](name=name, **args)
