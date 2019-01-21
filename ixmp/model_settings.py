# -*- coding: utf-8 -*-

from collections import namedtuple

ModelConfig = namedtuple('ModelConfig', ['model_file', 'inp', 'outp', 'args'])

_MODEL_REGISTRY = {}


def register_model(name, config):
    """Register a new model with ixmp.

    Parameters
    ----------
    name : str
        Model name.
    config : dict
        Configuration settings for the model, with the following keys. Each key
        is a format string that may use certain named values:

        - `model_file` (:class:`str`): GAMS source file (``.gms``) containing
          the model. E.g. "{model}_run.gms". Available values: model, case.
        - `inp` (:class:`str`): input path; location where the model expects a
          GDX file containing input data. E.g. "{model}_{case}_in.gdx".
          Available values: model, case.
        - `outp` (:class:`str`): output path; location where the model will
          create a GDX file containing output data. E.g.
          "{model}_{case}_out.gdx". Available values: model, case.
        - `args` (:class:`list` of :class:`str`): additional GAMS command-line
          args to be passed when invoking the model. Available values: model,
          case, inp, outp.

        The `model` and `case` formatting values are generated from
        :attr:`ixmp.Scenario.model` and :attr:`ixmp.Scenario.scenario`,
        respectively, with spaces (“ ”) converted to underscores ("_").
    """
    global _MODEL_REGISTRY
    _MODEL_REGISTRY[name] = config


def model_registered(name):
    global _MODEL_REGISTRY
    return name in _MODEL_REGISTRY


def model_config(name):
    global _MODEL_REGISTRY
    return _MODEL_REGISTRY[name]
