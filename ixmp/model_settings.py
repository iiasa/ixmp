# -*- coding: utf-8 -*-

from collections import namedtuple

ModelConfig = namedtuple('ModelConfig', ['model_file', 'inp', 'outp', 'args'])

_MODEL_REGISTRY = {}

def register_model(name, config):
    global _MODEL_REGISTRY
    _MODEL_REGISTRY[name] = config

def model_registered(name):
    global _MODEL_REGISTRY
    return name in _MODEL_REGISTRY

def model_config(name):
    global _MODEL_REGISTRY
    return _MODEL_REGISTRY[name]
