
from ixmp.core import *

from ixmp import (
    model_settings,
    utils,
    default_paths,
    config,
)

model_settings.register_model(
    'default',
    model_settings.ModelConfig(model_file='"{model}.gms"',
                               inp='{model}_in.gdx',
                               outp='{model}_out.gdx',
                               args=['--in="{inp}"', '--out="{outp}"'])
)

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
