from ._version import get_versions
__version__ = get_versions()['version']
del get_versions

from ixmp.core import (  # noqa: E402,F401
    Platform,
    TimeSeries,
    Scenario,
)

from ixmp.model_settings import ModelConfig, register_model  # noqa: E402
from ixmp.reporting import Reporter  # noqa: F401


register_model(
    'default',
    ModelConfig(model_file='"{model}.gms"',
                inp='{model}_in.gdx',
                outp='{model}_out.gdx',
                args=['--in="{inp}"', '--out="{outp}"'])
)
