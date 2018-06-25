import json
import os

from ixmp.default_path_constants import CONFIG_PATH
from ixmp.utils import logger


def get(key):
    """Return key from configuration file"""
    if not os.path.exists(CONFIG_PATH):
        raise RuntimeError(
            'ixmp has not been configured, do so with `$ ixmp-config -h`')

    with open(CONFIG_PATH, mode='r') as f:
        data = json.load(f)

    if key not in data:
        raise RuntimeError(
            '{} is not configured, do so with `$ ixmp-config -h`'.format(key))

    return data[key]


def config(db_config_path=None, default_dbprops_file=None):
    """Update configuration file with new values"""
    config = {}

    if db_config_path:
        db_config_path = os.path.abspath(os.path.expanduser(db_config_path))
        config['DB_CONFIG_PATH'] = db_config_path

    if default_dbprops_file:
        default_dbprops_file = os.path.abspath(
            os.path.expanduser(default_dbprops_file))
        config['DEFAULT_DBPROPS_FILE'] = default_dbprops_file

    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, mode='r') as f:
            data = json.load(f)
        data.update(config)
        config = data

    if config:
        dirname = os.path.dirname(CONFIG_PATH)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(CONFIG_PATH, mode='w') as f:
            logger().info('Updating configuration file: {}'.format(CONFIG_PATH))
            json.dump(config, f)
