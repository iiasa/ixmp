import json
import os

from ixmp.utils import logger
from ixmp.default_paths import CONFIG_PATH


def get(key):
    if not os.path.exists(CONFIG_PATH):
        raise RuntimeError(
            'ixmp has not been configured, do so with `$ ixmp-config -h`')

    with open(CONFIG_PATH, mode='r') as f:
        data = json.load(f)

    if key not in data:
        raise RuntimeError(
            '{} is not configured, do so with `$ ixmp-config -h`')

    return data[key]


def config(db_config_dir=None, default_dbprops_file=None):
    config = {}

    if db_config_dir:
        db_config_dir = os.path.abspath(os.path.expanduser(db_config_dir))
        config['DB_CONFIG_DIR'] = db_config_dir

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
        with open(CONFIG_PATH, mode='w') as f:
            logger().info('Updating configuration file: {}'.format(CONFIG_PATH))
            json.dump(config, f)
