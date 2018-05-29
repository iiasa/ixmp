import os

from ixmp import config


def default_dbprops_file():
    config.get('DEFAULT_DBPROPS_FILE')


def db_config_path():
    config.get('DB_CONFIG_PATH')


def find_dbprops(fname):
    """Search directories for file fname. First start in local dir (`.`), then look
    in ixmp default locations.

    Parameters
    ----------
    fname : string
        filename
    """
    # look local first
    if os.path.isfile(fname):
        return fname

    # otherwise look in default directory
    config_path = db_config_path()
    _fname = os.path.join(config_path, fname)
    if not os.path.isfile(_fname):
        raise IOError('Could not find {} either locally or in {}'.format(
            fname, config_path))
    return _fname
