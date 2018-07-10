import os

# path to local files
LOCAL_PATH = os.path.expanduser(os.path.join('~', '.local', 'ixmp'))
LOCAL_PATH = LOCAL_PATH.replace('Documents' + os.path.sep, '') # fix for R users

# path to local configuration values
CONFIG_PATH = os.path.join(LOCAL_PATH, 'config.json')

# path to local database instance
DEFAULT_LOCAL_DB_PATH = os.path.join(LOCAL_PATH, 'localdb', 'default')
