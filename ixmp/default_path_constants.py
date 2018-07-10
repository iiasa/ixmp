import os

# path to local files
LOCAL_PATH = os.path.expanduser(os.path.join('~', '.local', 'ixmp'))
# fix for R users
LOCAL_PATH = LOCAL_PATH.replace('Documents' + os.path.sep, '') 

# path to local configuration values
CONFIG_PATH = os.path.join(LOCAL_PATH, 'config.json')

# path to local database instance
DEFAULT_LOCAL_DB_PATH = os.path.join(LOCAL_PATH, 'localdb', 'default')
