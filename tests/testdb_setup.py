# TODO this file is unused; move contents to other files or delete.
import os
import pandas as pd
import ixmp

from testing_utils import here

db_dir = os.path.join(here, 'testdb')
test_db = os.path.join(db_dir, 'ixmptest')

# %% remove existing database files

for fname in [
        os.path.join(db_dir, 'ixmptest.lobs'),
        os.path.join(db_dir, 'ixmptest.properties'),
        os.path.join(db_dir, 'ixmptest.script')
]:
    if os.path.isfile(fname):
        os.remove(fname)

# %% launch the modeling platform instance, creating a new test database file

mp = ixmp.Platform(dbprops=test_db, dbtype='HSQLDB')

# %% initialize a new timeseries instance

ts = ixmp.TimeSeries(mp, 'Douglas Adams', 'Hitchhiker',
                     version='new', annotation='testing')
df = {'region': ['World', 'World'], 'variable': ['Testing', 'Testing'],
      'unit': ['???', '???'], 'year': [2010, 2020], 'value': [23.7, 23.8]}
df = pd.DataFrame.from_dict(df)
ts.add_timeseries(df)
ts.commit('importing a testing timeseries')
ts.set_as_default()

mp.close_db()
