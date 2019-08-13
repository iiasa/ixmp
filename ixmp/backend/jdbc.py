import os
from pathlib import Path

import jpype
from jpype import (
    JClass,
    JPackage as java,
)
import numpy as np

from ixmp.config import _config
from ixmp.utils import logger
from ixmp.backend.base import Backend


# Map of Python to Java log levels
LOG_LEVELS = {
    'CRITICAL': 'ALL',
    'ERROR': 'ERROR',
    'WARNING': 'WARN',
    'INFO': 'INFO',
    'DEBUG': 'DEBUG',
    'NOTSET': 'OFF',
}


class JDBCBackend(Backend):
    """Backend using JDBC to connect to Oracle and HSQLDB instances.

    Much of the code of this backend is implemented in Java, in the
    ixmp_source repository.
    """
    #: Reference to the at.ac.iiasa.ixmp.Platform Java object
    jobj = None

    #: Mapping from ixmp.TimeSeries object to the underlying
    #: at.ac.iiasa.ixmp.Scenario object (or subclasses of either)
    jindex = {}

    def __init__(self, dbprops=None, dbtype=None, jvmargs=None):
        start_jvm(jvmargs)
        self.dbtype = dbtype

        try:
            # if no dbtype is specified, launch Platform with properties file
            if dbtype is None:
                dbprops = _config.find_dbprops(dbprops)
                if dbprops is None:
                    raise ValueError("Not found database properties file "
                                     "to launch platform")
                logger().info("launching ixmp.Platform using config file at "
                              "'{}'".format(dbprops))
                self.jobj = java.ixmp.Platform("Python", str(dbprops))
            # if dbtype is specified, launch Platform with local database
            elif dbtype == 'HSQLDB':
                dbprops = dbprops or _config.get('DEFAULT_LOCAL_DB_PATH')
                logger().info("launching ixmp.Platform with local {} database "
                              "at '{}'".format(dbtype, dbprops))
                self.jobj = java.ixmp.Platform("Python", str(dbprops), dbtype)
            else:
                raise ValueError('Unknown dbtype: {}'.format(dbtype))
        except TypeError:
            msg = ("Could not launch the JVM for the ixmp.Platform."
                   "Make sure that all dependencies of ixmp.jar"
                   "are included in the 'ixmp/lib' folder.")
            logger().info(msg)
            raise

    # Platform methods

    def set_log_level(self, level):
        self.jobj.setLogLevel(LOG_LEVELS[level])

    def open_db(self):
        """(Re-)open the database connection."""
        self.jobj.openDB()

    def close_db(self):
        """Close the database connection.

        A HSQL database can only be used by one :class:`Backend` instance at a
        time. Any existing connection must be closed before a new one can be
        opened.
        """
        self.jobj.closeDB()

    def units(self):
        """Return all units described in the database."""
        return to_pylist(self.jobj.getUnitList())

    # Timeseries methods
    def ts_init(self, ts, annotation=None):
        """Initialize the ixmp.TimeSeries *ts*."""
        if ts.version == 'new':
            # Create a new TimeSeries
            jobj = self.jobj.newTimeSeries(ts.model, ts.scenario, annotation)
        elif isinstance(ts.version, int):
            # Load a TimeSeries of specific version
            jobj = self.jobj.getTimeSeries(ts.model, ts.scenario, ts.version)
        else:
            # Load the latest version of a TimeSeries
            jobj = self.jobj.getTimeSeries(ts.model, ts.scenario)

            # Update the version attribute
            ts.version = jobj.getVersion()

        # Add to index
        self.jindex[ts] = jobj

    def ts_discard_changes(self, ts):
        """Discard all changes and reload from the database."""
        self.jindex[ts].discardChanges()

    def ts_set_as_default(self, ts):
        """Set the current :attr:`version` as the default."""
        self.jindex[ts].setAsDefaultVersion()

    def ts_is_default(self, ts):
        """Return :obj:`True` if the :attr:`version` is the default version."""
        return bool(self.jindex[ts].isDefault())

    def ts_last_update(self, ts):
        """get the timestamp of the last update/edit of this TimeSeries"""
        return self.jindex[ts].getLastUpdateTimestamp().toString()

    def ts_run_id(self, ts):
        """get the run id of this TimeSeries"""
        return self.jindex[ts].getRunId()

    def ts_preload(self, ts):
        """Preload timeseries data to in-memory cache. Useful for bulk updates.
        """
        self.jindex[ts].preloadAllTimeseries()

    # Scenario methods
    def s_init(self, s, scheme=None, annotation=None):
        """Initialize the ixmp.Scenario *s*."""
        if s.version == 'new':
            jobj = self.jobj.newScenario(s.model, s.scenario, scheme,
                                         annotation)
        elif isinstance(s.version, int):
            jobj = self.jobj.getScenario(s.model, s.scenario, s.version)
        # constructor for `message_ix.Scenario.__init__` or `clone()` function
        elif isinstance(s.version,
                        JClass('at.ac.iiasa.ixmp.objects.Scenario')):
            jobj = s.version
        elif s.version is None:
            jobj = self.jobj.getScenario(s.model, s.scenario)
        else:
            raise ValueError('Invalid `version` arg: `{}`'.format(s.version))

        s.version = jobj.getVersion()
        s.scheme = jobj.getScheme()

        # Add to index
        self.jindex[s] = jobj




def start_jvm(jvmargs=None):
    """Start the Java Virtual Machine via JPype.

    Parameters
    ----------
    jvmargs : str or list of str, optional
        Additional arguments to pass to :meth:`jpype.startJVM`.
    """
    # TODO change the jvmargs default to [] instead of None
    if jpype.isJVMStarted():
        return

    jvmargs = jvmargs or []

    # Arguments
    args = [jpype.getDefaultJVMPath()]

    # Add the ixmp root directory, ixmp.jar and bundled .jar and .dll files to
    # the classpath
    module_root = Path(__file__).parents[1]
    jarfile = module_root / 'ixmp.jar'
    module_jars = list(module_root.glob('lib/*'))
    classpath = map(str, [module_root, jarfile] + list(module_jars))

    sep = ';' if os.name == 'nt' else ':'
    args.append('-Djava.class.path={}'.format(sep.join(classpath)))

    # Add user args
    args.extend(jvmargs if isinstance(jvmargs, list) else [jvmargs])

    # For JPype 0.7 (raises a warning) and 0.8 (default is False).
    # 'True' causes Java string objects to be converted automatically to Python
    # str(), as expected by ixmp Python code.
    kwargs = dict(convertStrings=True)

    jpype.startJVM(*args, **kwargs)

    # define auxiliary references to Java classes
    java.ixmp = java('at.ac.iiasa.ixmp')
    java.Integer = java('java.lang').Integer
    java.Double = java('java.lang').Double
    java.LinkedList = java('java.util').LinkedList
    java.HashMap = java('java.util').HashMap
    java.LinkedHashMap = java('java.util').LinkedHashMap


def to_pylist(jlist):
    """Transforms a Java.Array or Java.List to a python list"""
    # handling string array
    try:
        return np.array(jlist[:])
    # handling Java LinkedLists
    except Exception:
        return np.array(jlist.toArray()[:])
