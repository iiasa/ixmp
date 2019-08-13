import os
from pathlib import Path

import jpype
from jpype import (
    JPackage as java,
)
import numpy as np

from ixmp.config import _config
from ixmp.utils import logger
from ixmp.backend.base import Backend


class JDBCBackend(Backend):
    """Backend using JDBC to connect to Oracle and HSQLDB instances.

    Much of the code of this backend is implemented in Java, in the
    ixmp_source repository.
    """
    #: Reference to the at.ac.iiasa.ixmp.Platform Java object
    jobj = None

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
