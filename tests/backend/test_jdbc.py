import ixmp
import jpype
import pytest


def test_jvm_warn(recwarn):
    """Test that no warnings are issued on JVM start-up.

    A warning message is emitted e.g. for JPype 0.7 if the 'convertStrings'
    kwarg is not provided to jpype.startJVM.

    NB this function should be in test_core.py, but because pytest executes
    tests in file, then code order, it must be before the call to ix.Platform()
    below.
    """

    # Start the JVM for the first time in the test session
    from ixmp.backend.jdbc import start_jvm
    start_jvm()

    if jpype.__version__ > '0.7':
        # Zero warnings were recorded
        assert len(recwarn) == 0, recwarn.pop().message


def test_deprecated(tmp_env):
    """Deprecated semantics for JDBCBackend."""
    msg = r"'dbtype' argument to JDBCBackend; use 'driver'"
    with pytest.warns(DeprecationWarning, match=msg):
        ixmp.Platform(dbtype='HSQLDB')

    # Initializing with an invalid dbtype
    pytest.raises(ValueError, ixmp.Platform, dbtype='foo')
