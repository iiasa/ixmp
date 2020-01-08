import jpype
import pytest
from pytest import raises, warns
import ixmp
from ixmp.testing import make_dantzig


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


def test_close(test_mp, caplog):
    """Platform.close_db() doesn't throw needless exceptions."""
    # Close once
    test_mp.close_db()

    # Close again, once already closed
    test_mp.close_db()
    assert caplog.records[0].message == \
        'Database connection could not be closed or was already closed'


def test_pass_properties():
    ixmp.Platform(driver='hsqldb', url='jdbc:hsqldb:mem://ixmptest',
                  user='ixmp', password='ixmp')


def test_invalid_properties_file(test_data_path):
    # HyperSQL creates a file with a .properties suffix for every file-based
    # database, but these files do not contain the information needed to
    # instantiate a database connection
    with pytest.raises(ValueError,
                       match='Config file contains no database URL'):
        ixmp.Platform(dbprops=test_data_path / 'hsqldb.properties')


def test_connect_message(caplog, test_data_path):
    sample_props = test_data_path / 'testdb' / 'test.properties.sample'
    ixmp.Platform(dbprops=sample_props)
    assert caplog.records[-1].message == \
        'launching ixmp.Platform connected to jdbc:hsqldb:mem://ixmptest'


DEPRECATED = (
    # Handled in Platform:
    # Positional arguments that clash raise an error
    (['nonexistent.properties'], dict(backend='foo'), raises, ValueError,
     "backend='foo' conflicts with deprecated positional arguments for "
     "JDBCBackend"),

    # Handled in JDBCBackend:
    (['nonexistent.properties'], dict(), raises, FileNotFoundError, None),
    ([], dict(dbtype='HSQLDB'), warns, DeprecationWarning,
     r"'dbtype' argument to JDBCBackend; use 'driver'"),
    # Initialize with an invalid dbtype
    ([], dict(dbtype='foo'), raises, ValueError, None),
    # …with driver='oracle' and path
    ([], dict(backend='jdbc', driver='oracle', path='foo/bar'), raises,
     ValueError, None),
    # …with driver='oracle' and no url
    ([], dict(backend='jdbc', driver='oracle'), raises, ValueError, None),
    # …with driver='hsqldb' and no path
    ([], dict(backend='jdbc', driver='hsqldb'), raises, ValueError, None),
    # …with driver='hsqldb' and url
    ([], dict(backend='jdbc', driver='hsqldb', url='example.com:1234:SCHEMA'),
     raises, ValueError, None),
)


@pytest.mark.parametrize('args,kwargs,action,kind,match', DEPRECATED)
def test_deprecated(tmp_env, args, kwargs, action, kind, match):
    """Deprecated semantics for JDBCBackend."""
    with action(kind, match=match):
        ixmp.Platform(*args, **kwargs)


def test_deprecated_warns(tmp_env):
    # Both warns AND raises
    with pytest.raises(FileNotFoundError):
        with pytest.warns(DeprecationWarning, match="positional arguments to "
                          "Platform(…) for JDBCBackend"):
            ixmp.Platform('nonexistent.properties', name='default')


def test_gh_216(test_mp):
    scen = make_dantzig(test_mp)

    filters = dict(i=['seattle', 'beijing'])

    # Java code in ixmp_source would raise an exception because 'beijing' is
    # not in set i; but JDBCBackend removes 'beijing' from the filters before
    # calling the underlying method (https://github.com/iiasa/ixmp/issues/216)
    scen.par('a', filters=filters)
