import jpype
import pytest
from pytest import raises

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
    sample_props = test_data_path / 'test.properties.sample'
    ixmp.Platform(dbprops=sample_props)
    assert caplog.records[-1].message == \
        'launching ixmp.Platform connected to jdbc:hsqldb:mem://ixmptest'


def test_read_file(test_mp, tmp_path):
    be = test_mp._backend

    with pytest.raises(NotImplementedError):
        be.read_file(tmp_path / 'test.csv', ixmp.ItemType.ALL, filters={})


def test_write_file(test_mp, tmp_path):
    be = test_mp._backend

    with pytest.raises(NotImplementedError):
        be.write_file(tmp_path / 'test.csv', ixmp.ItemType.ALL, filters={})


# This variable formerly had 'warns' as the third element in some tuples, to
# test for deprecation warnings.
INIT_PARAMS = (
    # Handled in JDBCBackend:
    (['nonexistent.properties'], dict(), raises, ValueError, "platform name "
     r"'nonexistent.properties' not among \['default'"),
    (['nonexistent.properties'], dict(name='default'), raises, TypeError,
     None),
    # Using the dbtype keyword argument
    ([], dict(dbtype='HSQLDB'), raises, TypeError,
     r"JDBCBackend\(\) got an unexpected keyword argument 'dbtype'"),
    # Initialize with driver='oracle' and path
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


@pytest.mark.parametrize('args,kwargs,action,kind,match', INIT_PARAMS)
def test_init(tmp_env, args, kwargs, action, kind, match):
    """Semantics for JDBCBackend.__init__()."""
    with action(kind, match=match):
        ixmp.Platform(*args, **kwargs)


def test_gh_216(test_mp):
    scen = make_dantzig(test_mp)

    filters = dict(i=['seattle', 'beijing'])

    # Java code in ixmp_source would raise an exception because 'beijing' is
    # not in set i; but JDBCBackend removes 'beijing' from the filters before
    # calling the underlying method (https://github.com/iiasa/ixmp/issues/216)
    scen.par('a', filters=filters)