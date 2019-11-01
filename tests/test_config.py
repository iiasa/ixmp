import pytest

from ixmp._config import Config


@pytest.fixture
def cfg():
    """Return a :class:`ixmp.config.Config` object that doesn't read a file."""
    yield Config(read=False)


def test_locate(cfg):
    try:
        # The result of this test depends on the user's environment. If
        # $HOME/.local/share/ixmp exists, the call will succeed; otherwise,
        # it will fail.
        cfg._locate()
    except FileNotFoundError:
        pass

    with pytest.raises(FileNotFoundError):
        cfg._locate('nonexistent')


def test_set_get(cfg):
    # ixmp has no string keys by default, so we insert a fake one
    cfg._keys['test key'] = str
    cfg.values['test key'] = 'foo'

    # get() works
    assert cfg.get('test key') == 'foo'

    # set() changes the value
    cfg.set('test key', 'bar')
    assert cfg.get('test key') == 'bar'

    # set() with None makes no change
    cfg.set('test key', None)
    assert cfg.get('test key') == 'bar'


def test_config_platform(cfg):
    # Default platform is 'local'
    assert cfg.values['platform']['default'] == 'local'

    # Set another platform as the default
    cfg.add_platform('dummy', 'jdbc', 'oracle', 'url', 'user', 'password')
    cfg.add_platform('default', 'dummy')

    # Now the default platform is 'dummy'
    assert cfg.values['platform']['default'] == 'dummy'

    # Same info is retrieved for 'default' and the actual name
    assert cfg.get_platform_info('default') == cfg.get_platform_info('dummy')

    # Invalid calls
    with pytest.raises(ValueError):
        cfg.add_platform('invalid', 'notabackend', 'other', 'args')

    with pytest.raises(ValueError):
        cfg.get_platform_info('nonexistent')
