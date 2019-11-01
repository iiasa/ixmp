import pytest

from ixmp._config import Config


@pytest.fixture
def cfg():
    """Return a :class:`ixmp.config.Config` object that doesn't read a file."""
    yield Config(read=False)


def test_locate_nonexistent(cfg):
    with pytest.raises(FileNotFoundError):
        cfg._locate('nonexistent')


def test_config_default(cfg):
    # Default platform is 'local'
    assert cfg.values['platform']['default'] == 'local'

    # Set another platform as the default
    cfg.add_platform('dummy', 'jdbc', 'oracle', 'url', 'user', 'password')
    cfg.add_platform('default', 'dummy')

    # Now the default platform is 'dummy'
    assert cfg.values['platform']['default'] == 'dummy'
