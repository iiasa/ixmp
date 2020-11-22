from pathlib import Path

import pytest

from ixmp._config import KEYS, Config, _JSONEncoder, _locate


@pytest.fixture
def cfg():
    """Return a :class:`ixmp._config.Config` object without reading a file."""
    yield Config(read=False)


def test_encoder():
    # Custom encoder properly raises TypeError for unhandled value
    with pytest.raises(TypeError):
        _JSONEncoder().encode({"foo": range(10)})


def test_locate(cfg):
    try:
        # The result of this test depends on the user's environment. If
        # $HOME/.local/share/ixmp exists, the call will succeed; otherwise,
        # it will fail.
        _locate()
    except FileNotFoundError:
        pass

    with pytest.raises(FileNotFoundError):
        _locate("nonexistent")


def test_set_get(cfg):
    # ixmp has no string keys by default, so we insert a fake one
    KEYS["test key"] = (str, None)
    cfg.values["test key"] = "foo"

    # get() works
    assert cfg.get("test key") == "foo"

    # set() changes the value
    cfg.set("test key", "bar")
    assert cfg.get("test key") == "bar"

    # set() with None makes no change
    cfg.set("test key", None)
    assert cfg.get("test key") == "bar"

    # set() with invalid type raises exception
    KEYS["test key"] = (int, None)
    with pytest.raises(TypeError):
        cfg.set("test key", "foo")


def test_register(cfg):
    # New key can be registered
    cfg.register("new key", int, 42)

    # Default value is set on the instance
    assert cfg.get("new key") == 42

    # Can't re-register an existing key
    with pytest.raises(KeyError):
        cfg.register("new key", int, 43)

    # Register a key with type Path
    cfg.register("new key 2", Path)

    # The key can be with a string value, automatically converted to Path
    cfg.set("new key 2", "/foo/bar/baz")
    assert isinstance(cfg.get("new key 2"), Path)


def test_config_platform(cfg):
    # Default platform is 'local'
    assert cfg.values["platform"]["default"] == "local"

    # Set another platform as the default
    cfg.add_platform("dummy", "jdbc", "oracle", "url", "user", "password")
    cfg.add_platform("default", "dummy")

    # Now the default platform is 'dummy'
    assert cfg.values["platform"]["default"] == "dummy"

    # Same info is retrieved for 'default' and the actual name
    assert cfg.get_platform_info("default") == cfg.get_platform_info("dummy")

    # Invalid calls
    with pytest.raises(ValueError):
        cfg.add_platform("invalid", "notabackend", "other", "args")

    with pytest.raises(ValueError):
        cfg.get_platform_info("nonexistent")
