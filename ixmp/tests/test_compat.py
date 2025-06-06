"""Tests for compatibility with other packages, especially :mod:`message_ix`."""

import os
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from ixmp import config

if TYPE_CHECKING:
    from ixmp.testing import Runner


@pytest.fixture()
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Generator[Path, Any, None]:
    p0 = tmp_path_factory.mktemp("foo")
    p1 = p0.joinpath("bar", "baz")
    p1.mkdir(exist_ok=True, parents=True)
    yield p1


def test_message_model_dir(
    ixmp_cli: "Runner", tmp_env: os._Environ[str], tmp_path: Path
) -> None:
    """Configuration keys like 'message model dir' can be registered, set, and used."""
    value = tmp_path

    key = "TEST message model dir"

    # Setting the key without first registering it results in an error
    result = ixmp_cli.invoke(["config", "set", key, str(value)])
    assert 1 == result.exit_code
    assert (
        f"Error: No registered configuration key {repr(key)}" == result.output.strip()
    )

    # Register the key
    config.register(key, Path, Path(__file__).parent / "model")

    # The key can be set
    result = ixmp_cli.invoke(["config", "set", key, str(value)])
    assert 0 == result.exit_code

    # Force re-read of the key from file
    config.read()

    # Configuration value has the expected type
    assert isinstance(config.get(key), Path)
    # Expected value
    assert value == config.get(key)

    # Configuration
    result = ixmp_cli.invoke(["config", "get", key])
    assert str(value) == result.output.strip()

    # Clean for remainder of tests
    config.unregister(key)
    assert key not in config.keys()
