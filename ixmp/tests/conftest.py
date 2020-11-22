"""Test configuration."""
from pathlib import Path

import pytest

# Fixtures


@pytest.fixture(scope="session")
def test_data_path():
    """Path to the directory containing test data."""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def tutorial_path():
    """Path to the directory containing the tutorials."""
    return Path(__file__).parents[2] / "tutorial"
