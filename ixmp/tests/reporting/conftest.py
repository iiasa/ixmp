import pint
import pytest


@pytest.fixture(scope="session")
def ureg():
    """Application-wide units registry."""
    registry = pint.get_application_registry()

    # Used by .compat.ixmp, .compat.pyam
    registry.define("USD = [USD]")
    registry.define("case = [case]")

    yield registry
