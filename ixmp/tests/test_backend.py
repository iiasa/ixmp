"""Common tests of all backends."""

import pandas as pd
import pandas.testing as pdt
import pytest

import ixmp.core.scenario
from ixmp import Platform, Scenario
from ixmp.util.ixmp4 import is_ixmp4backend

TECH = ["t1", "t2", "t3", "t4"]


@pytest.fixture(scope="function")
def scenario(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest, test_mp: Platform
) -> Scenario:
    """A mock 'MESSAGE'-scheme scenario on `test_mp`."""
    monkeypatch.setattr(ixmp.core.scenario, "_TESTING", True)
    s = Scenario(
        test_mp,
        scheme="MESSAGE",
        model="test_cat_set_elements",
        scenario=request.node.name,
        version="new",
    )

    if is_ixmp4backend(s.platform._backend):
        # Add a 1D set "addon" indexed by "technology"
        s.init_set("addon", idx_sets=["technology"])

    # Add some technologies for use in tests
    s.add_set("technology", TECH)

    return s


def test_cat_set_elements(scenario: Scenario) -> None:
    """Basic behaviour of :meth:`.base.Backend.cat_set_elements` works."""
    # Retrieve a reference to the backend
    backend = scenario.platform._backend
    name, cat = "technology", "FOO"

    # Succeeds with a new type_tec ("FOO") and multiple elements
    backend.cat_set_elements(
        ms=scenario, name=name, cat=cat, keys=TECH[:3], is_unique=False
    )

    # type_tec can be retrieved and has the expected elements
    type_tec = scenario.set("type_tec")
    assert isinstance(type_tec, pd.Series)
    assert {"all", cat} == set(type_tec.values)

    # cat_tec can be retrieved and maps the new type_tec to its members
    cat_tec = scenario.set("cat_tec")
    assert isinstance(cat_tec, pd.DataFrame)
    pdt.assert_frame_equal(
        cat_tec.sort_index(axis=1), pd.DataFrame({name: TECH[:3], "type_tec": cat})
    )


def test_cat_set_elements_is_unique(scenario: Scenario) -> None:
    """Test :py:`is_unique=...` argument to :meth:`.cat_set_elements`."""
    backend = scenario.platform._backend
    name, cat = "technology", "BAR"

    # Raises with is_unique=True and multiple keys
    with pytest.raises(ValueError, match="Can only add one element"):
        backend.cat_set_elements(
            ms=scenario, name=name, cat=cat, keys=TECH[:2], is_unique=True
        )

    # Succeeds with is_unique=True and a single item
    # This also tests replacement of one unique item with another
    for value in (
        TECH[1:2],  # list of length 1
        TECH[1],  # str
    ):
        backend.cat_set_elements(
            ms=scenario, name=name, cat=cat, keys=value, is_unique=True
        )

        # type_tec can be retrieved and has the expected elements
        type_tec = scenario.set("type_tec")
        assert isinstance(type_tec, pd.Series)
        assert {"all", cat} == set(type_tec.values)

        # cat_tec can be retrieved and has the expected elements
        cat_tec = scenario.set("cat_tec")
        assert isinstance(cat_tec, pd.DataFrame)
        assert TECH[1] == cat_tec.query("type_tec == @cat").technology.item()


def test_cat_set_elements_addon(scenario: Scenario) -> None:
    """:meth:`.base.Backend.cat_set_elements` works for the special name "addon"."""
    backend = scenario.platform._backend
    name, cat = "addon", "BAZ"

    # Call succeeds
    backend.cat_set_elements(
        ms=scenario, name=name, cat=cat, keys=TECH[1:3], is_unique=False
    )

    # The cat_addon data frame can be retrieved
    cat_addon = scenario.set("cat_addon")
    assert isinstance(cat_addon, pd.DataFrame)

    # Expected column names; notice these are different from e.g. ["type_foo", "foo"]
    # in ordinary cases
    assert {"technology_addon", "type_addon"} == set(cat_addon.columns)

    # Values are as expected
    pdt.assert_frame_equal(
        cat_addon.sort_index(axis=1),
        pd.DataFrame({"technology_addon": TECH[1:3], "type_addon": cat}),
    )
