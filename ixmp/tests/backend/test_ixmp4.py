from typing import Any, Literal, cast

import pandas as pd
import pytest
from ixmp4.core.optimization.equation import EquationRepository
from ixmp4.core.optimization.indexset import IndexSetRepository
from ixmp4.core.optimization.parameter import ParameterRepository
from ixmp4.core.optimization.scalar import ScalarRepository
from ixmp4.core.optimization.table import TableRepository
from ixmp4.core.optimization.variable import VariableRepository

from ixmp import Platform, Scenario
from ixmp.backend.ixmp4 import (
    IXMP4Backend,
    _align_dtypes_for_filters,
    _ensure_filters_values_are_lists,
)

# NOTE Not writing a test for _index_and_set_attrs(). This is a helper function that's
# also used by JDBC, and is always called for Backend.init() and Backend.get(), so is
# tested already.
# @pytest.mark.ixmp4
# def test__index_and_set_attrs(test_mp: Platform) -> None:

pytestmark = pytest.mark.min_ixmp4_version


@pytest.mark.ixmp4
def test__get_repo(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")
    repos: dict[Literal["indexset", "scalar", "set", "par", "equ", "var"], Any] = {
        "indexset": IndexSetRepository,
        "scalar": ScalarRepository,
        "set": TableRepository,
        "par": ParameterRepository,
        "equ": EquationRepository,
        "var": VariableRepository,
    }

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Test correct kind of instance is returned
    for type, expected_repo in repos.items():
        repo = backend._get_repo(s=scenario, type=type)
        assert isinstance(repo, expected_repo)


@pytest.mark.ixmp4
def test__find_item(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Test unknown item name raises
    with pytest.raises(KeyError, match="No item called 'NotFound' found"):
        backend._find_item(s=scenario, name="NotFound")

    # Test finding an item and its type
    name = "foo"
    _type: Literal["indexset"] = "indexset"
    scenario.init_set(name=name)
    return_type, return_item = backend._find_item(
        s=scenario, name=name, types=tuple([_type])
    )
    assert (_type, name) == (return_type, return_item.name)


@pytest.mark.ixmp4
def test__get_item(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Test getting an item
    name = "foo"
    _type: Literal["indexset"] = "indexset"
    scenario.init_set(name=name)
    indexset = backend._get_item(s=scenario, name=name, type=_type)
    assert indexset.name == name


@pytest.mark.ixmp4
def test__get_indexset_or_table(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Test getting an item of "unknown" type (either 'indexset' or 'table');
    # getting a Table first tests getting an IndexSet, too
    indexset_name = "foo"
    scenario.init_set(name=indexset_name)
    table_name = "bar"
    scenario.init_set(name=table_name, idx_sets=[indexset_name])
    table = backend._get_indexset_or_table(s=scenario, name=table_name)
    assert table.name == table_name


@pytest.mark.ixmp4
def test__add_data_to_set(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Test adding to an Indexset and warning about `comment`
    indexset_name = "Indexset"
    scenario.init_set(name=indexset_name)
    key = "foo"
    backend._add_data_to_set(
        s=scenario, name=indexset_name, key=key, comment="Test comment"
    )
    indexset_data = scenario.set(indexset_name)
    assert isinstance(indexset_data, pd.Series)
    pd.testing.assert_series_equal(indexset_data, pd.Series([key]))

    # Test adding to a Table
    table_name = "Table"
    scenario.init_set(name=table_name, idx_sets=[indexset_name])
    backend._add_data_to_set(s=scenario, name=table_name, key=[key])
    # We can assume this data type for Tables
    pd.testing.assert_frame_equal(
        cast(pd.DataFrame, scenario.set(table_name)),
        pd.DataFrame({indexset_name: [key]}),
    )


@pytest.mark.ixmp4
def test__create_scalar(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Test creating a scalar with a comment
    name = "Scalar"
    value = 3.141592653589793238462643383279502884197169399375105820
    comment = "Comment"
    backend._create_scalar(
        s=scenario, name=name, value=value, unit=None, comment=comment
    )
    # NOTE We don't really have a way to retrieve Scalars from IXMP4Backend right now
    scalar = backend.index[scenario].optimization.scalars.get(name=name)
    assert scalar.value == value
    assert scalar.docs == comment


@pytest.mark.ixmp4
def test__add_data_to_parameter(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    # Set up auxiliary items
    unit_name = "Unit"
    test_mp.add_unit(unit=unit_name)
    indexset_name = "Indexset"
    scenario.init_set(name=indexset_name)
    key = "foo"
    scenario.add_set(name=indexset_name, key=key)

    # Test creating a parameter with a comment and key conversion to list[str]
    name = "Parameter"
    value = 3.141592653589793238462643383279502884197169399375105820
    comment = "Comment"
    scenario.init_par(name=name, idx_sets=[indexset_name])
    backend._add_data_to_parameter(
        s=scenario, name=name, key=key, value=value, unit=unit_name, comment=comment
    )
    parameter = backend.index[scenario].optimization.parameters.get(name=name)
    assert parameter.data == {
        indexset_name: [key],
        "values": [value],
        "units": [unit_name],
    }


@pytest.mark.ixmp4
def test__get_set_data(test_mp: Platform) -> None:
    scenario = Scenario(test_mp, "model", "scenario", version="new")

    # Ensure we're handling the correct type of backend
    backend = test_mp._backend
    assert isinstance(backend, IXMP4Backend)

    indexset_name = "Indexset"
    scenario.init_set(name=indexset_name)
    table_name = "Table"
    scenario.init_set(name=table_name, idx_sets=[indexset_name])

    # Test getting empty data (other cases tested in integration tests)
    indexset_data = backend._get_set_data(s=scenario, name=indexset_name)
    # We can assume this return type for Indexsets
    pd.testing.assert_series_equal(cast(pd.Series, indexset_data), pd.Series([]))
    table_data = backend._get_set_data(s=scenario, name=table_name)
    # We can assume this return type for Tables
    pd.testing.assert_frame_equal(
        cast(pd.DataFrame, table_data), pd.DataFrame(columns=[indexset_name])
    )


def test__ensure_filters_values_are_lists() -> None:
    filters = {"foo": [1, 2], "bar": 3}
    expected = {"foo": [1, 2], "bar": [3]}
    _ensure_filters_values_are_lists(filters=filters)
    assert filters == expected


def test__align_dtypes_for_filters() -> None:
    # This leads to dtypes 'int64' and 'object'
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ["baz", "foo", "bar"]})
    # Filters' types are determined based on first item assuming one type per key
    filters: dict[str, list[Any]] = {"foo": [1.0, 2.0, 3.0], "bar": [1, 2, 3]}
    expected = {"foo": [1, 2, 3], "bar": ["1", "2", "3"]}
    _align_dtypes_for_filters(filters=filters, data=df)

    assert filters == expected
