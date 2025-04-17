from typing import Any, Literal, cast

import pandas as pd
import pytest

from ixmp import Scenario
from ixmp.testing import min_ixmp4_version

pytestmark = min_ixmp4_version


def test__ensure_filters_values_are_lists() -> None:
    from ixmp.backend.ixmp4 import _ensure_filters_values_are_lists

    filters = {"foo": [1, 2], "bar": 3}
    expected = {"foo": [1, 2], "bar": [3]}
    _ensure_filters_values_are_lists(filters=filters)
    assert filters == expected


def test__align_dtypes_for_filters() -> None:
    from ixmp.backend.ixmp4 import _align_dtypes_for_filters

    # This leads to dtypes 'int64' and 'object'
    df = pd.DataFrame({"foo": [1, 2, 3], "bar": ["baz", "foo", "bar"]})
    # Filters' types are determined based on first item assuming one type per key
    filters: dict[str, list[Any]] = {"foo": [1.0, 2.0, 3.0], "bar": [1, 2, 3]}
    expected = {"foo": [1, 2, 3], "bar": ["1", "2", "3"]}
    _align_dtypes_for_filters(filters=filters, data=df)

    assert filters == expected


# Overriding fixture that usually parametrizes test_mp
@pytest.fixture(scope="module")
def backend() -> Literal["ixmp4"]:
    return "ixmp4"


# NOTE This marker usually parametrizes test creation, but with overriding `backend`,
# this might not be necessary. Keeping it for clarity.
@pytest.mark.ixmp4
class TestIxmp4Functions:
    """Test group for all functions touching ixmp4 directly."""

    # NOTE Not writing a test for _index_and_set_attrs(). This is a helper function
    # that's also used by JDBC, and is always called for Backend.init() and
    # Backend.get(), so is tested already.

    def test__get_repo(self, ixmp4_backend, scenario: Scenario) -> None:
        from ixmp4.core.optimization.equation import EquationRepository
        from ixmp4.core.optimization.indexset import IndexSetRepository
        from ixmp4.core.optimization.parameter import ParameterRepository
        from ixmp4.core.optimization.scalar import ScalarRepository
        from ixmp4.core.optimization.table import TableRepository
        from ixmp4.core.optimization.variable import VariableRepository

        repos: dict[Literal["indexset", "scalar", "set", "par", "equ", "var"], Any] = {
            "indexset": IndexSetRepository,
            "scalar": ScalarRepository,
            "set": TableRepository,
            "par": ParameterRepository,
            "equ": EquationRepository,
            "var": VariableRepository,
        }

        # Test correct kind of instance is returned
        for type, expected_repo in repos.items():
            repo = ixmp4_backend._get_repo(s=scenario, type=type)
            assert isinstance(repo, expected_repo)

    def test__find_item(self, ixmp4_backend, scenario: Scenario) -> None:
        # Test unknown item name raises
        with pytest.raises(KeyError, match="No item called 'NotFound' found"):
            ixmp4_backend._find_item(s=scenario, name="NotFound")

        # Test finding an item and its type
        name = "foo"
        _type: Literal["indexset"] = "indexset"
        scenario.init_set(name=name)
        return_type, return_item = ixmp4_backend._find_item(
            s=scenario, name=name, types=tuple([_type])
        )
        assert (_type, name) == (return_type, return_item.name)

    def test__get_item(self, ixmp4_backend, scenario: Scenario) -> None:
        # Test getting an item
        name = "foo"
        _type: Literal["indexset"] = "indexset"
        scenario.init_set(name=name)
        indexset = ixmp4_backend._get_item(s=scenario, name=name, type=_type)
        assert indexset.name == name

    def test__get_indexset_or_table(self, ixmp4_backend, scenario: Scenario) -> None:
        # Test getting an item of "unknown" type (either 'indexset' or 'table');
        # getting a Table first tests getting an IndexSet, too
        indexset_name = "foo"
        scenario.init_set(name=indexset_name)
        table_name = "bar"
        scenario.init_set(name=table_name, idx_sets=[indexset_name])
        table = ixmp4_backend._get_indexset_or_table(s=scenario, name=table_name)
        assert table.name == table_name

    def test__add_data_to_set(self, ixmp4_backend, scenario: Scenario) -> None:
        # Test adding to an Indexset and warning about `comment`
        indexset_name = "Indexset"
        scenario.init_set(name=indexset_name)
        key = "foo"
        ixmp4_backend._add_data_to_set(
            s=scenario, name=indexset_name, key=key, comment="Test comment"
        )
        indexset_data = scenario.set(indexset_name)
        assert isinstance(indexset_data, pd.Series)
        pd.testing.assert_series_equal(indexset_data, pd.Series([key]))

        # Test adding to a Table
        table_name = "Table"
        scenario.init_set(name=table_name, idx_sets=[indexset_name])
        ixmp4_backend._add_data_to_set(s=scenario, name=table_name, key=[key])
        # We can assume this data type for Tables
        pd.testing.assert_frame_equal(
            cast(pd.DataFrame, scenario.set(table_name)),
            pd.DataFrame({indexset_name: [key]}),
        )

    def test__create_scalar(self, ixmp4_backend, scenario: Scenario) -> None:
        # Test creating a scalar with a comment
        name = "Scalar"
        value = 3.141592653589793238462643383279502884197169399375105820
        comment = "Comment"
        ixmp4_backend._create_scalar(
            s=scenario, name=name, value=value, unit=None, comment=comment
        )
        # NOTE We don't really have a way to retrieve Scalars from IXMP4Backend
        scalar = ixmp4_backend.index[scenario].optimization.scalars.get(name=name)
        assert scalar.value == value
        assert scalar.docs == comment

    def test__add_data_to_parameter(self, ixmp4_backend, scenario: Scenario) -> None:
        # Set up auxiliary items
        unit_name = "Unit"
        ixmp4_backend.set_unit(name=unit_name, comment="Create test unit")
        indexset_name = "Indexset"
        scenario.init_set(name=indexset_name)
        key = "foo"
        scenario.add_set(name=indexset_name, key=key)

        # Test creating a parameter with a comment and key conversion to list[str]
        name = "Parameter"
        value = 3.141592653589793238462643383279502884197169399375105820
        comment = "Comment"
        scenario.init_par(name=name, idx_sets=[indexset_name])
        ixmp4_backend._add_data_to_parameter(
            s=scenario, name=name, key=key, value=value, unit=unit_name, comment=comment
        )
        parameter = ixmp4_backend.index[scenario].optimization.parameters.get(name=name)
        assert parameter.data == {
            indexset_name: [key],
            "values": [value],
            "units": [unit_name],
        }

    def test__get_set_data(self, ixmp4_backend, scenario: Scenario) -> None:
        indexset_name = "Indexset"
        scenario.init_set(name=indexset_name)
        table_name = "Table"
        scenario.init_set(name=table_name, idx_sets=[indexset_name])

        # Test getting empty data (other cases tested in integration tests)
        indexset_data = ixmp4_backend._get_set_data(s=scenario, name=indexset_name)
        # We can assume this return type for Indexsets
        pd.testing.assert_series_equal(cast(pd.Series, indexset_data), pd.Series([]))
        table_data = ixmp4_backend._get_set_data(s=scenario, name=table_name)
        # We can assume this return type for Tables
        pd.testing.assert_frame_equal(
            cast(pd.DataFrame, table_data), pd.DataFrame(columns=[indexset_name])
        )
