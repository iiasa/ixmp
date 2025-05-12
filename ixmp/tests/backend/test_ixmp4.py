from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
import pytest

from ixmp import Scenario
from ixmp.backend.common import ItemType
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

    def test__ni(self, ixmp4_backend) -> None:
        with pytest.raises(NotImplementedError):
            ixmp4_backend._ni()

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

    # Test some edge cases for standard functions
    def test_handle_config(self, ixmp4_backend) -> None:
        # Test raising for unhandled positional args
        with pytest.raises(ValueError, match="Unhandled positional args"):
            ixmp4_backend.handle_config(["test arg"], {"foo": "bar"})

        # Test raising for missing required key
        with pytest.raises(ValueError, match="'ixmp4_name' keyword argument"):
            ixmp4_backend.handle_config([], {"foo": "bar"})

    def test_set_node(self, ixmp4_backend, caplog: pytest.LogCaptureFixture) -> None:
        from ixmp.backend.ixmp4 import log

        # Test logging warnings for unused/required parameters
        parent = "Parent"
        synonym = "Synonym"
        with caplog.at_level("WARNING", logger=log.name):
            ixmp4_backend.set_node(
                name="Region", parent=parent, hierarchy=None, synonym=synonym
            )

        expected = [
            f"Discarding parent parameter {parent}; unused in ixmp4.",
            f"Discarding synonym parameter {synonym}; unused in ixmp4.",
            "IXMP4Backend.set_node() requires to specify 'hierarchy'! "
            "Using 'None' as a (meaningless) default.",
        ]
        assert caplog.messages == expected

    def test_clone(
        self, ixmp4_backend, caplog: pytest.LogCaptureFixture, scenario: Scenario
    ) -> None:
        from ixmp.backend.ixmp4 import log

        # Test logging a warning for first_model_year
        with caplog.at_level("WARNING", logger=log.name):
            ixmp4_backend.clone(
                s=scenario,
                platform_dest=scenario.platform,
                model=scenario.model,
                scenario=scenario.scenario + "_clone",
                annotation="not used",
                keep_solution=False,
                first_model_year=1,
            )

        expected = (
            "ixmp4-backed Scenarios don't support cloning from `first_model_year` only!"
        )
        assert expected in caplog.messages

    def test_clear_solution(
        self, ixmp4_backend, caplog: pytest.LogCaptureFixture, scenario: Scenario
    ) -> None:
        from ixmp.backend.ixmp4 import log

        # Test logging a warning for from_year
        with caplog.at_level("WARNING", logger=log.name):
            ixmp4_backend.clear_solution(s=scenario, from_year=1)

        expected = (
            "ixmp4 does not support removing the solution only after a certain year"
        )
        assert expected in caplog.messages

    def test_run_id(self, ixmp4_backend, scenario: Scenario) -> None:
        # NOTE Depending on what run_id() should actually fetch, this needs adapting
        # scenario sets up a new Run, which has version 1
        assert ixmp4_backend.run_id(ts=scenario) == 1

    def test_item_delete_elements(self, ixmp4_backend, scenario: Scenario) -> None:
        # Prepare some data
        run = ixmp4_backend.index[scenario]
        indexset_data = "foo"
        indexset = run.optimization.indexsets.create("Indexset")
        indexset.add(data=indexset_data)
        table_data = {indexset.name: [indexset_data]}
        table = run.optimization.tables.create(
            "Table", constrained_to_indexsets=[indexset.name]
        )
        table.add(data=table_data)

        # Assert data is stored in scenario
        set_data = scenario.set(name=table.name)
        assert isinstance(set_data, pd.DataFrame)
        assert not set_data.empty

        # Test data deletion for Tables
        ixmp4_backend.item_delete_elements(
            s=scenario, type="set", name=table.name, keys=[[indexset_data]]
        )
        new_data = scenario.set(name=table.name)
        assert isinstance(new_data, pd.DataFrame)
        assert new_data.empty

    def test_delete_item(self, ixmp4_backend, scenario: Scenario) -> None:
        # Create a 'set' to delete
        run = ixmp4_backend.index[scenario]
        indexset = run.optimization.indexsets.create("Indexset")
        ixmp4_backend.delete_item(s=scenario, type="set", name=indexset.name)

        # Test there are no 'sets' on scenario anymore
        assert ixmp4_backend.list_items(s=scenario, type="set") == []

    def test_write_file(self, ixmp4_backend) -> None:
        # Test raising an error for unknown file extension
        with pytest.raises(NotImplementedError):
            ixmp4_backend.write_file(
                path=Path("none.txt"), item_type=ItemType.SET, filters={}
            )

        # Test raising with incorrect ItemType
        with pytest.raises(NotImplementedError):
            ixmp4_backend.write_file(
                path=Path("none.gdx"), item_type=ItemType.EQU, filters={}
            )

    def test_read_file(self, ixmp4_backend, scenario: Scenario) -> None:
        # Test raising an error for unknown file extension
        with pytest.raises(NotImplementedError):
            ixmp4_backend.read_file(
                path=Path("none.txt"), item_type=ItemType.SET, filters={}
            )

        # Test raising with incorrect ItemType
        with pytest.raises(NotImplementedError):
            ixmp4_backend.read_file(
                path=Path("none.gdx"), item_type=ItemType.EQU, filters={}
            )

        # Test raising when filters doesn't include a proper Scenario
        with pytest.raises(
            ValueError, match="read from GDX requires a Scenario object"
        ):
            ixmp4_backend.read_file(
                path=Path("none.gdx"), item_type=ItemType.MODEL, filters={}
            )

        # TODO Should the ixmp4_backend get its own TypedDict for kwargs that only
        # allows our expected values? If so, we probably need to adjust.
        # Test raising with extra kwargs
        with pytest.raises(ValueError, match="keyword arguments"):
            ixmp4_backend.read_file(
                path=Path("none.gdx"),
                item_type=ItemType.MODEL,
                init_items=True,
                filters={"scenario": scenario},
            )


class TestOptions:
    @pytest.mark.parametrize(
        "exp, jdbc_compat_arg",
        (
            (False, "0"),
            (False, "false"),
            (False, "False"),
            (False, "no"),
            (False, "NO"),
            (True, "1"),
            (True, "FOO"),
            (True, "true"),
            (True, "True"),
            (True, "yes"),
            (True, "YES"),
        ),
    )
    def test_init(self, exp: bool, jdbc_compat_arg) -> None:
        from ixmp.backend.ixmp4 import Options

        opts = Options(ixmp4_name="foo", jdbc_compat=jdbc_compat_arg)
        assert exp is opts.jdbc_compat
