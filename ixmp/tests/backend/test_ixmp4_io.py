from pathlib import Path
from typing import Union

import gams.transfer as gt
import pytest

import ixmp
from ixmp.backend.ixmp4 import IXMP4Backend
from ixmp.backend.ixmp4_io import (
    _add_items_to_container,
    _align_records_and_domain,
    _convert_ixmp4_items_to_containerdata,
    _domain,
    _ensure_correct_item_order,
    _read_equations_to_run,
    _read_variables_to_run,
    _record_versions,
    _records,
    _set_columns_to_read_from_records,
    _update_item_in_container,
    read_gdx_to_run,
    write_run_to_gdx,
)
from ixmp.util.ixmp4 import ContainerData

# TODO Should all tests handling scenarios get `request` and use unique scenario names?
# TODO Should we make the run-retrieval its own fixture?


@pytest.mark.ixmp4
def test__domain(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]
    indexset = run.optimization.indexsets.create("Indexset")
    table = run.optimization.tables.create(
        "Table", constrained_to_indexsets=[indexset.name]
    )

    assert _domain(indexset) is None
    assert _domain(table) == [indexset.name]


@pytest.mark.ixmp4
def test__records(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    test_mp.add_unit("unit")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]

    # Test records of a Scalar
    scalar = run.optimization.scalars.create("Scalar", 3.14, "unit")
    assert _records(scalar) == scalar.value

    # Test records of an empty Table
    indexset = run.optimization.indexsets.create("Indexset")
    table = run.optimization.tables.create(
        "Table", constrained_to_indexsets=[indexset.name]
    )
    assert _records(table) is None

    # Test records of an IndexSet
    indexset.add(["foo", "bar"])
    assert _records(indexset) == indexset.data

    # Test records of a Parameter
    parameter = run.optimization.parameters.create(
        "Parameter", constrained_to_indexsets=[indexset.name]
    )
    parameter.add(
        {indexset.name: ["foo", "bar"], "values": [1, 2], "units": ["unit"] * 2}
    )
    expected = parameter.data.copy()
    expected.pop("units")
    assert _records(parameter) == expected


@pytest.mark.ixmp4
def test__ensure_correct_item_order(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    test_mp.add_unit("unit")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]

    years = run.optimization.indexsets.create("years")
    nodes = run.optimization.indexsets.create("nodes")
    type_years = run.optimization.indexsets.create("type_years")
    type_nodes = run.optimization.indexsets.create("type_nodes")

    assert _ensure_correct_item_order(
        items=[type_years, years, type_nodes, nodes], repo=run.optimization.indexsets
    ) == [years, nodes, type_years, type_nodes]


@pytest.mark.ixmp4
def test__align_records_and_domain(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]

    # Test item without domain order
    equation = run.optimization.equations.create("Equation")
    expected: dict[str, Union[list[float], list[int], list[str]]] = {"foo": [1, 2, 3]}
    assert _align_records_and_domain(item=equation, records=expected) == expected

    # Test sorting of records
    indexset_1 = run.optimization.indexsets.create("Indexset 1")
    indexset_2 = run.optimization.indexsets.create("Indexset 2")
    parameter = run.optimization.parameters.create(
        "Parameter", constrained_to_indexsets=[indexset_1.name, indexset_2.name]
    )
    records: dict[str, Union[list[float], list[int], list[str]]] = {
        indexset_2.name: [1, 2, 3],
        "values": [1.0, 2.0, 3.0],
        indexset_1.name: ["1", "2", "3"],
    }
    expected = {
        indexset_1.name: ["1", "2", "3"],
        indexset_2.name: [1, 2, 3],
        "values": [1.0, 2.0, 3.0],
    }

    assert _align_records_and_domain(item=parameter, records=records) == expected


def test__update_item_in_container() -> None:
    item_list = [
        ContainerData(
            name="table", kind="Table", records=["foo", "bar"], domain=["indexset"]
        ),
        ContainerData(name="scalar", kind="Scalar", records=3.14),
        ContainerData(name="equation", kind="Equation", records=None),
        ContainerData(name="variable", kind="Variable", records=None),
    ]

    container = gt.Container()

    for item in item_list:
        _update_item_in_container(container=container, item=item)

    assert all(container.hasSymbols(symbols=[item.name for item in item_list]))


def test__add_items_to_container() -> None:
    container = gt.Container()

    # Test handling of empty list
    empty_list: list[ContainerData] = []
    _add_items_to_container(container=container, items=empty_list)
    assert container.data == {}

    # Test adding new item
    records = ["foo", "bar"]
    item_list = [ContainerData(name="Indexset", kind="IndexSet", records=records)]
    _add_items_to_container(container=container, items=item_list)
    assert container.hasSymbols("Indexset")
    indexsets = container.getSets()
    assert len(indexsets) == 1
    indexset = indexsets[0]
    assert indexset.records["uni"].to_list() == records

    # Test updating existing item
    records = ["baz"]
    item_list = [ContainerData(name="Indexset", kind="IndexSet", records=records)]
    _add_items_to_container(container=container, items=item_list)
    indexsets = container.getSets()
    assert len(indexsets) == 1
    indexset = indexsets[0]
    assert indexset.records["uni"].to_list() == records


@pytest.mark.ixmp4
def test__convert_ixmp4_items_to_containerdata(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]

    # Test handling of empty list
    empty_list = _convert_ixmp4_items_to_containerdata(items=[])
    assert len(empty_list) == 0

    # Test adding an Indexset (covers no new cases, but we need to index a Table)
    indexset = run.optimization.indexsets.create("Indexset")
    indexset.add(data=["foo", "bar", "baz"])
    container_list = _convert_ixmp4_items_to_containerdata(items=[indexset])
    assert len(container_list) == 1
    indexset_data = container_list[0]
    assert indexset.name == indexset_data.name
    assert indexset.data == indexset_data.records

    # Test adding an item where records are a dict
    table = run.optimization.tables.create(
        "Table", constrained_to_indexsets=[indexset.name]
    )
    table.add(data={indexset.name: ["baz", "foo", "bar"]})
    container_list = _convert_ixmp4_items_to_containerdata(items=[table])
    table_data = container_list[0]
    assert table.name == table_data.name
    assert table.data == table_data.records


def test__record_version() -> None:
    container = gt.Container()

    # Test recording the package version of something that's always present
    _record_versions(container=container, packages=["ixmp"])
    indexsets = container.getSets()
    assert len(indexsets) == 1
    indexset = indexsets[0]
    assert indexset.name == "ixmp_version"
    assert indexset.domain == ["*", "*"]


@pytest.mark.ixmp4
def test_write_run_to_gdx(test_mp: ixmp.Platform, tmp_path: Path) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]

    # Test minimal call targeting all kinds of items
    # NOTE All other functions are unit-tested already and other tests call write() with
    # actual data, so we probably don't need to replicate here
    file_path = tmp_path / "test_write.gdx"
    write_run_to_gdx(
        run=run,
        file_name=file_path,
        container_data=[],
        record_version_packages=["ixmp"],
        include_variables_and_equations=True,
    )
    container = gt.Container(load_from=tmp_path / "test_write.gdx")
    assert len(container.data) == 1
    assert "ixmp_version" in container.data.keys()


@pytest.mark.ixmp4
def test__set_columns_to_read_from_records(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]
    default_columns = ["levels", "marginals"]

    # Test an Equation without dimensions
    equation = run.backend.optimization.equations.create(run_id=run.id, name="Equation")
    columns = _set_columns_to_read_from_records(item=equation)
    assert columns == default_columns

    # Test an indexed Variable
    indexset = run.optimization.indexsets.create("Indexset")
    variable = run.backend.optimization.variables.create(
        run_id=run.id, name="Variable", constrained_to_indexsets=[indexset.name]
    )
    columns = _set_columns_to_read_from_records(item=variable)
    assert columns == [indexset.name] + default_columns

    # Test an Equation with dimension names
    equation_2 = run.backend.optimization.equations.create(
        run_id=run.id,
        name="Equation 2",
        constrained_to_indexsets=[indexset.name],
        column_names=["Column"],
    )
    columns = _set_columns_to_read_from_records(item=equation_2)
    assert columns == ["Column"] + default_columns


# NOTE Keeping the read_equ()/read_var() tests separate because the read_*() functions
# are separate


@pytest.mark.ixmp4
def test__read_variables_to_run(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]
    indexset = run.optimization.indexsets.create("Indexset")
    indexset.add(data=["foo", "bar", "baz"])
    variable = run.backend.optimization.variables.create(
        run_id=run.id, name="Variable", constrained_to_indexsets=[indexset.name]
    )
    container = gt.Container()
    # NOTE Reading GDX always contains 'level', 'marginal', 'lower', 'upper', 'scale'
    # The first two will be changed to plural, the latter three ignored
    records = {
        indexset.name: ["foo", "bar", "baz"],
        "level": [1.0, 2.0, 3.0],
        "marginal": [0, 0, 0],
        "lower": [0, 0, 0],
        "upper": [0, 0, 0],
        "scale": [0, 0, 0],
    }
    container.addVariable(name="Variable", domain=[indexset.name], records=records)

    _read_variables_to_run(container=container, run=run, variables=[variable])
    expected = {
        indexset.name: records[indexset.name],
        "levels": records["level"],
        "marginals": records["marginal"],
    }
    variable = run.backend.optimization.variables.get(run_id=run.id, name=variable.name)
    assert variable.data == expected


@pytest.mark.ixmp4
def test__read_equations_to_run(test_mp: ixmp.Platform) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]
    indexset = run.optimization.indexsets.create("Indexset")
    indexset.add(data=["foo", "bar", "baz"])
    equation = run.backend.optimization.equations.create(
        run_id=run.id, name="Equation", constrained_to_indexsets=[indexset.name]
    )
    container = gt.Container()
    # NOTE Reading GDX always contains 'level', 'marginal', 'lower', 'upper', 'scale'
    # The first two will be changed to plural, the latter three ignored
    records = {
        indexset.name: ["foo", "bar", "baz"],
        "level": [1.0, 2.0, 3.0],
        "marginal": [0, 0, 0],
        "lower": [0, 0, 0],
        "upper": [0, 0, 0],
        "scale": [0, 0, 0],
    }
    container.addEquation(
        name="Equation", type="E", domain=[indexset.name], records=records
    )

    _read_equations_to_run(container=container, run=run, equations=[equation])
    expected = {
        indexset.name: records[indexset.name],
        "levels": records["level"],
        "marginals": records["marginal"],
    }
    equation = run.backend.optimization.equations.get(run_id=run.id, name=equation.name)
    assert equation.data == expected


@pytest.mark.ixmp4
def test_read_gdx_to_run(test_mp: ixmp.Platform, tmp_path: Path) -> None:
    scen = ixmp.Scenario(test_mp, "model", "scenario", version="new")

    # TODO Remove after proper parametrization
    assert isinstance(test_mp._backend, IXMP4Backend)

    run = test_mp._backend.index[scen]

    # NOTE Names without space to produce "valid GAMS names"
    variable_1 = run.backend.optimization.variables.create(
        run_id=run.id, name="Variable1"
    )
    variable_2 = run.backend.optimization.variables.create(
        run_id=run.id, name="Variable2"
    )
    variable_3 = run.backend.optimization.variables.create(
        run_id=run.id, name="Variable3"
    )
    records: dict[str, Union[list[float], list[int], list[str]]] = {
        "level": [1.0],
        "marginal": [0],
        "lower": [0],
        "upper": [0],
        "scale": [0],
    }
    file_path = tmp_path / "test_read.gdx"
    write_run_to_gdx(
        run=run,
        file_name=file_path,
        container_data=[
            ContainerData(name=var.name, kind="Variable", records=records)
            for var in [variable_1, variable_2, variable_3]
        ],
        record_version_packages=["ixmp"],
        include_variables_and_equations=True,
    )
    var_list = [variable_1.name, variable_3.name]

    # Test minimal confirmable call (with otherwise unused parameters)
    read_gdx_to_run(
        run=run,
        result_file=file_path,
        equ_list=[],
        var_list=var_list,
        comment="Test comment",
        check_solution=True,
    )
    for var in run.optimization.variables.list():
        # 'variable_2' is not supposed to be read in
        expected = (
            {}
            if var.name == variable_2.name
            else {"levels": records["level"], "marginals": records["marginal"]}
        )

        assert var.data == expected
