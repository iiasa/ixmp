import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Literal, Optional, TypeVar, Union, cast

import gams.transfer as gt
import pandas as pd
from ixmp4.core import Run
from ixmp4.core.optimization.base import Lister
from ixmp4.core.optimization.equation import Equation
from ixmp4.core.optimization.indexset import IndexSet, IndexSetRepository
from ixmp4.core.optimization.parameter import Parameter
from ixmp4.core.optimization.scalar import Scalar
from ixmp4.core.optimization.table import Table
from ixmp4.core.optimization.variable import Variable
from ixmp4.data.abstract.optimization.equation import Equation as AbstractEquation
from ixmp4.data.abstract.optimization.variable import Variable as AbstractVariable

from ixmp.model.gams import gams_info
from ixmp.util.ixmp4 import ContainerData

log = logging.getLogger(__name__)

# Type variable that can be any one of these 6 types, but not a union of 2+ of them
Item4 = TypeVar("Item4", Equation, IndexSet, Parameter, Scalar, Table, Variable)


def _domain(item: Item4) -> Optional[list[str]]:
    """Return domain for `item`.

    For IndexSets and Scalars, this is :obj:`None`.

    For all others, this is `item.indexsets`.
    """
    if isinstance(item, (IndexSet, Scalar)):
        return None
    else:
        return item.indexset_names


def _records(
    item: Item4,
) -> Union[
    float,
    Union[list[float], list[int], list[str]],
    dict[str, Union[list[float], list[int], list[str]]],
    None,
]:
    """Return records for `item`.

    For Scalars, this is `item.value`.

    For empty other items, this is :obj:`None`.

    For IndexSets and Tables, this is `item.data`.

    For Parameters, this is `item.data` without the column 'units'.

    For Equations and Variables, this is `item.data`
    without the columns 'levels' and 'marginals'.
    """
    if isinstance(item, Scalar):
        return item.value
    elif not len(item.data):
        return None

    if isinstance(item, (IndexSet, Table)):
        return item.data

    result = item.data

    # Pop items not to be stored
    for name in {
        Equation: ["levels", "marginals"],
        Parameter: ["units"],
        Variable: ["levels", "marginals"],
    }[type(item)]:
        result.pop(name)

    return result


def _ensure_correct_item_order(items: list[Item4], repo: Lister) -> list[Item4]:
    """Reorder items to ensure the GDX file is written correctly.

    gamsapi stores all unique elements of all items in a single list internally. If item
    A adds elements that item B shares, item B is not adding them again. If B's elements
    are then requested, they are returned starting with those in A in their order,
    disrupting expectations.
    """
    if isinstance(repo, IndexSetRepository):
        # Move all indexsets called 'type_*' to the end of the list
        for type_indexset in list(
            filter(lambda item: item.name.startswith("type_"), items)
        ):
            items.remove(type_indexset)
            items.append(type_indexset)

    return items


def _align_records_and_domain(
    item: Item4, records: dict[str, Union[list[float], list[int], list[str]]]
) -> dict[str, Union[list[float], list[int], list[str]]]:
    """Align the order of `records.keys()` with domain of `item`."""
    # This function will only be called for these types
    assert isinstance(item, (Table, Parameter, Variable, Equation))

    # `records` may contain keys that are column_names, but not indexsets
    domain_order = item.column_names or item.indexset_names

    if not domain_order:
        # This could happen for Variables or Equations
        return records
    else:
        # Parameters contain values that we want to keep
        if isinstance(item, Parameter):
            domain_order.append("values")

        return {key: records[key] for key in domain_order}


def _update_item_in_container(container: gt.Container, item: ContainerData) -> None:
    """Update `item` in `container`.

    Note that this overwrites existing records of `item`.
    """
    # NOTE This is not updating the comment

    kwargs = dict(name=item.name, domain=item.domain, records=item.records)

    if item.kind in ("IndexSet", "Table"):
        container.addSet(**kwargs)
    elif item.kind in ("Scalar", "Parameter"):
        container.addParameter(**kwargs)
    elif item.kind == "Equation":
        # NOTE This was hardcoded in ixmp_source the same way; not sure how the
        # 'type' in the container affects anything
        kwargs["type"] = "E"
        container.addEquation(**kwargs)
    else:  # Variable
        container.addVariable(**kwargs)


def _add_items_to_container(
    container: gt.Container, items: list[ContainerData]
) -> None:
    """Add or update `items` to/in `container`."""
    if not items:
        return  # Nothing to be done

    for item in items:
        # Identify the corresponding gams.transfer class
        klass = {
            "IndexSet": gt.Set,
            "Parameter": gt.Parameter,
            "Scalar": gt.Parameter,
            "Table": gt.Set,
            "Variable": gt.Variable,
            "Equation": gt.Equation,
        }[item.kind]

        # NOTE A container may already contain some data from the MESSAGE-specific setup
        # This seems to imply a UniqueConstraint over all item names, though.
        if not container.hasSymbols(item.name):
            # NOTE This was hardcoded in ixmp_source the same way; not sure how the
            # 'type' in the container affects anything
            kwargs = {"type": "E"} if item.kind == "Equation" else {}

            # Create an item instance linked to the container
            klass(
                container=container,
                name=item.name,
                domain=item.domain,
                records=item.records,
                # Optional, but must be str
                description=item.docs or "",
                **kwargs,
            )
        else:
            _update_item_in_container(container=container, item=item)


def _convert_ixmp4_items_to_containerdata(items: list[Item4]) -> list[ContainerData]:
    """Convert list of ixmp4 `items` to ContainerData."""
    if not items:
        return []  # Nothing to be done

    # Identify the ixmp4 class name
    kind = cast(
        Literal["Scalar", "IndexSet", "Table", "Parameter", "Equation", "Variable"],
        type(items[0]).__name__,
    )

    container_items: list[ContainerData] = []

    for item in items:
        records = _records(item=item)
        # The order of keys in 'domain' is used by gamsapi to overwrite the order of
        # keys in 'records', so make sure they are aligned:
        # NOTE ixmp4 ensures that all _records.keys() are in _domain and vice-versa
        if isinstance(records, dict):
            records = _align_records_and_domain(item=item, records=records)

        container_items.append(
            ContainerData(
                name=item.name,
                kind=kind,
                records=records,
                domain=_domain(item),
                docs=item.docs,
            )
        )

    return container_items


def _record_versions(container: gt.Container, packages: list[str]) -> None:
    """Store Python package versions as set elements to be written to GDX.

    The values are stored in a 2-dimensional set named ``ixmp_version``, where the
    first element is the package name, and the second is its version according to
    :func:`importlib.metadata.version`). If the package is not installed, the
    string "(not installed)" is stored.
    """
    from importlib.metadata import PackageNotFoundError, version

    name = "ixmp_version"

    # Each tuple consists of (package_name, package_version)
    versions: list[tuple[str, str]] = []

    # Handle each identified package
    for package in packages:
        try:
            # Retrieve the version; replace characters not supported by GAMS
            package_version = version(package).replace(".", "-")
        except PackageNotFoundError:
            package_version = "(not installed)"  # Not installed

        versions.append((package, package_version))

    # Add Set to the container
    container.addSet(name=name, domain=["*", "*"], records=versions)


def write_run_to_gdx(
    run: Run,
    file_name: Path,
    container_data: list[ContainerData],
    record_version_packages: list[str],
    include_variables_and_equations: bool = False,
) -> None:
    """Write data from the `run` to a GDX file at `file_name` via a GAMS Container.

    Parameters
    ----------
    run : :class:`ixmp4.core.Run`
        The Run in which the Scenario data is stored.
    file_name: :obj:`Path`
        The location at which the GDX file should be written.
    container_data : list of :class:`ixmp.util.ixmp4.ContainerData`.
        Data that should also be written to the GDX file
        in the form of ContainerData.
    record_version_packages : list of str
        A list of package names for which versions will be stored in the GDX file.
    include_variables_and_equations: bool, optional
        Flag to indicate whether Variabels and Equations should be written to the GDX
        file.
        Default: :obj:`False`.
    """

    # TODO How to deal with [*] Set? That seems to be handled by GAMS automatically.

    # Define the container
    container = gt.Container(system_directory=str(gams_info().system_dir))

    repository: list[Lister] = [
        run.optimization.indexsets,
        run.optimization.scalars,
        run.optimization.tables,
        run.optimization.parameters,
        run.optimization.variables,
        run.optimization.equations,
    ]
    idx = slice(None) if include_variables_and_equations else slice(-2)
    for r in repository[idx]:
        # Reorder items if necessary for GAMS to successfully read the GDX
        ixmp4_items = _ensure_correct_item_order(items=r.list(), repo=r)

        # Convert ixmp4 items to ContainerData to streamline adding to container
        container_items = _convert_ixmp4_items_to_containerdata(items=ixmp4_items)

        _add_items_to_container(container, container_items)

    # Add additional data *after* the required items to avoid confusing GAMS' internal
    # Unique Element List
    _add_items_to_container(container, container_data)

    _record_versions(container=container, packages=record_version_packages)

    container.write(write_to=file_name)


# NOTE since we currently only read Variables and Equations, this function only covers
# these cases
def _set_columns_to_read_from_records(
    item: Union[AbstractVariable, AbstractEquation],
) -> list[str]:
    """Gather all columns for `item` to read from GDX records."""
    # Prepare columns to select from container.data
    # DF also includes lower, upper, scale
    columns = ["levels", "marginals"]
    item_columns = item.column_names or item.indexset_names
    if item_columns:
        item_columns.extend(columns)

    return item_columns if item_columns else columns


# NOTE not sure we only need equations and variables; if we need others, abstracting one
# function for reading would not be as easy, since we might need different details, so
# I'm keeping them separate for now


def _read_variables_to_run(
    container: gt.Container, run: Run, variables: Iterable[AbstractVariable]
) -> None:
    """Read `variables` from `container` and store them in `run`."""
    for variable in variables:
        columns_of_interest = _set_columns_to_read_from_records(item=variable)

        try:
            records = pd.DataFrame(container.data[variable.name].records)
        except KeyError:
            # container doesn't contain this variable
            continue

        # Avoid touching the DB for empty data
        if not records.empty:
            # NOTE Overwriting the columns changes GAMS' "level" and "marginal" to the
            # ixmp4-expected "levels" and "marginals"
            # NOTE This assumes that the order the columns are defined in the gams code
            # is equal to the order in item.column_names and item.indexsets
            records.columns = pd.Index(
                columns_of_interest + ["lower", "upper", "scale"]
            )
            run.backend.optimization.variables.add_data(
                id=variable.id, data=records[columns_of_interest]
            )


def _read_equations_to_run(
    container: gt.Container, run: Run, equations: Iterable[AbstractEquation]
) -> None:
    """Read `equations` from `container` and store them in `run`."""
    for equation in equations:
        columns_of_interest = _set_columns_to_read_from_records(item=equation)

        try:
            records = pd.DataFrame(container.data[equation.name].records)
        except KeyError:
            # container doesn't contain this equation
            continue

        # Avoid touching the DB for empty data
        if not records.empty:
            # NOTE Overwriting the columns changes GAMS' "level" and "marginal" to the
            # ixmp4-expected "levels" and "marginals"
            # NOTE This assumes that the order the columns are defined in the gams code
            # is equal to the order in item.column_names and item.indexsets
            records.columns = pd.Index(
                columns_of_interest + ["lower", "upper", "scale"]
            )
            run.backend.optimization.equations.add_data(
                id=equation.id, data=records[columns_of_interest]
            )


def read_gdx_to_run(
    run: Run,
    result_file: Path,
    equ_list: list[str],
    var_list: list[str],
    comment: str,
    check_solution: bool,
) -> None:
    """Read data from `result_file` to `run` via a GAMS Container.

    Parameters
    ----------
    run : :class:`ixmp4.core.Run`
        The Run in which the Scenario data is to be stored.
    result_file: :obj:`Path`
        The location from which the GDX file should be read.
    equ_list : list of str
        Names of Equations to read from `result_file`.
    var_list : list of str
        Names of Variables to read from `result_file`.
    comment : str
        Unused by ixmp4.
        A comment to store when adding the data to `run`.
    check_solution : bool
        Unused by ixmp4.
        A flag to indicate whether the solution should be checked
        (for consistency, maybe?).
    """
    # Warn about unused parameters
    if comment:
        log.warning(f"Ignoring comment {comment} for now; unused by ixmp4!")
    if check_solution:
        log.warning(
            f"Ignoring check_solution={check_solution} for now; unused by ixmp4!"
        )

    # Create a GAMS Container from the `result_file`
    container = gt.Container(
        load_from=result_file, system_directory=str(gams_info().system_dir)
    )

    # Load requested Variables and read them to `run`
    # NOTE This handles empty `var_list`, too,
    # which is not necessary as long as any Variables are required in message_ix
    variables = (
        run.backend.optimization.variables.list(run_id=run.id, name__in=var_list)
        if len(var_list)
        else run.backend.optimization.variables.list(run_id=run.id)
    )
    _read_variables_to_run(container=container, run=run, variables=variables)

    # Load requested Equations and read them to `run`
    # NOTE This handles empty `equ_list`, too,
    # which is not necessary as long as any Equations are required in message_ix
    equations = (
        run.backend.optimization.equations.list(run_id=run.id, name__in=equ_list)
        if len(equ_list)
        else run.backend.optimization.equations.list(run_id=run.id)
    )
    _read_equations_to_run(container=container, run=run, equations=equations)
