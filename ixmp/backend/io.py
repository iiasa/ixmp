import logging
from collections import deque
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

from ixmp.util import as_str_list, maybe_check_out, maybe_commit
from ixmp.util.ixmp4 import ContainerData

from . import ItemType

# Type variable that can be any one of these 6 types, but not a union of 2+ of them
Item4 = TypeVar("Item4", Equation, IndexSet, Parameter, Scalar, Table, Variable)


log = logging.getLogger(__name__)

#: Maximum number of rows supported by the Excel file format. See
#: :meth:`.to_excel` and :ref:`excel-data-format`.
EXCEL_MAX_ROWS = 1048576


def ts_read_file(ts, path, firstyear=None, lastyear=None):
    """Read data from a CSV or Microsoft Excel file at *path* into *ts*.

    See also
    --------
    .TimeSeries.add_timeseries
    .TimeSeries.read_file
    """

    if path.suffix == ".csv":
        df = pd.read_csv(path)
    elif path.suffix == ".xlsx":
        df = pd.read_excel(path)

    ts.check_out(timeseries_only=True)
    ts.add_timeseries(df, year_lim=(firstyear, lastyear))

    msg = f"adding timeseries data from {path}"
    if firstyear:
        msg += f" from {firstyear}"
    if lastyear:
        msg += f" until {lastyear}"
    ts.commit(msg)


def s_write_excel(be, s, path, item_type, filters=None, max_row=None):
    """Write *s* to a Microsoft Excel file at *path*.

    See also
    --------
    .Scenario.to_excel
    """
    # Default: empty dict
    filters = filters or dict()

    # Types of items to write
    ix_types = ["set", "par"]
    if ItemType.VAR in item_type:
        ix_types.append("var")
    if ItemType.EQU in item_type:
        ix_types.append("equ")

    # item name -> ixmp type
    name_type = {}
    for ix_type in ix_types:
        names = sorted(be.list_items(s, ix_type))
        name_type.update({n: ix_type for n in names})

    # Open file
    writer = pd.ExcelWriter(path)

    omitted = set()
    empty_sets = []

    # Don't allow the user to set a value that will truncate data
    max_row = min(int(max_row or EXCEL_MAX_ROWS), EXCEL_MAX_ROWS)

    for name, ix_type in name_type.items():
        if ix_type == "par":
            # Use only the filters corresponding to dimensions of this item
            item_filters = {
                k: v for k, v in filters.items() if k in be.item_index(s, name, "names")
            }
        else:
            item_filters = None

        # Extract data: dict, pd.Series, or pd.DataFrame
        data = be.item_get_elements(s, ix_type, name, item_filters)

        if isinstance(data, dict):
            # Scalar equ/par/var: series with index like 'value', 'unit'.
            # Convert to DataFrame with 1 row.
            data = pd.Series(data, name=name).to_frame().transpose()
        elif isinstance(data, pd.Series):
            # Index set: use own name as the header
            data.name = name

        if data.empty:
            if ix_type != "set":
                # Don't write empty equ/par/var
                omitted.add(name)
            else:
                # Write empty sets later
                empty_sets.append((name, data))
            continue

        # Write data in multiple sheets

        # List of break points, plus the final row
        splits = list(range(0, len(data), max_row)) + [len(data)]
        # Pairs of row numbers, e.g. (0, 100), (100, 200), ...
        first_last = zip(splits, splits[1:])

        for sheet_num, (first_row, last_row) in enumerate(first_last, start=1):
            # Format the sheet name, possibly with a suffix
            sheet_name = name + (f"({sheet_num})" if sheet_num > 1 else "")

            # Subset the data (only on rows, if a DataFrame) and write
            data.iloc[first_row:last_row].to_excel(
                writer, sheet_name=sheet_name, index=False
            )

    # Discard entries that were not written
    for name in omitted:
        name_type.pop(name)

    # Write the name -> type map
    pd.Series(name_type, name="ix_type").rename_axis(
        index="item"
    ).reset_index().to_excel(writer, sheet_name="ix_type_mapping", index=False)

    # Write empty sets last
    for name, data in empty_sets:
        data.to_excel(writer, sheet_name=name, index=False)

    writer.close()


def maybe_init_item(scenario, ix_type, name, new_idx, path):
    """Call :meth:`~.init_set`, :meth:`.init_par`, etc. if possible.

    Logs an intelligible warning and then raises ValueError in two cases:

    - the *new_idx* is ambiguous, e.g. containing index names that cannot be
      used to infer index sets, or
    - an existing item has index names that are different from *new_idx*.

    """
    try:
        # [] and None are equivalent; convert to be consistent
        existing_names = scenario.idx_names(name) or None
    except KeyError:
        # Item does not exist

        # Check for ambiguous index names
        ambiguous_idx = sorted(set(new_idx or []) - set(scenario.set_list()))
        if len(ambiguous_idx):
            log.warning(
                f"Cannot read {ix_type} {repr(name)}: index set(s) cannot be "
                f"inferred for name(s) {ambiguous_idx}"
            )
            raise ValueError from None

        # Initialize
        getattr(scenario, f"init_{ix_type}")(name, new_idx)
    else:
        # Item exists; check that is has the same index names

        # [] and None are equivalent; convert to be consistent
        if isinstance(new_idx, list) and new_idx == []:
            new_idx = None

        if existing_names != new_idx:
            log.warning(
                f"Existing {ix_type} {repr(name)} has index names(s) "
                f" {existing_names} != {new_idx} in {path.name}"
            )
            raise ValueError from None


# FIXME reduce complexity 22 → ≤13
def s_read_excel(  # noqa: C901
    be, s, path, add_units=False, init_items=False, commit_steps=False
):
    """Read data from a Microsoft Excel file at *path* into *s*.

    See also
    --------
    .Scenario.read_excel
    """
    log.info(f"Read data from {path}")

    # Get item name -> ixmp type mapping as a pd.Series
    xf = pd.ExcelFile(path, engine="openpyxl")
    name_type = xf.parse("ix_type_mapping", index_col="item")["ix_type"]

    # Queue of (set name, data) to add
    sets_to_add = deque((n, None) for n in name_type.index[name_type == "set"])

    def parse_item_sheets(name):
        """Read data for item *name*, possibly across multiple sheets."""
        dfs = [xf.parse(name)]

        # Collect data from repeated sheets due to max_row limit
        for x in filter(lambda n: n.startswith(name + "("), xf.sheet_names):
            dfs.append(xf.parse(x))

        # Concatenate once and return
        return pd.concat(dfs, axis=0)

    # Add sets in two passes:
    # 1. Index sets, required to initialize other sets.
    # 2. Sets indexed by others.
    while True:
        try:
            # Get an item from the queue
            name, data = sets_to_add.popleft()
        except IndexError:
            break  # Finished

        # log.debug(name)

        first_pass = data is None
        if first_pass:
            # Read data
            data = parse_item_sheets(name)

        # Determine index set(s) for this set
        idx_sets = data.columns.to_list()
        if len(idx_sets) == 1:
            if idx_sets == [0]:  # pragma: no cover
                # Old-style export with uninformative '0' as a column header;
                # assume it is an index set
                log.warning(f"Add {name} with header '0' as index set")
                idx_sets = None
            elif idx_sets == [name]:
                # Set's own name as column header -> an index set
                idx_sets = None
            else:
                pass  # 1-D set indexed by another set

        if first_pass and idx_sets is not None:
            # Indexed set; append to the queue to process later
            sets_to_add.append((name, data))
            continue

        # At this point: either an index set, or second pass when all index
        # sets have been init'd and populated
        if init_items:
            try:
                maybe_init_item(s, "set", name, idx_sets, path)
            except ValueError:
                continue  # Ambiguous or conflicting; skip this set

        # Convert data as expected by add_set
        if len(data.columns) == 1:
            # Convert data frame into 1-D vector
            data = data.iloc[:, 0].values

            if idx_sets is not None:
                # Indexed set must be input as list of list of str
                data = list(map(as_str_list, data))

        try:
            s.add_set(name, data)
        except KeyError:
            raise ValueError(f"no set {repr(name)}; try init_items=True")

    maybe_commit(s, commit_steps, f"Loaded sets from {path}")

    # List of existing units for reference
    units = set(be.get_units())

    # Add equ/par/var data
    for name, ix_type in name_type[name_type != "set"].items():
        if ix_type in ("equ", "var"):
            log.info(f"Cannot read {ix_type} {repr(name)}")
            continue

        # Only parameters beyond this point

        df = parse_item_sheets(name)

        maybe_check_out(s)

        if add_units:
            # New units appearing in this parameter
            to_add = set(df["unit"].unique()) - units

            for unit in to_add:
                log.info(f"Add missing unit: {unit}")
                # FIXME cannot use the comment f'Loaded from {path}' here; too
                #       long for JDBCBackend
                be.set_unit(unit, "Loaded from file")

            # Update the reference set to avoid re-adding these units
            units |= to_add

        # NB if equ/var were imported, also need to filter 'lvl', 'mrg' here
        idx_sets = list(filter(lambda v: v not in ("value", "unit"), df.columns))

        if init_items:
            try:
                # Same as init_scalar if idx_sets == []
                maybe_init_item(s, ix_type, name, idx_sets, path)
            except ValueError:
                continue  # Ambiguous or conflicting; skip this parameter

        if not len(idx_sets):
            # No index sets -> scalar parameter; must supply empty 'key' column
            # for add_par()
            df["key"] = None

        s.add_par(name, df)

        # Commit after every parameter
        maybe_commit(s, commit_steps, f"Loaded {ix_type} {repr(name)} from {path}")

    maybe_commit(s, not commit_steps, f"Import from {path}")


def _domain(item: Item4) -> Optional[list[str]]:
    """Return domain for `item`.

    For IndexSets and Scalars, this is :obj:`None`.

    For all others, this is `item.indexsets`.
    """
    if isinstance(item, (IndexSet, Scalar)):
        return None
    else:
        return item.indexsets


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
    domain_order = item.column_names or item.indexsets

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
    container = gt.Container()

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
    item_columns = item.column_names or item.indexsets
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

        records = pd.DataFrame(container.data[variable.name].records)

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
                variable_id=variable.id, data=records[columns_of_interest]
            )


def _read_equations_to_run(
    container: gt.Container, run: Run, equations: Iterable[AbstractEquation]
) -> None:
    """Read `equations` from `container` and store them in `run`."""
    for equation in equations:
        columns_of_interest = _set_columns_to_read_from_records(item=equation)

        records = pd.DataFrame(container.data[equation.name].records)

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
                equation_id=equation.id, data=records[columns_of_interest]
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
    container = gt.Container(load_from=result_file)

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
