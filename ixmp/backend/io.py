import logging
from collections import deque
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import gams.transfer as gt
import pandas as pd

# from gams import GamsWorkspace
from ixmp4.core import Run
from ixmp4.core.optimization.indexset import IndexSet
from ixmp4.core.optimization.scalar import Scalar
from ixmp4.data.abstract.optimization.equation import Equation as AbstractEquation
from ixmp4.data.abstract.optimization.variable import Variable as AbstractVariable

from ixmp.util import as_str_list, maybe_check_out, maybe_commit

from . import ItemType

if TYPE_CHECKING:
    from typing import TypeVar

    from ixmp4.core.optimization.equation import Equation

    # from ixmp4.core.optimization.indexset import IndexSet
    from ixmp4.core.optimization.parameter import Parameter

    # from ixmp4.core.optimization.scalar import Scalar
    from ixmp4.core.optimization.table import Table
    from ixmp4.core.optimization.variable import Variable

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


def _add_to_container(container: gt.Container, items: list[Item4]) -> None:
    """Add ixmp4 data `items` to `container`."""
    if not items:
        return  # Nothing to be done

    # Identify the ixmp4 class name and corresponding gams.transfer class
    kind = type(items[0]).__name__
    klass = {
        "IndexSet": gt.Set,
        "Parameter": gt.Parameter,
        "Scalar": gt.Parameter,
        "Table": gt.Set,
        "Variable": gt.Variable,
        "Equation": gt.Equation,
    }[kind]

    def domain(item: Item4) -> Optional[list[str]]:
        if isinstance(item, (IndexSet, Scalar)):
            return None
        else:
            return item.indexsets

    def records(
        item: Item4,
    ) -> Union[
        float,
        Union[list[float], list[int], list[str]],
        dict[str, Union[list[float], list[int], list[str]]],
        None,
    ]:
        if isinstance(item, Scalar):
            return item.value
        elif not len(item.data):
            return None

        if isinstance(item, IndexSet):
            return item.data

        result = item.data

        # Pop items not to be stored
        for name in {
            "Equation": ["levels", "marginals"],
            "Parameter": ["units"],
            "Variable": ["levels", "marginals"],
        }[kind]:
            result.pop(name)

        return result

    for item in items:
        # The gams documentation confuses me: The docstring says `type` is required, the
        # example says no. It seems to work fine like this, but if we do need a value,
        # maybe we could guess based on
        # https://github.com/iiasa/ixmp_source/blob/master/src/main/java/at/ac/iiasa/ixmp/objects/Scenario.java#L1926,
        klass(
            container=container,
            name=item.name,
            domain=domain(item),
            records=records(item),
            # Optional, but must be str
            description=item.docs if item.docs else "",
        )


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
    record_version_packages: list[str],
    include_variables_and_equations: bool = False,
) -> None:
    """Write scenario data from the `run` to a GDX file via GAMS container."""

    # TODO How to deal with [*] Set?

    container = gt.Container()

    repository = [
        run.optimization.indexsets,
        run.optimization.scalars,
        run.optimization.tables,
        run.optimization.parameters,
        run.optimization.variables,
        run.optimization.equations,
    ]
    idx = slice(None) if include_variables_and_equations else slice(-2)
    for r in repository[idx]:
        # FIXME Type of 'r' is inferred as BaseFacade, the narrowest common supertype of
        #       IndexSetRepository etc.; BaseFacade.list() is not defined.
        _add_to_container(container, r.list())  # type: ignore [attr-defined]

    _record_versions(container=container, packages=record_version_packages)

    container.write(write_to=file_name)


def _read_variables_to_run(
    container: gt.Container, run: Run, variables: Iterable[AbstractVariable]
) -> None:
    for variable in variables:
        # Prepare columns to select from container.data
        # DF also includes lower, upper, scale
        columns = ["level", "marginal"]
        variable_columns = variable.column_names or variable.indexsets
        if variable_columns:
            columns += variable_columns

        run.backend.optimization.variables.add_data(
            variable_id=variable.id,
            data=(
                container.data[variable.name]
                .records[columns]
                .rename(columns={"level": "levels", "marginal": "marginals"})
            ),
        )


def _read_equations_to_run(
    container: gt.Container, run: Run, equations: Iterable[AbstractEquation]
) -> None:
    for equation in equations:
        # Prepare columns to select from container.data
        # DF also includes lower, upper, scale
        columns = ["level", "marginal"]
        equation_columns = equation.column_names or equation.indexsets
        if equation_columns:
            columns += equation_columns

        run.backend.optimization.equations.add_data(
            equation_id=equation.id,
            data=(
                container.data[equation.name]
                .records[columns]
                .rename(columns={"level": "levels", "marginal": "marginals"})
            ),
        )


def read_gdx_to_run(
    run: Run,
    result_file: Path,
    equ_list: list[str],
    var_list: list[str],
    comment: str,
    check_solution: bool,
) -> None:
    if comment:
        log.warning(f"Ignoring comment {comment} for now; unused by ixmp4!")
    if check_solution:
        log.warning(
            f"Ignoring check_solution={check_solution} for now; unused by ixmp4!"
        )

    variables = (
        run.backend.optimization.variables.list(name__in=var_list)
        if len(var_list)
        else run.backend.optimization.variables.list()
    )
    equations = (
        run.backend.optimization.equations.list(name__in=equ_list)
        if len(equ_list)
        else run.backend.optimization.equations.list()
    )

    container = gt.Container(load_from=result_file)

    _read_variables_to_run(container=container, run=run, variables=variables)
    _read_equations_to_run(container=container, run=run, equations=equations)
