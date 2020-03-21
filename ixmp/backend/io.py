from contextlib import contextmanager
import logging

import pandas as pd

from ixmp.utils import as_str_list


log = logging.getLogger(__name__)


def s_write_excel(be, s, path):
    """Write *s* to a Microsoft Excel file at *path*."""
    # item name -> ixmp type
    name_type = {}
    for ix_type in ('set', 'par', 'var', 'equ'):
        name_type.update({n: ix_type for n in be.list_items(s, ix_type)})

    # Open file
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    omitted = set()

    for name, ix_type in name_type.items():
        # Extract data: dict, pd.Series, or pd.DataFrame
        data = be.item_get_elements(s, ix_type, name)

        if isinstance(data, dict):
            # Scalar equ/par/var: series with index like 'value', 'unit'.
            # Convert to DataFrame with 1 row.
            data = pd.Series(data, name=name) \
                     .to_frame() \
                     .transpose()
        elif isinstance(data, pd.Series):
            # Index set: use own name as the header
            data.name = name

        # Write empty sets, but not equ/par/var
        if ix_type != 'set' and data.empty:
            omitted.add(name)
            continue

        data.to_excel(writer, sheet_name=name, index=False)

    # Discard entries that were not written
    for name in omitted:
        name_type.pop(name)

    # Write the name -> type map
    pd.Series(name_type, name='ix_type') \
      .rename_axis(index='item') \
      .reset_index() \
      .to_excel(writer, sheet_name='ix_type_mapping', index=False)

    writer.save()


@contextmanager
def handle_existing(scenario, ix_type, name, new_idx_sets, path):
    """Context manager for :meth:`~.init_set`, :meth:`.init_par`, etc.

    If the init_*() call within the context fails, this checks whether the
    exception is fatal; if so, it is re-raised with a readable message.
    """
    try:
        yield
    except ValueError as e:
        if 'exists' not in e.args[0]:
            raise  # Some other ValueError

        # Check that existing item has the same index sets

        # [] and None are equivalent; convert to be consistent
        existing = scenario.idx_sets(name) or None
        if isinstance(new_idx_sets, list) and new_idx_sets == []:
            new_idx_sets = None

        if existing != new_idx_sets:
            raise ValueError(f'{ix_type} {name!r} has index sets '
                             f'{existing} in Scenario; {new_idx_sets}'
                             f'in {path}')


def s_read_excel(be, s, path, add_units=False, init_items=False,
                 commit_steps=False):
    """Read data from a Microsoft Excel file at *path* into *s*.

    Parameters
    ----------
    be : Backend
    s : Scenario
    path : os.PathLike
    add_units : bool, optional
        Add missing units, if any, to the Platform instance.
    init_items : bool, optional
        Initialize sets and parameters that do not already exist in the
        Scenario.
    commit_steps : bool, optional
        Commit changes after every data addition.
    """
    log.info(f'Reading data from {path}')

    # Get item name -> ixmp type mapping as a pd.Series
    xf = pd.ExcelFile(path)
    name_type = xf.parse('ix_type_mapping', index_col='item')['ix_type']

    # List of *set name, data) to add
    sets_to_add = [(n, None) for n in name_type.index[name_type == 'set']]

    # Add sets in two passes:
    # 1. Index sets, required to initialize other sets.
    # 2. Sets indexed by others.
    for name, data in sets_to_add:
        first_pass = data is None
        if first_pass:
            # Read data
            data = xf.parse(name)

        if (first_pass and len(data.columns) == 1) or not first_pass:
            # Index set or second appearance; add immediately
            idx_sets = data.columns

            if init_items:
                # Determine index set(s) for this set
                if len(idx_sets) == 1:
                    if idx_sets == [0]:
                        # Old-style export with uninformative '0' as a column
                        # header; assume it's an index set
                        log.warning(f"Add {name} with header '0' as index set")
                        idx_sets = None
                    elif idx_sets == [name]:
                        # Set's own name as column header -> an index set
                        idx_sets = None
                    else:
                        pass  # 1-D set indexed by another set

                with handle_existing(s, 'set', name, idx_sets, path):
                    s.init_set(name, idx_sets)

            if len(data.columns) == 1:
                # Convert data frame into 1-D vector
                data = data.iloc[:, 0].values

                if idx_sets is not None:
                    # Indexed set must be input as list of list of str
                    data = list(map(as_str_list, data))

            try:
                s.add_set(name, data)
            except KeyError:
                raise ValueError(f'no set {name!r}; try init_items=True')
        else:
            # Reappend to the list to process later
            sets_to_add.append((name, data))

    if commit_steps:
        s.commit(f'Loaded sets from {path}')
        s.check_out()

    if add_units:
        # List of existing units for reference
        units = set(be.get_units())

    # Read parameter data
    for name, ix_type in name_type[name_type != 'set'].items():
        if ix_type in ('equ', 'var'):
            log.info(f'Cannot import {ix_type} {name!r}')
            continue

        # Only parameters beyond this point

        df = xf.parse(name)

        if add_units:
            # New units appearing in this parameter
            to_add = set(df['unit'].unique()) - units

            for unit in to_add:
                log.info(f'Add missing unit: {unit}')
                # FIXME cannot use the comment f'Loaded from {path}' here; too
                #       long for JDBCBackend
                be.set_unit(unit, f'Loaded from file')

            # Update the reference set to avoid re-adding these units
            units |= to_add

        # NB if equ/var were imported, also need to filter 'lvl', 'mrg' here
        idx_sets = list(
            filter(lambda v: v not in ('value', 'unit'), df.columns)
        )

        if init_items:
            # Same as init_scalar if idx_sets == []
            with handle_existing(s, ix_type, name, idx_sets, path):
                s.init_par(name, idx_sets)

        if not len(idx_sets):
            # No index sets -> scalar parameter; must supply empty 'key' column
            # for add_par()
            df['key'] = None

        s.add_par(name, df)

        if commit_steps:
            # Commit after every parameter
            s.commit(f'Loaded {ix_type} {name!r} from {path}')
            s.check_out()
