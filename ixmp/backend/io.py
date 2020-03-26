import logging

import pandas as pd

from ixmp.utils import as_str_list


log = logging.getLogger(__name__)


def ts_read_file(ts, path, firstyear=None, lastyear=None):
    """Read data from a CSV or Microsoft Excel file at *path* into *ts*.

    See also
    --------
    TimeSeries.add_timeseries
    TimeSeries.read_file
    """

    if path.suffix == '.csv':
        df = pd.read_csv(path)
    elif path.suffix == '.xlsx':
        df = pd.read_excel(path)

    ts.check_out(timeseries_only=True)
    ts.add_timeseries(df, year_lim=(firstyear, lastyear))

    msg = f'adding timeseries data from {path}'
    if firstyear:
        msg += f' from {firstyear}'
    if lastyear:
        msg += f' until {lastyear}'
    ts.commit(msg)


def s_write_excel(be, s, path):
    """Write *s* to a Microsoft Excel file at *path*.

    See also
    --------
    Scenario.to_excel
    """
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
            msg = (f'Cannot read {ix_type} {name!r}: index set(s) cannot be '
                   f'inferred for name(s) {ambiguous_idx}')
            log.warning(msg)
            raise ValueError from None

        # Initialize
        getattr(scenario, f'init_{ix_type}')(name, new_idx)
    else:
        # Item exists; check that is has the same index names

        # [] and None are equivalent; convert to be consistent
        if isinstance(new_idx, list) and new_idx == []:
            new_idx = None

        if existing_names != new_idx:
            msg = (f'Existing {ix_type} {name!r} has index names(s) '
                   f' {existing_names} != {new_idx} in {path.name}')
            log.warning(msg)
            raise ValueError from None


def s_read_excel(be, s, path, add_units=False, init_items=False,
                 commit_steps=False):
    """Read data from a Microsoft Excel file at *path* into *s*.

    See also
    --------
    Scenario.read_excel
    """
    log.info(f'Read data from {path}')

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
            idx_sets = data.columns.to_list()

            if init_items:
                # Determine index set(s) for this set
                if len(idx_sets) == 1:
                    if idx_sets == [0]:  # pragma: no cover
                        # Old-style export with uninformative '0' as a column
                        # header; assume it's an index set
                        log.warning(f"Add {name} with header '0' as index set")
                        idx_sets = None
                    elif idx_sets == [name]:
                        # Set's own name as column header -> an index set
                        idx_sets = None
                    else:
                        pass  # 1-D set indexed by another set

                try:
                    maybe_init_item(s, 'set', name, idx_sets, path)
                except ValueError:
                    continue  # Ambiguous or conflicting; skip this set

            if len(data.columns) == 1:
                # Convert data frame into 1-D vector
                data = data.iloc[:, 0].values

                if idx_sets is not None and idx_sets != [name]:
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

    # Add equ/par/var data
    for name, ix_type in name_type[name_type != 'set'].items():
        if ix_type in ('equ', 'var'):
            log.info(f'Cannot read {ix_type} {name!r}')
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
            try:
                # Same as init_scalar if idx_sets == []
                maybe_init_item(s, ix_type, name, idx_sets, path)
            except ValueError:
                continue  # Ambiguous or conflicting; skip this parameter

        if not len(idx_sets):
            # No index sets -> scalar parameter; must supply empty 'key' column
            # for add_par()
            df['key'] = None

        s.add_par(name, df)

        if commit_steps:
            # Commit after every parameter
            s.commit(f'Loaded {ix_type} {name!r} from {path}')
            s.check_out()
