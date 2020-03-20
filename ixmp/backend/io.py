import logging

import pandas as pd


log = logging.getLogger(__name__)


def s_write_excel(be, s, path):
    """Write *s* to a Microsoft Excel file at *path*."""
    # List items to be exported
    name_type = {}
    for ix_type in ('set', 'par', 'var', 'equ'):
        for name in be.list_items(s, ix_type):
            name_type[name] = ix_type

    # Open file
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    # Extract and write data
    for name, ix_type in name_type.items():
        data = be.item_get_elements(s, ix_type, name)

        if isinstance(data, dict):
            data = pd.Series(data, name=name).to_frame()
            if ix_type in ('par', 'var', 'equ'):
                # Scalar parameter
                data = data.transpose()
        elif isinstance(data, pd.Series):
            data.name = name

        if data.empty:
            continue

        data.to_excel(writer, sheet_name=name, index=False)

    # Also write the name -> type map
    pd.Series(name_type, name='ix_type') \
      .rename_axis(index='item') \
      .reset_index() \
      .to_excel(writer, sheet_name='ix_type_mapping', index=False)

    writer.save()


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

    # Get item name -> ixmp type mapping
    xf = pd.ExcelFile(path)
    name_type = xf.parse('ix_type_mapping').set_index('item')['ix_type']

    # List of sets to add
    sets_to_add = [(n, None) for n in name_type.index[name_type == 'set']]

    # Add sets
    for name, data in sets_to_add:
        first_pass = data is None
        if first_pass:
            data = xf.parse(name)
        if (first_pass and data.columns == [name]) or not first_pass:
            # Index set or second appearance; add immediately
            if init_items:
                idx_sets = data.columns if len(data.columns) > 1 else None
                s.init_set(name, idx_sets)
            if data.columns == [name]:
                data = data[name].values
            s.add_set(name, data)
        else:
            # Reappend to the list to process later
            sets_to_add.append[(name, data)]

    if commit_steps:
        s.commit(f'Loaded initial data from {path}')
        s.check_out()

    if add_units:
        units = set(be.get_units())

    # Read parameter data
    for name, ix_type in name_type[name_type != 'set'].items():
        if ix_type in ('equ', 'var'):
            log.info(f'Not importing item {name!r} of type {ix_type!r}')
            continue

        df = xf.parse(name)

        if add_units:
            to_add = set(df['unit'].unique()) - units
            for unit in to_add:
                log.info(f'Adding missing unit: {unit}')
                be.set_unit(unit, f'Loaded from {path}')

            # Update the set
            units |= to_add

        idx_sets = list(
            filter(lambda v: v not in ('value', 'unit'), df.columns)
        )

        if init_items:
            s.init_par(name, idx_sets)

        if not len(idx_sets):
            # No index sets -> scalar parameter
            df['key'] = None

        s.add_par(name, df)

        if commit_steps:
            s.commit(f'Loaded {name} from {path}')
            s.check_out()
