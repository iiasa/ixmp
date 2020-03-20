import pandas as pd


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
