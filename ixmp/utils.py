import logging

import numpy as np
import pandas as pd

import ixmp as ix

# globally accessible logger
_LOGGER = None


def logger():
    """Access global logger"""
    global _LOGGER
    if _LOGGER is None:
        logging.basicConfig()
        _LOGGER = logging.getLogger()
        _LOGGER.setLevel('INFO')
    return _LOGGER


def pd_read(f, *args, **kwargs):
    """Try to read a file with pandas, no fancy stuff"""
    if f.endswith('csv'):
        return pd.read_csv(f, *args, **kwargs)
    else:
        return pd.read_excel(f, *args, **kwargs)


def pd_write(df, f, *args, **kwargs):
    """Try to write one or more dfs with pandas, no fancy stuff"""
    is_pd = isinstance(df, (pd.DataFrame, pd.Series))
    if f.endswith('csv'):
        if not is_pd:
            raise ValueError('Must pass a Dataframe if using csv files')
        df.to_csv(f, *args, **kwargs)
    else:
        writer = pd.ExcelWriter(f, engine='xlsxwriter')
        if is_pd:
            sheet_name = kwargs.pop('sheet_name', 'Sheet1')
            df = {sheet_name: df}
        for k, v in df.items():
            v.to_excel(writer, sheet_name=k, *args, **kwargs)
        writer.save()


def numcols(df):
    dtypes = df.dtypes
    return [i for i in dtypes.index
            if dtypes.loc[i].name.startswith(('float', 'int'))]


def import_timeseries(mp, data, model, scenario, version=None,
                      firstyear=None, lastyear=None):
    if not isinstance(mp, ix.Platform):
        raise ValueError("{} is not a valid 'ixmp.Platform' instance".
                         format(mp))

    if version is not None:
        version = int(version)
    scen = ix.Scenario(mp, model, scenario, version)

    df = ix.utils.pd_read(data)
    df = df.rename(columns={c: str(c).lower() for c in df.columns})

    cols = ix.utils.numcols(df)
    years = [int(i) for i in cols]
    fyear = int(firstyear or min(years))
    lyear = int(lastyear or max(years))
    cols = [c for c in cols if int(c) >= fyear and int(c) <= lyear]
    df = df[['region', 'variable', 'unit'] + cols]
    df.region = [x if x == 'World' else 'R11_' + x for x in df.region]

    scen.check_out(timeseries_only=True)
    scen.add_timeseries(df)

    annot = 'adding timeseries data from file {}'.format(data)
    if firstyear is not None:
        annot = '{} from {}'.format(annot, firstyear)
    if lastyear is not None:
        annot = '{} until {}'.format(annot, lastyear)
    scen.commit(annot)
