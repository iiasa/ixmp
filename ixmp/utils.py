from collections.abc import Iterable
import logging
import re
from urllib.parse import urlparse

import pandas as pd
import six
from pathlib import Path


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


def as_str_list(arg, idx_names=None):
    """Convert various *arg* to list of str.

    Several types of arguments are handled:
    - None: returned as None.
    - str: returned as a length-1 list of str.
    - list of values: returned as a list with each value converted to str
    - dict, with list of idx_names: the idx_names are used to look up values
      in the dict, the resulting list has the corresponding values in the same
      order.

    """
    if arg is None:
        return None
    elif idx_names is None:
        # arg must be iterable
        return list(map(str, arg)) if islistable(arg) else [str(arg)]
    else:
        return [str(arg[idx]) for idx in idx_names]


def isstr(x):
    """Returns True if x is a string"""
    return isinstance(x, six.string_types)


def isscalar(x):
    """Returns True if x is a scalar"""
    return not isinstance(x, Iterable) or isstr(x)


def islistable(x):
    """Returns True if x is a list but not a string"""
    return isinstance(x, Iterable) and not isstr(x)


def check_year(y, s):
    """Returns True if y is an int, raises an error if y is not None"""
    if y is not None:
        if not isinstance(y, int):
            raise ValueError('arg `{}` must be an integer!'.format(s))
        return True


def parse_url(url):
    """Parse *url* and return Platform and Scenario information.

    A URL (Uniform Resource Locator), as the name implies, uniquely identifies
    a specific scenario and (optionally) version of a model, as well as
    (optionally) the database in which it is stored. ixmp URLs take forms
    like::

        ixmp://PLATFORM/MODEL/SCENARIO[#VERSION]
        MODEL/SCENARIO[#VERSION]

    where:

    - The ``PLATFORM`` is a configured platform name; see :obj:`ixmp.config`.
    - ``MODEL`` may not contain the forward slash character ('/'); ``SCENARIO``
      may contain any number of forward slashes. Both must be supplied.
    - ``VERSION`` is optional but, if supplied, must be an integer.

    Returns
    -------
    platform_info : dict
        Keyword argument 'name' for the :class:`Platform` constructor.
    scenario_info : dict
        Keyword arguments for a :class:`Scenario` on the above platform:
        'model', 'scenario' and, optionally, 'version'.

    Raises
    ------
    ValueError
        For malformed URLs.
    """
    components = urlparse(url)
    if components.scheme not in ('ixmp', ''):
        raise ValueError('URL must begin with ixmp:// or //')

    platform_info = dict()
    if components.netloc:
        platform_info['name'] = components.netloc

    scenario_info = dict()

    path = components.path.split('/')
    if len(path):
        # If netloc was given, path begins with '/'; discard
        path = path if len(path[0]) else path[1:]

        if len(path) < 2:
            raise ValueError("URL path must be 'MODEL/SCENARIO'")

        scenario_info['model'] = path[0]
        scenario_info['scenario'] = '/'.join(path[1:])

    if len(components.query):
        raise ValueError(f"queries ({components.query}) not supported in URLs")

    if len(components.fragment):
        scenario_info['version'] = int(components.fragment)

    return platform_info, scenario_info


def pd_read(f, *args, **kwargs):
    """Try to read a file with pandas, no fancy stuff"""
    f = Path(f)
    if f.suffix == '.csv':
        return pd.read_csv(f, *args, **kwargs)
    else:
        return pd.read_excel(f, *args, **kwargs)


def pd_write(df, f, *args, **kwargs):
    """Try to write one or more dfs with pandas, no fancy stuff"""
    f = Path(f)
    is_pd = isinstance(df, (pd.DataFrame, pd.Series))
    if f.suffix == '.csv':
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
    """Return the indices of the numeric columns of *df*."""
    dtypes = df.dtypes
    return [i for i in dtypes.index
            if dtypes.loc[i].name.startswith(('float', 'int'))]


def filtered(df, filters):
    """Returns a filtered dataframe based on a filters dictionary"""
    if filters is None:
        return df

    mask = pd.Series(True, index=df.index)
    for k, v in filters.items():
        isin = df[k].isin(as_str_list(v))
        mask = mask & isin
    return df[mask]


def import_timeseries(scenario, data, firstyear=None, lastyear=None):
    """Import from a *data* file into *scenario*."""
    df = pd_read(data)
    df = df.rename(columns={c: str(c).lower() for c in df.columns})

    cols = numcols(df)
    years = [int(i) for i in cols]
    fyear = int(firstyear or min(years))
    lyear = int(lastyear or max(years))
    cols = [c for c in cols if int(c) >= fyear and int(c) <= lyear]
    df = df[['region', 'variable', 'unit'] + cols]
    df.region = [x if x == 'World' else 'R11_' + x for x in df.region]

    scenario.check_out(timeseries_only=True)
    scenario.add_timeseries(df)

    annot = 'adding timeseries data from file {}'.format(data)
    if firstyear is not None:
        annot = '{} from {}'.format(annot, firstyear)
    if lastyear is not None:
        annot = '{} until {}'.format(annot, lastyear)

    scenario.commit(annot)


def format_scenario_list(platform, model=None, scenario=None, match=None,
                         default_only=False, as_url=False):
    """Return a formatted list of TimeSeries on *platform*.

    Parameters
    ----------
    platform : :class:`.Platform`
    model : str, optional
        Model name to restrict results. Passed to :meth:`.scenario_list`.
    scenario : str, optional
        Scenario name to restrict results. Passed to :meth:`.scenario_list`.
    match : str, optional
        Regular expression to restrict results. Only results where the model or
        scenario name matches are returned.
    default_only : bool, optional
        Only return TimeSeries where a default version has been set with
        :meth:`TimeSeries.set_as_default`.
    as_url : bool, optional
        Format results as ixmp URLs.

    Returns
    -------
    list of str
        If *as_url* is :obj:`False`, also include summary information.
    """

    try:
        match = re.compile('.*' + match + '.*')
    except TypeError:
        pass

    def describe(df):
        N = len(df)
        min = df.version.min()
        max = df.version.max()

        result = dict(N=N, range='')
        if N > 1:
            result['range'] = '{}–{}'.format(min, max)
            if N != max:
                result['range'] += ' ({} versions)'.format(N)

        try:
            mask = df.is_default.astype(bool)
            result['default'] = df.loc[mask, 'version'].iat[0]
        except IndexError:
            result['default'] = max

        return pd.Series(result)

    info = platform.scenario_list(model=model, scen=scenario,
                                  default=default_only) \
        .groupby(['model', 'scenario']) \
        .apply(describe) \
        .reset_index()

    info['scenario'] = info['scenario'] \
        .str.cat(info['default'].astype(str), sep='#')

    if match:
        info = info[info['model'].str.match(match)
                    | info['scenario'].str.match(match)]

    if as_url:
        info['url'] = 'ixmp://{}'.format(platform.name)
        urls = info['url'].str.cat([info['model'], info['scenario']], sep='/')
        lines = urls.tolist()
    else:
        lines = []

        if len(info):
            info['scenario'] = info['scenario'] \
                .str.ljust(info['scenario'].str.len().max())

        for model, m_info in info.groupby(['model']):
            lines.extend([
                '',
                model + '/',
                '  ' + '\n  '.join(m_info['scenario'].str.cat(m_info['range']))
            ])

    # Summary information
    if not as_url:
        lines.extend([
            '',
            str(len(info['model'].unique())) + ' model name(s)',
            str(len(info['scenario'].unique())) + ' scenario name(s)',
            str(len(info)) + ' (model, scenario) combination(s)',
            str(info['N'].sum()) + ' total scenarios',
        ])

    return lines


def update_par(scenario, name, data):
    """Update parameter *name* in *scenario* using *data*, without overwriting.

    Only values which do not already appear in the parameter data are added.
    """
    tmp = pd.concat([scenario.par(name), data])
    columns = list(filter(lambda c: c != 'value', tmp.columns))
    tmp = tmp.drop_duplicates(subset=columns, keep=False)

    if len(tmp):
        scenario.add_par(name, tmp)
