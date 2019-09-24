import collections
import logging
import os
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
from urllib.parse import urlparse

import pandas as pd
import six

import ixmp


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


def isstr(x):
    """Returns True if x is a string"""
    return isinstance(x, six.string_types)


def isscalar(x):
    """Returns True if x is a scalar"""
    return not isinstance(x, collections.Iterable) or isstr(x)


def islistable(x):
    """Returns True if x is a list but not a string"""
    return isinstance(x, collections.Iterable) and not isstr(x)


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

    Details:

    - The ``PLATFORM`` is interpreted as follows:

      - The exact string 'local': the default local database.
      - Any string ending in '.local' (e.g. 'foo.local'): the local database
        of that name (e.g. 'foo') in the ``DEFAULT_LOCAL_DB_PATH``.
      - Any other string (e.g. 'bar'): the remote database described by a
        corresponding database properties file (e.g. 'bar.properties') located
        in the current directory or the ``DB_CONFIG_PATH``.

      See :class:`Config <ixmp.config.Config>` for more information about
      search paths.

    - ``MODEL`` may not contain the forward slash character ('/'); ``SCENARIO``
      may contain any number of forward slashes. Both must be supplied.
    - ``VERSION`` is optional, but if supplied must be an integer.

    Returns
    -------
    platform_info : dict
        Keyword arguments 'dbprops' and 'dbtype' for the :class:`Platform`
        constructor. Depending on input, one or both may be absent.
    scenario_info : dict
        Keyword arguments for a :class:`Scenario` on the above platform:
        'model', 'scenario' and, optionally, 'version'.

    Raises
    ------
    ValueError
        For malformed urls.
    """
    components = urlparse(url)
    if components.scheme not in ('ixmp', ''):
        raise ValueError('URL must begin with ixmp:// or //')

    platform_info = dict()

    platform = components.netloc

    # Determine the backend information for the platform
    if platform == 'local':
        platform_info['dbtype'] = 'HSQLDB'
    elif platform.endswith('.local'):
        platform_info['dbtype'] = 'HSQLDB'
        platform_info['dbprops'] = platform.split('.local')[0]
    elif platform:
        if not platform.endswith('.properties'):
            platform += '.properties'
        platform_info = dict(dbprops=platform)

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
    if not isinstance(mp, ixmp.Platform):
        raise ValueError("{} is not a valid 'ixmp.Platform' instance".
                         format(mp))

    if version is not None:
        version = int(version)
    scen = ixmp.Scenario(mp, model, scenario, version)

    df = ixmp.utils.pd_read(data)
    df = df.rename(columns={c: str(c).lower() for c in df.columns})

    cols = ixmp.utils.numcols(df)
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


def harmonize_path(path_or_str):
    """Harmonize mixed '\' and '/' separators in pathlib.Path or str.

    On Windows, R's file.path(...) uses '/', not '\', as a path separator.
    Python's str(WindowsPath(...)) uses '\'. Mixing outputs from the two
    functions (e.g. through rixmp) produces path strings with both kinds of
    separators.
    """
    args = ('/', '\\') if os.name == 'nt' else ('\\', '/')
    return Path(str(path_or_str).replace(*args))
