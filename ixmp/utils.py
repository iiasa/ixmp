import logging
import re
import sys
from collections.abc import Iterable
from functools import partial
from inspect import Parameter, signature
from pathlib import Path
from typing import Dict, Iterator, List, Tuple
from urllib.parse import urlparse

import pandas as pd

from ixmp.backend import ItemType

log = logging.getLogger(__name__)

# globally accessible logger
_LOGGER = None


def logger():
    """Access global logger"""
    global _LOGGER
    if _LOGGER is None:
        logging.basicConfig()
        _LOGGER = logging.getLogger()
        _LOGGER.setLevel("INFO")
    return _LOGGER


def as_str_list(arg, idx_names=None):
    """Convert various *arg* to list of str.

    Several types of arguments are handled:
    - None: returned as None.
    - str: returned as a length-1 list of str.
    - iterable of values: returned as a list with each value converted to str
    - dict, with list of idx_names: the idx_names are used to look up values
      in the dict, the resulting list has the corresponding values in the same
      order.

    """
    if arg is None:
        return None
    elif idx_names is None:
        # arg must be iterable
        # NB narrower ABC Sequence does not work here; e.g. test_excel_io()
        #    fails via Scenario.add_set().
        if isinstance(arg, Iterable) and not isinstance(arg, str):
            return list(map(str, arg))
        else:
            return [str(arg)]
    else:
        return [str(arg[idx]) for idx in idx_names]


def isscalar(x):
    """Returns True if x is a scalar"""
    return not isinstance(x, Iterable) or isinstance(x, str)


def check_year(y, s):
    """Returns True if y is an int, raises an error if y is not None"""
    if y is not None:
        if not isinstance(y, int):
            raise ValueError("arg `{}` must be an integer!".format(s))
        return True


def diff(a, b, filters=None) -> Iterator[Tuple[str, pd.DataFrame]]:
    """Compute the difference between Scenarios `a` and `b`.

    :func:`diff` combines :func:`pandas.merge` and :meth:`Scenario.items`.
    Only parameters are compared. :func:`~pandas.merge` is called with the
    arguments ``how="outer", sort=True, suffixes=("_a", "_b"), indicator=True";
    the merge is performed on all columns except 'value' or 'unit'.

    Yields
    ------
    tuple of str, pandas.DataFrame
        Tuples of item name and data.
    """
    # Iterators; index 0 corresponds to `a`, 1 to `b`
    items = [
        a.items(filters=filters, type=ItemType.PAR),
        b.items(filters=filters, type=ItemType.PAR),
    ]
    # State variables for loop
    name = ["", ""]
    data: List[pd.DataFrame] = [None, None]

    # Elements for first iteration
    name[0], data[0] = next(items[0])
    name[1], data[1] = next(items[1])

    while True:
        # Convert scalars to pd.DataFrame
        data = list(map(maybe_convert_scalar, data))

        # Compare the names from `a` and `b` to ensure matching items
        if name[0] == name[1]:
            # Matching items in `a` and `b`
            current_name = name[0]
            left, right = data
        else:
            # Mismatched; either `a` or `b` has no data for these filters
            current_name = min(*name)
            if name[0] > current_name:
                # No data in `a` for `current_name`; create an empty DataFrame
                left, right = pd.DataFrame(columns=data[1].columns), data[1]
            else:
                left, right = data[0], pd.DataFrame(columns=data[0].columns)

        # Either merge on remaining columns; or, for scalars, on the indices
        on = sorted(set(left.columns) - {"value", "unit"})
        on_arg: Dict[str, object] = (
            dict(on=on) if on else dict(left_index=True, right_index=True)
        )

        # Merge the data from each side
        yield current_name, pd.merge(
            left,
            right,
            how="outer",
            **on_arg,
            sort=True,
            suffixes=("_a", "_b"),
            indicator=True,
        )

        # Maybe advance each iterators
        for i in (0, 1):
            try:
                if name[i] == current_name:
                    # data was compared in this iteration; advance
                    name[i], data[i] = next(items[i])
            except StopIteration:
                # No more data for this iterator.
                # Use "~" because it sorts after all ASCII characters
                name[i], data[i] = "~ end", None

        if name[0] == name[1] == "~ end":
            break


def maybe_check_out(timeseries, state=None):
    """Check out `timeseries` depending on `state`.

    If `state` is :obj:`None`, then :meth:`check_out` is called.

    Returns
    -------
    :obj:`True`
        if `state` was :obj:`None` and a check out was performed, i.e.
        `timeseries` was previously in a checked-in state.
    :obj:`False`
        if `state` was :obj:`None` and no check out was performed, i.e.
        `timeseries` was already in a checked-out state.
    `state`
        if `state` was not :obj:`None` and no check out was attempted.

    Raises
    ------
    ValueError
        If `timeseries` is a :class:`.Scenario` object and
        :meth:`~.Scenario.has_solution` is :obj:`True`.

    See Also
    --------
    :meth:`.TimeSeries.check_out`
    :meth:`.Scenario.check_out`
    """
    if state is not None:
        return state

    try:
        timeseries.check_out()
    except RuntimeError:
        # If `timeseries` is new (has not been committed), the checkout
        # attempt raises an exception
        return False
    else:
        return True


def maybe_commit(timeseries, condition, message):
    """Commit `timeseries` with `message` if `condition` is :obj:`True`.

    Returns
    -------
    :obj:`True`
        if a commit is performed.
    :obj:`False`
        if any exception is raised during the attempted commit. The exception
        is logged with level ``INFO``.

    See Also
    --------
    :meth:`.TimeSeries.commit`
    """
    if not condition:
        return False

    try:
        timeseries.commit(message)
    except RuntimeError as exc:
        log.info(f"maybe_commit() didn't commit: {exc}")
        return False
    else:
        return True


def maybe_convert_scalar(obj) -> pd.DataFrame:
    """Convert `obj` to :class:`pandas.DataFrame`.

    Parameters
    ----------
    obj
        Any value returned by :meth:`Scenario.par`. For a scalar
        (0-dimensional) parameter, this will be :class:`dict`.

    Returns
    -------
    pandas.DataFrame
        :meth:`maybe_convert_scalar` always returns a data frame.
    """
    if isinstance(obj, dict):
        return pd.DataFrame.from_dict({0: obj}, orient="index")
    else:
        return obj


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
    if components.scheme not in ("ixmp", ""):
        raise ValueError("URL must begin with ixmp:// or //")

    platform_info = dict()
    if components.netloc:
        platform_info["name"] = components.netloc

    scenario_info = dict()

    path = components.path.split("/")
    if len(path):
        # If netloc was given, path begins with '/'; discard
        path = path if len(path[0]) else path[1:]

        if len(path) < 2:
            raise ValueError("URL path must be 'MODEL/SCENARIO'")

        scenario_info["model"] = path[0]
        scenario_info["scenario"] = "/".join(path[1:])

    if len(components.query):
        raise ValueError(f"queries ({components.query}) not supported in URLs")

    if len(components.fragment):
        try:
            version = int(components.fragment)
        except ValueError:
            if components.fragment != "new":
                raise
            else:
                version = "new"
        finally:
            scenario_info["version"] = version

    return platform_info, scenario_info


def partial_split(func, kwargs):
    """Forgiving version of :func:`functools.partial`.

    Returns a partial object and leftover kwargs not applicable to `func`.
    """
    # Names of parameters to
    par_names = signature(func).parameters
    func_args, extra = {}, {}
    for name, value in kwargs.items():
        if (
            name in par_names
            and par_names[name].kind == Parameter.POSITIONAL_OR_KEYWORD
        ):
            func_args[name] = value
        else:
            extra[name] = value

    return partial(func, **func_args), extra


def year_list(x):
    """Return the elements of x that can be cast to year (int)."""
    lst = []
    for i in x:
        try:
            int(i)  # this is a year
            lst.append(i)
        except ValueError:
            pass
    return lst


def filtered(df, filters):
    """Returns a filtered dataframe based on a filters dictionary"""
    if filters is None:
        return df

    mask = pd.Series(True, index=df.index)
    for k, v in filters.items():
        isin = df[k].isin(as_str_list(v))
        mask = mask & isin
    return df[mask]


def format_scenario_list(
    platform, model=None, scenario=None, match=None, default_only=False, as_url=False
):
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
        match = re.compile(".*" + match + ".*")
    except TypeError:
        pass

    def describe(df):
        N = len(df)
        min = df.version.min()
        max = df.version.max()

        result = dict(N=N, range="")
        if N > 1:
            result["range"] = "{}–{}".format(min, max)
            if N != max:
                result["range"] += " ({} versions)".format(N)

        try:
            mask = df.is_default.astype(bool)
            result["default"] = df.loc[mask, "version"].iat[0]
        except IndexError:
            result["default"] = max

        return pd.Series(result)

    info = (
        platform.scenario_list(model=model, scen=scenario, default=default_only)
        .groupby(["model", "scenario"])
        .apply(describe)
        .reset_index()
    )

    if not len(info):
        # No results; re-create a minimal empty data frame
        info = pd.DataFrame([], columns=["model", "scenario", "default", "N"])

    info["scenario"] = info["scenario"].str.cat(info["default"].astype(str), sep="#")

    if match:
        info = info[info["model"].str.match(match) | info["scenario"].str.match(match)]

    lines = []

    if as_url:
        info["url"] = "ixmp://{}".format(platform.name)
        urls = info["url"].str.cat([info["model"], info["scenario"]], sep="/")
        lines = urls.tolist()
    else:
        width = 0 if not len(info) else info["scenario"].str.len().max()
        info["scenario"] = info["scenario"].str.ljust(width)

        for model, m_info in info.groupby(["model"]):
            lines.extend(
                [
                    "",
                    model + "/",
                    "  " + "\n  ".join(m_info["scenario"].str.cat(m_info["range"])),
                ]
            )

        lines.append("")

    # Summary information
    if not as_url:
        lines.extend(
            [
                str(len(info["model"].unique())) + " model name(s)",
                str(len(info["scenario"].unique())) + " scenario name(s)",
                str(len(info)) + " (model, scenario) combination(s)",
                str(info["N"].sum()) + " total scenarios",
            ]
        )

    return lines


def show_versions(file=sys.stdout):
    """Print information about ixmp and its dependencies to *file*."""
    import importlib
    from subprocess import DEVNULL, CalledProcessError, check_output

    from xarray.util.print_versions import get_sys_info

    from ixmp.model.gams import gams_version

    def _git_log(mod):
        cmd = ["git", "log", "--oneline", "--no-color", "--decorate", "-n 1"]
        try:
            cwd = Path(mod.__file__).parent
            info = check_output(cmd, cwd=cwd, encoding="utf-8", stderr=DEVNULL)
        except Exception:
            # Occurs if "git log" fails; or if mod.__file__ is None (#338)
            return ""
        else:
            return f"\n     {info.rstrip()}"

    deps = [
        None,  # Prints a separator
        # ixmp stack
        "ixmp",
        "message_ix",
        "message_data",
        None,
        # ixmp dependencies
        "click",
        "dask",
        "graphviz",
        "jpype",
        "openpyxl",
        "pandas",
        "pint",
        "xarray",
        "yaml",
        None,
        # Optional dependencies, dependencies of message_ix and message_data
        "iam_units",
        "jupyter",
        "matplotlib",
        "plotnine",
        "pyam",
        None,
    ]

    info = []
    for module_name in deps:
        try:
            # Import the module
            mod = sys.modules.get(module_name, None) or importlib.import_module(
                module_name
            )
        except Exception:
            # Couldn't import
            info.append((module_name, None))
            continue

        # Retrieve git log information, if any
        gl = _git_log(mod)
        try:
            version = mod.__version__
        except Exception:
            # __version__ not available
            version = "installed"
        finally:
            info.append((module_name, version + gl))

        if module_name == "jpype":
            info.append(("… JVM path", mod.getDefaultJVMPath()))

    # Also display GAMS version, if any
    try:
        version = gams_version()
    except (CalledProcessError, FileNotFoundError):
        version = "'gams' executable not in PATH"
    finally:
        info.extend([("GAMS", version), (None, None)])

    # Use xarray to get system & Python information
    info.extend(get_sys_info()[1:])  # Exclude the commit number

    for k, stat in info:
        if (k, stat) == (None, None):
            # Separator line
            print("", file=file)
        else:
            print(f"{k + ':':12} {stat}", file=file)


def update_par(scenario, name, data):
    """Update parameter *name* in *scenario* using *data*, without overwriting.

    Only values which do not already appear in the parameter data are added.
    """
    tmp = pd.concat([scenario.par(name), data])
    columns = list(filter(lambda c: c != "value", tmp.columns))
    tmp = tmp.drop_duplicates(subset=columns, keep=False)

    if len(tmp):
        scenario.add_par(name, tmp)
