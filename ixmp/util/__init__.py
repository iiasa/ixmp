import logging
import re
import sys
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from functools import lru_cache
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlparse
from warnings import warn

import numpy as np
import pandas as pd

from ixmp.backend.common import ItemType

if TYPE_CHECKING:
    from ixmp import TimeSeries

log = logging.getLogger(__name__)

# Globally accessible logger.
# TODO remove when :func:`logger` is removed.
_LOGGER = None

#: Packages to inspect in :func:`show_versions`.
SHOW_VERSION_PACKAGES: tuple[str, ...] = (
    "",  # Prints a separator
    # ixmp stack
    "ixmp",
    "message_ix",
    "message_ix_models",
    "message_data",
    "",
    # ixmp dependencies
    "click",
    "dask",
    "genno",
    "graphviz",
    "ixmp4",
    "jpype",
    "openpyxl",
    "pandas",
    "pint",
    "xarray",
    "yaml",
    "",
    # Optional dependencies, dependencies of message_ix and message_data
    "iam_units",
    "jupyter",
    "matplotlib",
    "plotnine",
    "pyam",
    "",
)


def logger():
    """Access global logger.

    .. deprecated:: 3.3
       To control logging from ixmp, instead use :mod:`logging` to retrieve it:

       .. code-block:: python

          import logging
          ixmp_logger = logging.getLogger("ixmp")

          # Example: set the level to INFO
          ixmp_logger.setLevel(logging.INFO)
    """
    warn(
        "ixmp.util.logger() is deprecated as of 3.3.0, and will be removed in ixmp "
        '5.0. Use logging.getLogger("ixmp").',
        DeprecationWarning,
    )
    return logging.getLogger("ixmp")


def as_str_list(arg, idx_names: Optional[Iterable[str]] = None) -> list[str]:
    """Convert various `arg` to list of str.

    Several types of arguments are handled:

    - :obj:`None`: returned as None.
    - class:`str`: returned as a length-1 list of str.
    - iterable of values: :class:`str` is called on each value.
    - :class:`dict`, with `idx_names`: the `idx_names` are used to look up values in the
      dict. The return value has the corresponding values in the same order.

    """
    if arg is None:
        return []
    elif idx_names is None:
        # arg must be iterable
        # NB narrower ABC Sequence does not work here; e.g. test_excel_io() fails via
        #    Scenario.add_set().
        if isinstance(arg, Iterable) and not isinstance(arg, str):
            return list(map(str, arg))
        else:
            return [str(arg)]
    else:
        return [str(arg[idx]) for idx in idx_names]


def isscalar(x):
    """Returns True if `x` is a scalar."""
    warn(
        "ixmp.util.isscalar() will be removed in ixmp >= 5.0. Use numpy.isscalar()",
        DeprecationWarning,
    )
    return np.isscalar(x)


def check_year(y, s):
    """Returns True if y is an int, raises an error if y is not None"""
    if y is not None:
        if not isinstance(y, int):
            raise ValueError("arg `{}` must be an integer!".format(s))
        return True


def diff(a, b, filters=None) -> Iterator[tuple[str, pd.DataFrame]]:
    """Compute the difference between Scenarios `a` and `b`.

    :func:`diff` combines :func:`pandas.merge` and :meth:`.Scenario.items`. Only
    parameters are compared. :func:`~pandas.merge` is called with the arguments
    ``how="outer", sort=True, suffixes=("_a", "_b"), indicator=True``; the merge is
    performed on all columns except 'value' or 'unit'.

    Yields
    ------
    tuple of str, pandas.DataFrame
        tuples of item name and data.
    """
    # Iterators; index 0 corresponds to `a`, 1 to `b`
    items = [
        a.items(filters=filters, type=ItemType.PAR, par_data=True),
        b.items(filters=filters, type=ItemType.PAR, par_data=True),
    ]
    # State variables for loop
    name = ["", ""]
    data: list[pd.DataFrame] = [pd.DataFrame(), pd.DataFrame()]

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
        on_arg: Mapping[str, Any] = (
            dict(on=on) if on else dict(left_index=True, right_index=True)
        )

        # Merge the data from each side
        yield (
            current_name,
            pd.merge(
                left,
                right,
                how="outer",
                **on_arg,
                sort=True,
                suffixes=("_a", "_b"),
                indicator=True,
            ),
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
                name[i], data[i] = "~ end", pd.DataFrame()

        if name[0] == name[1] == "~ end":
            break


@contextmanager
def discard_on_error(ts: "TimeSeries"):
    """Context manager to discard changes to `ts` and close the DB on any exception.

    For :class:`.JDBCBackend`, this can avoid leaving `ts` in a "locked" state in the
    database.

    Examples
    --------
    >>> mp = ixmp.Platform()
    >>> s = ixmp.Scenario(mp, ...)
    >>> with discard_on_error(s):
    ...     s.add_par(...)  # Any code
    ...     s.not_a_method()  # Code that raises some exception

    Before the the exception in the final line is raised (and possibly handled by
    surrounding code):

    - Any changes—for example, here changes due to the call to :meth:`.add_par`—are
      discarded/not committed;
    - ``s`` is guaranteed to be in a non-locked state; and
    - :meth:`.close_db` is called on ``mp``.
    """
    mp = ts.platform
    try:
        yield
    except Exception as e:
        log.info(
            f"Avoid locking {ts!r} before raising {e.__class__.__name__}: "
            + str(e).splitlines()[0].strip('"')
        )

        try:
            ts.discard_changes()
        except Exception:  # pragma: no cover
            pass  # Some exception trying to discard changes()
        else:
            log.info(f"Discard {ts.__class__.__name__.lower()} changes")

        mp.close_db()
        log.info("Close database connection")

        raise


def maybe_check_out(timeseries, state=None):
    """Check out `timeseries` depending on `state`.

    If `state` is :obj:`None`, then :meth:`.TimeSeries.check_out` is called.

    Returns
    -------
    :obj:`True`
        if `state` was :obj:`None` and a check out was performed, i.e. `timeseries` was
        previously in a checked-in state.
    :obj:`False`
        if `state` was :obj:`None` and no check out was performed, i.e. `timeseries`
        was already in a checked-out state.
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
        # If `timeseries` is new (has not been committed), the checkout attempt raises
        # an exception
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
        Any value returned by :meth:`.Scenario.par`. For a scalar (0-dimensional)
        parameter, this will be :class:`dict`.

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
        Keyword argument 'name' for the :class:`.Platform` constructor.
    scenario_info : dict
        Keyword arguments for a :class:`.Scenario` on the above platform:
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
                raise ValueError(
                    f"URL version must be int or 'new'; got '{components.fragment}'"
                )
            else:
                version = "new"

        scenario_info["version"] = version

    return platform_info, scenario_info


def to_iamc_layout(df: pd.DataFrame) -> pd.DataFrame:
    """Transform `df` to the IAMC structure/layout.

    The returned object has:

    - Any (Multi)Index levels reset as columns.
    - Lower-case column names 'region', 'variable', 'subannual', and 'unit'.
    - If not present in `df`, the value 'Year' in the 'subannual' column.

    Parameters
    ----------
    df : pandas.DataFrame
        May have a 'node' column, which will be renamed to 'region'.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
        If 'region', 'variable', or 'unit' is not among the column names.
    """
    # Reset the index if meaningful entries are included there
    if not list(df.index.names) == [None]:
        df.reset_index(inplace=True)

    # Rename columns in lower case, and transform 'node' to 'region'
    cols = {c: str(c).lower() for c in df.columns}
    cols.update(node="region")
    df = df.rename(columns=cols)

    required_cols = ["region", "variable", "unit"]
    missing = list(set(required_cols) - set(df.columns))
    if len(missing):
        raise ValueError(f"missing required columns {repr(missing)}")

    # Add a column 'subannual' with the default value
    if "subannual" not in df.columns:
        df["subannual"] = "Year"

    return df


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
        :meth:`.TimeSeries.set_as_default`.
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

    # group_keys silences a warning in pandas 1.5.0
    info = (
        platform.scenario_list(model=model, scen=scenario, default=default_only)
        .groupby(["model", "scenario"], group_keys=True)
        .apply(describe, include_groups=False)
    )

    # If we have no results; re-create a minimal empty data frame
    info = (
        info.reset_index()
        if len(info)
        else pd.DataFrame([], columns=["model", "scenario", "default", "N"])
    )

    info["scenario"] = info["scenario"].str.cat(info["default"].astype(str), sep="#")

    if match:
        info = info[info["model"].str.match(match) | info["scenario"].str.match(match)]

    lines = []

    if as_url:
        info["url"] = f"ixmp://{platform.name}"
        urls = info["url"].str.cat([info["model"], info["scenario"]], sep="/")
        lines = urls.tolist()
    else:
        width = 0 if not len(info) else info["scenario"].str.len().max()
        info["scenario"] = info["scenario"].str.ljust(width + 2)

        for model, m_info in info.groupby("model"):
            lines.extend(
                [
                    "",
                    model + "/",
                    "  " + "\n  ".join(m_info["scenario"].str.cat(m_info["range"])),
                ]
            )

        lines.append("")

    # Summary information
    lines.extend(
        []
        if as_url
        else [
            f"{len(info['model'].unique())} model name(s)",
            f"{len(info['scenario'].unique())} scenario name(s)",
            f"{len(info)} (model, scenario) combination(s)",
            f"{info['N'].sum()} total scenarios",
        ]
    )

    return lines


def show_versions(file=sys.stdout, *, packages: Optional[Iterable[str]] = None) -> None:
    """Print information about ixmp and its dependencies to `file`.

    See also
    --------
    SHOW_VERSION_PACKAGES
    """
    from importlib import import_module
    from importlib.metadata import PackageNotFoundError, version

    try:
        from importlib.metadata import packages_distributions
    except ImportError:  # Python 3.9
        from importlib_metadata import packages_distributions  # type: ignore [no-redef]
    from subprocess import DEVNULL, check_output

    from xarray.util.print_versions import get_sys_info

    from ixmp.model.gams import gams_info

    # Retrieve the mapping from package (e.g. 'yaml') to distribution ('PyYAML') names
    package_to_dist = packages_distributions()

    git_log_cmd = ["git", "log", "--oneline", "--no-color", "--decorate", "-n 1"]

    def git_log(spec: "ModuleSpec") -> str:
        """Retrieve Git log information about the module at `spec`."""
        try:
            assert spec.origin is not None
            cwd = Path(spec.origin).parent
            info = check_output(git_log_cmd, cwd=cwd, encoding="utf-8", stderr=DEVNULL)
        except Exception:  # pragma: no cover
            return ""  # Occurs if "git log" fails; or if spec.origin is None (#338)
        else:
            return f"\n{'git:':>18} {info.rstrip()}"

    def module_version(spec: "ModuleSpec") -> str:
        """Get the module version and any exception that occurs when importing it."""
        try:
            # Map from the package name to distribution name
            v = version(package_to_dist.get(spec.name, [spec.name])[0])
        except PackageNotFoundError:
            v = "(not installed)"
        try:
            # Import the module
            sys.modules.get(spec.name, None) or import_module(spec.name)
            exc_info = ""
        except Exception as e:  # pragma: no cover
            exc_info = f"\n    Error on import: {e!r}"

        return f"{v}{exc_info}"

    info: list[tuple[str, str]] = []  # Info lines

    for name in packages or SHOW_VERSION_PACKAGES:
        if name == "":
            info.append(("", ""))
        elif spec := find_spec(name):
            info.append((name, module_version(spec) + git_log(spec)))

            # Additional info line for JPype
            if name == "jpype":
                stat = import_module(name).getDefaultJVMPath()
                info.append((f"{'Java VM path':>17}", stat))
        else:
            # No spec associated with `module_name`
            info.append((name, "(not installed)"))

    # Add GAMS version and system directory
    gi = gams_info()
    info.extend([("GAMS", gi.version), (f"{'system dir':>17}", str(gi.system_dir))])

    # Use xarray to get Python & system information
    info.append(("", ""))
    info.extend(get_sys_info()[1:])  # Exclude the commit number

    # Format and write to `file`
    for k, stat in info:
        print("" if (k == stat == "") else f"{k + ':':18} {stat}", file=file)


def update_par(scenario, name, data):
    """Update parameter *name* in *scenario* using *data*, without overwriting.

    Only values which do not already appear in the parameter data are added.
    """
    tmp = pd.concat([scenario.par(name), data])
    columns = list(filter(lambda c: c != "value", tmp.columns))
    tmp = tmp.drop_duplicates(subset=columns, keep=False)

    if len(tmp):
        scenario.add_par(name, tmp)


class DeprecatedPathFinder(MetaPathFinder):
    """Handle imports from deprecated module locations."""

    map: Mapping[re.Pattern, str]

    def __init__(self, package: str, name_map: Mapping[str, str]):
        # Prepend the package name to the source and destination
        self.map = {
            re.compile(rf"{package}\.{k}"): f"{package}.{v}"
            for k, v in name_map.items()
        }

    @lru_cache(maxsize=128)
    def new_name(self, name):
        # Apply each pattern in self.map successively
        new_name = name
        for pattern, repl in self.map.items():
            new_name = pattern.sub(repl, new_name)

        if name != new_name:
            from warnings import warn

            warn(
                f"Importing from {name!r} is deprecated and will fail in a future "
                f"version. Use {new_name!r}.",
                DeprecationWarning,
                3,
            )

        return new_name

    def find_spec(self, name, path, target=None):
        new_name = self.new_name(name)
        if new_name == name:
            return None  # No known transformation; let the importlib defaults handle.

        # Get an import spec for the module
        spec = find_spec(new_name)
        if not spec:
            return None

        # Create a new spec that loads the module from its current location as if it
        # were `name`
        new_spec = ModuleSpec(
            name=name,
            # Create a new loader that loads from the actual file with the desired name
            loader=SourceFileLoader(fullname=name, path=spec.origin),
            origin=spec.origin,
        )
        # These can't be passed through the constructor
        new_spec.submodule_search_locations = spec.submodule_search_locations

        return new_spec
