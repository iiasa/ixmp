import logging
from collections.abc import Sequence
from contextlib import contextmanager, nullcontext
from os import PathLike
from pathlib import Path
from typing import Any, Literal, Optional, Union
from warnings import warn
from weakref import ProxyType, proxy

import pandas as pd

from ixmp import IAMC_IDX
from ixmp.backend.common import FIELDS, ItemType
from ixmp.core.platform import Platform
from ixmp.util import (
    as_str_list,
    maybe_check_out,
    maybe_commit,
    parse_url,
    to_iamc_layout,
    year_list,
)

log = logging.getLogger(__name__)


class TimeSeries:
    """Collection of data in time series format.

    TimeSeries is the parent/super-class of :class:`.Scenario`.

    Parameters
    ----------
    mp : :class:`Platform`
        ixmp instance in which to store data.
    model : str
        Model name.
    scenario : str
        Scenario name.
    version : int or str, optional
        If omitted and a default version of the (`model`, `scenario`) has been
        designated (see :meth:`set_as_default`), load that version. If :class:`int`,
        load a specific version. If ``'new'``, create a new TimeSeries.
    annotation : str, optional
        A short annotation/comment used when ``version='new'``.
    """

    #: Name of the model associated with the TimeSeries.
    model: str

    #: Name of the scenario associated with the TimeSeries.
    scenario: str

    #: Version of the TimeSeries. Immutable for a specific instance.
    version = None

    def __init__(
        self,
        mp: Platform,
        model: str,
        scenario: str,
        version: Optional[Union[int, str]] = None,
        annotation: Optional[str] = None,
        **kwargs,
    ):
        # Check arguments
        if not isinstance(mp, Platform):
            raise TypeError("mp is not a valid `ixmp.Platform` instance")
        elif version and not (version == "new" or isinstance(version, int)):
            raise ValueError(f"version={repr(version)}")
        elif version == "new" and annotation is None:
            log.info(
                f"Missing annotation for new {self.__class__.__name__}"
                f" {model}/{scenario}"
            )
            annotation = ""

        # scheme= keyword argument only passed from Scenario.__init__; otherwise must be
        # None
        scheme = kwargs.get("scheme", None)
        if scheme:
            if self.__class__ is TimeSeries:
                raise TypeError("'scheme' argument to TimeSeries()")
            else:
                self.scheme = scheme

        # Set attributes
        self.model = model
        self.scenario = scenario

        # Store a weak reference to the Platform object. This reference is not enough
        # to keep the Platform alive, i.e. 'del mp' will work even while this TimeSeries
        # object lives.
        self.platform = mp if isinstance(mp, ProxyType) else proxy(mp)

        if version == "new":
            # Initialize a new object
            self._backend("init", annotation)
        else:
            # Retrieve an existing object
            self.version = version
            self._backend("get")

    def _backend(self, method, *args, **kwargs):
        """Convenience for calling *method* on the backend.

        The weak reference to the Platform object is used, if the Platform is still
        alive.
        """
        return self.platform._backend(self, method, *args, **kwargs)

    def __del__(self):
        # Instruct the back end to free memory associated with the TimeSeries
        try:
            self._backend("del_ts")
        except (AttributeError, ReferenceError):
            pass  # The Platform has already been garbage-collected

    @classmethod
    def from_url(
        cls, url: str, errors: Literal["warn", "raise"] = "warn"
    ) -> tuple[Optional["TimeSeries"], Platform]:
        """Instantiate a TimeSeries (or Scenario) given an ``ixmp://`` URL.

        The following are equivalent::

            from ixmp import Platform, TimeSeries
            mp = Platform(name='example')
            scen = TimeSeries(mp 'model', 'scenario', version=42)

        and::

            from ixmp import TimeSeries
            scen, mp = TimeSeries.from_url('ixmp://example/model/scenario#42')

        Parameters
        ----------
        url : str
            See :meth:`parse_url <ixmp.util.parse_url>`.
        errors : 'warn' or 'raise'
            If 'warn', a failure to load the TimeSeries is logged as a warning, and the
            platform is still returned. If 'raise', the exception is raised.

        Returns
        -------
        tuple
            with 2 elements:

            - The :class:`.TimeSeries` referenced by the `url`.
            - The :class:`.Platform` referenced by the `url`, on which the first element
              is stored.
        """
        assert errors in ("warn", "raise"), "errors= must be 'warn' or 'raise'"

        platform_info, scenario_info = parse_url(url)
        platform = Platform(**platform_info)

        try:
            ts = cls(platform, **scenario_info)
        except Exception as e:
            if errors == "warn":
                # FIXME ixmp4 errors might have empty e.args
                log.warning(
                    f"{e.__class__.__name__}: {e.args[0]}\n"
                    f"when loading {cls.__name__} from url: {repr(url)}"
                )
                return None, platform
            else:
                raise
        else:
            return ts, platform

    # Transactions and versioning

    def has_solution(self) -> bool:
        # Only Scenario class can have a solution
        return False

    def check_out(self, timeseries_only: bool = False) -> None:
        """Check out the TimeSeries.

        Data in the TimeSeries can only be modified when it is in a checked-out state.

        See Also
        --------
        util.maybe_check_out
        """
        self._backend("check_out", timeseries_only)

    def commit(self, comment: str) -> None:
        """Commit all changed data to the database.

        If the TimeSeries was newly created (with ``version='new'``), :attr:`version`
        is updated with a new version number assigned by the backend. Otherwise,
        :meth:`commit` does not change the :attr:`version`.

        Parameters
        ----------
        comment : str
            Description of the changes being committed.

        See Also
        --------
        util.maybe_commit
        """
        self._backend("commit", comment)

    def discard_changes(self) -> None:
        """Discard all changes and reload from the database."""
        self._backend("discard_changes")

    @contextmanager
    def transact(
        self, message: str = "", condition: bool = True, discard_on_error: bool = False
    ):
        """Context manager to wrap code in a 'transaction'.

        Parameters
        ----------
        message : str
            Commit message to use, if any commit is performed.
        condition : bool
            If :obj:`True` (the default):

            - Before entering the code block, the TimeSeries (or :class:`.Scenario`) is
              checked out.
            - On exiting the code block normally (without an exception), changes are
              committed with `message`.

            If :obj:`False`, nothing occurs on entry or exit.
        discard_on_error : bool
            If :obj:`True` (default :obj:`False`), then the anti-locking behaviour of
            :func:`.discard_on_error` also applies to any exception raised in the block.

        Example
        -------
        >>> # `ts` is currently checked in/locked
        >>> with ts.transact(message="replace 'foo' with 'bar' in set x"):
        >>>    # `ts` is checked out and may be modified
        >>>    ts.remove_set("x", "foo")
        >>>    ts.add_set("x", "bar")
        >>> # Changes to `ts` have been committed
        """
        # TODO implement __enter__ and __exit__ to allow simpler "with ts: …"
        from ixmp.util import discard_on_error as discard_on_error_cm

        if condition:
            maybe_check_out(self)
        try:
            # Use the discard_on_error context manager (cm) if the parameter of the same
            # name is True
            with discard_on_error_cm(self) if discard_on_error else nullcontext():
                yield
        finally:
            maybe_commit(self, condition, message)

    def set_as_default(self) -> None:
        """Set the current :attr:`version` as the default."""
        self._backend("set_as_default")

    def is_default(self) -> bool:
        """Return :obj:`True` if the :attr:`version` is the default version."""
        return self._backend("is_default")

    def last_update(self) -> str:
        """Get the timestamp of the last update/edit of this TimeSeries."""
        return self._backend("last_update")

    def run_id(self) -> int:
        """Get the run id of this TimeSeries."""
        return self._backend("run_id")

    @property
    def url(self) -> str:
        """URL fragment for the TimeSeries.

        This has the format ``{model name}/{scenario name}#{version}``, with the same
        values passed when creating the TimeSeries instance.

        Examples
        --------
        To form a complete URL (e.g. to use with :meth:`.from_url`), use a configured
        :class:`.ixmp.Platform` name:

        >>> platform_name = "my-ixmp-platform"
        >>> mp = Platform(platform_name)
        >>> ts = TimeSeries(mp, "foo", "bar", 34)
        >>> ts.url
        "foo/bar#34"
        >>> f"ixmp://{platform_name}/{ts.url}"
        "ixmp://platform_name/foo/bar#34"

        .. note:: Use caution: because Platform configuration is system-specific, other
           systems must have the same configuration for `platform_name` in order for
           the URL to refer to the same TimeSeries/Scenario.
        """
        return f"{self.model}/{self.scenario}#{self.version}"

    # Time series data

    def preload_timeseries(self) -> None:
        """Preload timeseries data to in-memory cache. Useful for bulk updates."""
        self._backend("preload")

    def add_timeseries(
        self,
        df: pd.DataFrame,
        meta: bool = False,
        year_lim: tuple[Optional[int], Optional[int]] = (None, None),
    ) -> None:
        """Add time series data.

        Parameters
        ----------
        df : pandas.DataFrame
            Data to add. `df` must have the following columns:

            - `region` or `node`
            - `variable`
            - `unit`

            Additional column names may be either of:

            - `year` and `value`—long, or 'tabular', format.
            - one or more specific years—wide, or 'IAMC' format.

            To support subannual temporal resolution of timeseries data, a column
            `subannual` is optional in `df`. The entries in this column must have been
            defined in the Platform instance using :meth:`.add_timeslice` beforehand. If
            no column `subannual` is included in `df`, the data is assumed to contain
            yearly values. See :meth:`.timeslices` for a detailed description of the
            feature.

        meta : bool, optional
            If :obj:`True`, store `df` as metadata. Metadata is treated specially when
            :meth:`Scenario.clone` is called for Scenarios created with
            ``scheme='MESSAGE'``.

        year_lim : tuple, optional
            Respectively, earliest and latest years to add from `df`; data for other
            years is ignored.
        """
        meta = bool(meta)

        # Ensure consistent column names
        df = to_iamc_layout(df)

        if "value" in df.columns:
            # Long format; pivot to wide
            all_cols = [i for i in df.columns if i not in ["year", "value"]]
            df = pd.pivot_table(
                df, values="value", index=all_cols, columns=["year"]
            ).reset_index()
        df.set_index(["region", "variable", "unit", "subannual"], inplace=True)

        # Discard non-numeric columns, e.g. 'model', 'scenario', write warning about
        # non-expected cols to log
        year_cols = year_list(df.columns)
        other_cols = [
            i for i in df.columns if i not in ["model", "scenario"] + year_cols
        ]
        if len(other_cols) > 0:
            log.warning(f"Dropped extra column(s) {other_cols} from data")

        df = df.loc[:, year_cols]

        # Columns (year) as integer
        df.columns = df.columns.astype(int)

        # Identify columns to drop
        def predicate(y: Any) -> bool:
            return y < (year_lim[0] or y) or (year_lim[1] or y) < y

        df.drop(list(filter(predicate, df.columns)), axis=1, inplace=True)

        # Add one time series per row
        for key, data in df.iterrows():
            assert isinstance(key, tuple)
            r, v, u, sa = key
            # Values as float; exclude NA
            self._backend(
                "set_data", r, v, data.astype(float).dropna().to_dict(), u, sa, meta
            )

    def timeseries(
        self,
        region: Optional[Union[str, Sequence[str]]] = None,
        variable: Optional[Union[str, Sequence[str]]] = None,
        unit: Optional[Union[str, Sequence[str]]] = None,
        year: Optional[Union[int, Sequence[int]]] = None,
        iamc: bool = False,
        subannual: Union[bool, str] = "auto",
    ) -> pd.DataFrame:
        """Retrieve time series data.

        Parameters
        ----------
        iamc : bool, optional
            Return data in wide/'IAMC' format. If :obj:`False`, return data in long
            format; see :meth:`add_timeseries`.
        region : str or list of str, optional
            Regions to include in returned data.
        variable : str or list of str, optional
            Variables to include in returned data.
        unit : str or list of str, optional
            Units to include in returned data.
        year : int or list of int, optional
            Years to include in returned data.
        subannual : bool or 'auto', optional
            Whether to include column for sub-annual specification (if :class:`bool`);
            if 'auto', include column if sub-annual data (other than 'Year') exists in
            returned data frame.

        Raises
        ------
        ValueError
            If `subannual` is :obj:`False` but Scenario has (filtered) sub-annual data.

        Returns
        -------
        pandas.DataFrame
            Specified data.
        """
        # Retrieve data, convert to pandas.DataFrame
        df = pd.DataFrame(
            self._backend(
                "get_data",
                as_str_list(region) or [],
                as_str_list(variable) or [],
                as_str_list(unit) or [],
                year if isinstance(year, Sequence) else [] if year is None else [year],
            ),
            columns=FIELDS["ts_get"],
        ).assign(model=self.model, scenario=self.scenario)

        # drop `subannual` column if not requested (False) or required ('auto')
        if subannual is not True:
            has_subannual = not all(df["subannual"] == "Year")
            if subannual is False and has_subannual:
                raise ValueError(
                    "timeseries data has subannual values, use `subannual=True or "
                    "'auto'`"
                )
            if not has_subannual:
                df.drop("subannual", axis=1, inplace=True)

        if iamc:
            # Convert to wide format
            index = IAMC_IDX
            if "subannual" in df.columns:
                index = index + ["subannual"]
            df = (
                df.pivot_table(index=index, columns="year")["value"]
                .reset_index()
                .rename_axis(columns=None)
            )

        return df

    def remove_timeseries(self, df: pd.DataFrame) -> None:
        """Remove time series data.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to remove. `df` must have the following columns:

            - `region` or `node`
            - `variable`
            - `unit`
            - `year`
        """
        # Ensure consistent column names
        df = to_iamc_layout(df)

        id_cols = ["region", "variable", "unit", "subannual"]
        if "year" not in df.columns:
            # Reshape from wide to long format
            df = pd.melt(df, id_vars=id_cols, var_name="year", value_name="value")

        # Remove all years for a given (r, v, u) combination at once
        for (r, v, u, t), data in df.groupby(id_cols):
            self._backend("delete", r, v, t, data["year"].tolist(), u)

    # Geodata

    def add_geodata(self, df: pd.DataFrame) -> None:
        """Add geodata.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to add. `df` must have the following columns:

            - `region`
            - `variable`
            - `subannual`
            - `unit`
            - `year`
            - `value`
            - `meta`
        """
        for _, row in df.astype({"year": int, "meta": int}).iterrows():
            self._backend(
                "set_geo",
                row.region,
                row.variable,
                row.subannual,
                row.year,
                row.value,
                row.unit,
                row.meta,
            )

    def remove_geodata(self, df: pd.DataFrame) -> None:
        """Remove geodata from the TimeSeries instance.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to remove. `df` must have the following columns:

            - `region`
            - `variable`
            - `unit`
            - `subannual`
            - `year`
        """
        # Remove all years for a given (r, v, t, u) combination at once
        for (r, v, t, u), data in df.groupby(
            ["region", "variable", "subannual", "unit"]
        ):
            self._backend("delete_geo", r, v, t, data["year"].tolist(), u)

    def get_geodata(self) -> pd.DataFrame:
        """Fetch geodata and return it as dataframe.

        Returns
        -------
        :class:`pandas.DataFrame`
            Specified data.
        """
        # TODO remove astype here; this is the responsibility of Backend
        return (
            pd.DataFrame(self._backend("get_geo"), columns=FIELDS["ts_get_geo"])
            .reset_index(drop=True)
            .astype({"meta": "int64", "year": "int64"})
        )

    # Metadata

    def get_meta(self, name: Optional[str] = None):
        """Get :ref:`data-meta` for this object.

        Metadata with the given `name`, attached to this (:attr:`model` name,
        :attr:`scenario` name, :attr:`version`), is retrieved.

        Parameters
        ----------
        name : str, optional
            Metadata name/identifier.
        """
        all_meta = self.platform._backend.get_meta(
            self.model, self.scenario, self.version
        )
        return all_meta[name] if name else all_meta

    def set_meta(self, name_or_dict: Union[str, dict[str, Any]], value=None) -> None:
        """Set :ref:`data-meta` for this object.

        Parameters
        ----------
        name_or_dict : str or dict
            If :class:`dict`, a mapping of names/identifiers to values. Otherwise,
            use the metadata identifier.
        value : str or float or int or bool, optional
            Metadata value.
        """
        if isinstance(name_or_dict, str):
            name_or_dict = {name_or_dict: value}
        elif not isinstance(name_or_dict, dict):
            raise TypeError(
                f"name_or_dict must be str or dict; got {type(name_or_dict)}"
            )
        self.platform._backend.set_meta(
            name_or_dict, self.model, self.scenario, self.version
        )

    def delete_meta(self, *args, **kwargs) -> None:
        """Remove :ref:`data-meta` for this object.

        .. deprecated:: 3.1

           Use :meth:`.remove_meta`.

        Parameters
        ----------
        name : str or list of str
            Either single metadata name/identifier, or list of names.
        """
        warn("TimeSeries.delete_meta(); use remove_meta()", DeprecationWarning)
        self.remove_meta(*args, **kwargs)

    def remove_meta(self, name: Union[str, Sequence[str]]) -> None:
        """Remove :ref:`data-meta` for this object.

        Parameters
        ----------
        name : str or list of str
            Either single metadata name/identifier, or list of names.
        """
        self.platform._backend.remove_meta(
            as_str_list(name), self.model, self.scenario, self.version
        )

    # File I/O

    def read_file(
        self,
        path: PathLike,
        firstyear: Optional[int] = None,
        lastyear: Optional[int] = None,
    ) -> None:
        """Read time series data from a CSV or Microsoft Excel file.

        Parameters
        ----------
        path : os.PathLike
            File to read. Must have suffix '.csv' or '.xlsx'.
        firstyear : int, optional
            Only read data from years equal to or later than this year.
        lastyear : int, optional
            Only read data from years equal to or earlier than this year.

        See also
        --------
        .Scenario.read_excel
        """
        self.platform._backend.read_file(
            Path(path),
            ItemType.TS,
            filters=dict(scenario=self),
            firstyear=firstyear,
            lastyear=lastyear,
        )
