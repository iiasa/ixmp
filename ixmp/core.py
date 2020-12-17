import logging
from functools import partial
from itertools import repeat, zip_longest
from pathlib import Path
from typing import List, Union
from warnings import warn
from weakref import ProxyType, proxy

import numpy as np
import pandas as pd

from ._config import config
from .backend import BACKENDS, FIELDS, ItemType
from .model import get_model
from .utils import as_str_list, check_year, logger, parse_url, year_list

log = logging.getLogger(__name__)

# %% default settings for column headers

IAMC_IDX: List[Union[str, int]] = ["model", "scenario", "region", "variable", "unit"]


class Platform:
    """Instance of the modeling platform.

    A Platform connects two key components:

    1. A **back end** for storing data such as model inputs and outputs.
    2. One or more **model(s)**; codes in Python or other languages or
       frameworks that run, via :meth:`Scenario.solve`, on the data stored in
       the Platform.

    The Platform parameters control these components. :class:`TimeSeries` and
    :class:`Scenario` objects tied to a single Platform; to move data between
    platforms, see :meth:`Scenario.clone`.

    Parameters
    ----------
    name : str
        Name of a specific :ref:`configured <configuration>` backend.
    backend : 'jdbc'
        Storage backend type. 'jdbc' corresponds to the built-in
        :class:`.JDBCBackend`; see :obj:`.BACKENDS`.
    backend_args
        Keyword arguments to specific to the `backend`. See
        :class:`.JDBCBackend`.
    """

    # Storage back end for the platform
    _backend = None

    # List of method names which are handled directly by the backend
    _backend_direct = [
        "add_model_name",
        "add_scenario_name",
        "close_db",
        "get_doc",
        "get_meta",
        "get_model_names",
        "get_scenario_names",
        "open_db",
        "remove_meta",
        "set_doc",
        "set_meta",
    ]

    def __init__(self, name=None, backend=None, **backend_args):
        if name is None:
            if backend is None and not len(backend_args):
                # No arguments given: use the default platform config
                name = "default"
            elif backend is None:
                # Only backend_args given
                log.info("Using default JDBC backend")
                kwargs = {"class": "jdbc"}
            else:
                # Backend and maybe backend_args were given
                kwargs = {"class": backend}

        if name:
            # Using a named platform config; retrieve it
            self.name, kwargs = config.get_platform_info(name)

        # Overwrite any platform config with explicit keyword arguments
        kwargs.update(backend_args)

        # Retrieve the Backend class
        try:
            backend_class = kwargs.pop("class")
            backend_class = BACKENDS[backend_class]
        except KeyError:
            raise ValueError(
                f"backend class {repr(backend_class)} not among "
                + str(sorted(BACKENDS.keys()))
            )

        # Instantiate the backend
        self._backend = backend_class(**kwargs)

    def __getattr__(self, name):
        """Convenience for methods of Backend."""
        if name in self._backend_direct:
            return getattr(self._backend, name)
        else:
            raise AttributeError(name)

    def set_log_level(self, level):
        """Set log level for the Platform and its storage :class:`.Backend`.

        Parameters
        ----------
        level : str
            Name of a :py:ref:`Python logging level <levels>`.
        """
        if not (level in dir(logging) or isinstance(level, int)):
            raise ValueError(
                f"{repr(level)} is not a valid Python logger level, see "
                "https://docs.python.org/3/library/logging.html#logging-level"
            )

        # Set the level for the 'ixmp' logger
        # NB this may produce unexpected results when multiple Platforms exist
        #    and different log levels are set. To fix, could use a sub-logger
        #    per Platform instance.
        logging.getLogger("ixmp").setLevel(level)

        # Set the level for the 'ixmp.backend.*' logger. For JDBCBackend, this
        # also has the effect of setting the level for Java log messages that
        # are printed to stdout.
        self._backend.set_log_level(level)

    def get_log_level(self):
        """Return log level of the storage :class:`.Backend`, if any.

        Returns
        -------
        str
            Name of a :py:ref:`Python logging level <levels>`.
        """
        return self._backend.get_log_level()

    def scenario_list(self, default=True, model=None, scen=None):
        """Return information about TimeSeries and Scenarios on the Platform.

        Parameters
        ----------
        default : bool, optional
            Return *only* the default version of each TimeSeries/Scenario (see
            :meth:`TimeSeries.set_as_default`). Any (`model`, `scenario`)
            without a default version is omitted. If :obj:`False`, return all
            versions.
        model : str, optional
            A model name. If given, only return information for *model*.
        scen : str, optional
            A scenario name. If given, only return information for *scen*.

        Returns
        -------
        :class:`pandas.DataFrame`
            Scenario information, with the columns:

            - ``model``, ``scenario``, ``version``, and ``scheme``—Scenario
              identifiers; see :class:`Scenario`.
            - ``is_default``—:obj:`True` if the ``version`` is the default
              version for the (``model``, ``scenario``).
            - ``is_locked``—:obj:`True` if the Scenario has been locked for
              use.
            - ``cre_user`` and ``cre_date``—database user that created the
              Scenario, and creation time.
            - ``upd_user`` and ``upd_date``—user and time for last modification
              of the Scenario.
            - ``lock_user`` and ``lock_date``—user that locked the Scenario and
              lock time.
            - ``annotation``: description of the Scenario or changelog.
        """
        return pd.DataFrame(
            self._backend.get_scenarios(default, model, scen),
            columns=FIELDS["get_scenarios"],
        )

    def export_timeseries_data(
        self,
        path,
        default=True,
        model=None,
        scenario=None,
        variable=None,
        unit=None,
        region=None,
        export_all_runs=False,
    ):
        """Export timeseries data to CSV file across multiple scenarios.

        Refer :meth:`.add_timeseries` of :class:`Timeseries` to get more
        information about adding timeseries data to scenario.

        Parameters
        ----------
        path : os.PathLike
            File name to export data to; must have the suffix '.csv'.

            Result file will contain the following columns:

            - model
            - scenario
            - version
            - variable
            - unit
            - region
            - meta
            - subannual
            - year
            - value
        default : bool, optional
            :obj:`True` to include only TimeSeries versions marked as default.
        model: str, optional
            Only return data for this model name.
        scenario: str, optional
            Only return data for this scenario name.
        variable: list of str, optional
            Only return data for variable name(s) in this list.
        unit: list of str, optional
            Only return data for unit name(s) in this list.
        region: list of str, optional
            Only return data for region(s) in this list.
        export_all_runs: boolean, optional
            Export all existing model+scenario run combinations.


        Returns
        -------
        None
        """
        if export_all_runs and (model or scenario):
            raise ValueError(
                "Invalid arguments: "
                "export_all_runs cannot be used when providing "
                "a model or scenario."
            )
        filters = {
            "model": as_str_list(model) or [],
            "scenario": as_str_list(scenario) or [],
            "variable": as_str_list(variable) or [],
            "unit": as_str_list(unit) or [],
            "region": as_str_list(region) or [],
            "default": default,
            "export_all_runs": export_all_runs,
        }

        self._backend.write_file(path, ItemType.TS, filters=filters)

    def add_unit(self, unit, comment="None"):
        """Define a unit.

        Parameters
        ----------
        unit : str
            Name of the unit.
        comment : str, optional
            Annotation describing the unit or why it was added. The current
            database user and timestamp are appended automatically.
        """
        if unit in self.units():
            msg = "unit `{}` is already defined in the platform instance"
            logger().info(msg.format(unit))
            return

        self._backend.set_unit(unit, comment)

    def units(self):
        """Return all units defined on the Platform.

        Returns
        -------
        numpy.ndarray of str
        """
        return self._backend.get_units()

    def regions(self):
        """Return all regions defined for the IAMC-style timeseries format
        including known synonyms.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        return pd.DataFrame(self._backend.get_nodes(), columns=FIELDS["get_nodes"])

    def _existing_node(self, name):
        """Check whether the node *name* has been defined.

        If :obj:`True`, log a warning and return True. Otherwise, return False.
        """
        for _, r in self.regions().iterrows():
            if r.region != name:
                continue

            log.warning(
                f"region {repr(name)} is already defined on the Platform"
                + (f" as a synonym for {repr(r.mapped_to)}" if r.mapped_to else "")
                + (f" under parent {repr(r.parent)}" if r.parent else "")
            )
            return True

        return False

    def add_region(self, region, hierarchy, parent="World"):
        """Define a region including a hierarchy level and a 'parent' region.

        .. tip::
           On a :class:`Platform` backed by a shared database, a region may
           already exist with a different spelling. Use :meth:`regions` first
           to check, and consider calling :meth:`add_region_synonym` instead.

        Parameters
        ----------
        region : str
            Name of the region.
        parent : str, default 'World'
            Assign a 'parent' region.
        hierarchy : str
            Hierarchy level of the region (e.g., country, R11, basin)
        """
        if not self._existing_node(region):
            self._backend.set_node(region, parent, hierarchy)

    def add_region_synonym(self, region, mapped_to):
        """Define a synonym for a `region`.

        When adding timeseries data using the synonym in the region column, it
        will be converted to `mapped_to`.

        Parameters
        ----------
        region : str
            Name of the region synonym.
        mapped_to : str
            Name of the region to which the synonym should be mapped.
        """
        if not self._existing_node(region):
            self._backend.set_node(region, synonym=mapped_to)

    def timeslices(self):
        """Return all subannual timeslices defined in this Platform instance.

        Timeslices are a way to represent subannual temporal resolution in
        timeseries data. A timeslice consists of a **name** (e.g., 'january',
        'summer'), a **category** (e.g., 'months', 'seasons'), and a
        **duration** given relative to a full year.

        The category and duration do not have any functional relevance within
        the ixmp framework, but they may be useful for pre- or post-processing.
        For example, they can be used to filter all timeslices of a certain
        category (e.g., all months) from the :class:`pandas.DataFrame` returned
        by this function or to aggregate subannual data to full-year results.

        A timeslice is related to the index set 'time'
        in a :class:`message_ix.Scenario` to indicate a subannual temporal
        dimension. Alas, timeslices and set elements of time have to be
        initialized/defined independently.

        See :meth:`add_timeslice` to initialize additional timeslices in the
        Platform instance.

        Returns
        -------
        :class:`pandas.DataFrame`
            DataFrame of timeslices, categories and duration
        """
        return pd.DataFrame(
            self._backend.get_timeslices(), columns=FIELDS["get_timeslices"]
        )

    def add_timeslice(self, name, category, duration):
        """Define a subannual timeslice including a category and duration.

        See :meth:`timeslices` for a detailed description of timeslices.

        Parameters
        ----------
        name : str
            Unique name of the timeslice.
        category : str
            Timeslice category (e.g. 'common', 'month', etc).
        duration : float
            Duration of timeslice as fraction of year.
        """
        slices = self.timeslices().set_index("name")
        if name in slices.index:
            msg = "timeslice `{}` already defined with duration {}"
            existing_duration = slices.loc[name].duration
            if not np.isclose(duration, existing_duration):
                raise ValueError(msg.format(name, existing_duration))
            logger().info(msg.format(name, duration))
        else:
            self._backend.set_timeslice(name, category, duration)

    def check_access(self, user, models, access="view"):
        """Check access to specific models.

        Parameters
        ----------
        user: str
            Registered user name
        models : str or list of str
            Model(s) name
        access : str, optional
            Access type - view or edit

        Returns
        -------
        bool or dict of bool
        """
        models_list = as_str_list(models)
        result = self._backend.get_auth(user, models_list, access)
        if isinstance(models, str):
            return result[models]
        else:
            return {model: result.get(model) == 1 for model in models_list}


class TimeSeries:
    """Collection of data in time series format.

    TimeSeries is the parent/super-class of :class:`Scenario`.

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
        designated (see :meth:`set_as_default`), load that version.
        If :class:`int`, load a specific version.
        If ``'new'``, create a new TimeSeries.
    annotation : str, optional
        A short annotation/comment used when ``version='new'``.
    """

    #: Name of the model associated with the TimeSeries
    model = None

    #: Name of the scenario associated with the TimeSeries
    scenario = None

    #: Version of the TimeSeries. Immutable for a specific instance.
    version = None

    def __init__(self, mp, model, scenario, version=None, annotation=None, **kwargs):
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

        # scheme= keyword argument only passed from Scenario.__init__;
        # otherwise must be None
        scheme = kwargs.get("scheme", None)
        if scheme:
            if self.__class__ is TimeSeries:
                raise TypeError("'scheme' argument to TimeSeries()")
            else:
                self.scheme = scheme

        # Set attributes
        self.model = model
        self.scenario = scenario

        # Store a weak reference to the Platform object. This reference is not
        # enough to keep the Platform alive, i.e. 'del mp' will work even while
        # this TimeSeries object lives.
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

        The weak reference to the Platform object is used, if the Platform is
        still alive.
        """
        return self.platform._backend(self, method, *args, **kwargs)

    def __del__(self):
        # Instruct the back end to free memory associated with the TimeSeries
        try:
            self._backend("del_ts")
        except ReferenceError:
            pass  # The Platform has already been garbage-collected

    # Transactions and versioning
    # functions for platform management
    def has_solution(self):
        # only Scenario class can have a solution
        return False

    def check_out(self, timeseries_only=False):
        """Check out the TimeSeries.

        Data in the TimeSeries can only be modified when it is in a checked-out
        state.

        See Also
        --------
        utils.maybe_check_out
        """
        self._backend("check_out", timeseries_only)

    def commit(self, comment):
        """Commit all changed data to the database.

        If the TimeSeries was newly created (with ``version='new'``),
        :attr:`version` is updated with a new version number assigned by the
        backend. Otherwise, :meth:`commit` does not change the :attr:`version`.

        Parameters
        ----------
        comment : str
            Description of the changes being committed.

        See Also
        --------
        utils.maybe_commit
        """
        self._backend("commit", comment)

    def discard_changes(self):
        """Discard all changes and reload from the database."""
        self._backend("discard_changes")

    def set_as_default(self):
        """Set the current :attr:`version` as the default."""
        self._backend("set_as_default")

    def is_default(self):
        """Return :obj:`True` if the :attr:`version` is the default version."""
        return self._backend("is_default")

    def last_update(self):
        """Get the timestamp of the last update/edit of this TimeSeries."""
        return self._backend("last_update")

    def run_id(self):
        """Get the run id of this TimeSeries."""
        return self._backend("run_id")

    # functions for importing and retrieving timeseries data

    def preload_timeseries(self):
        """Preload timeseries data to in-memory cache. Useful for bulk updates."""
        self._backend("preload")

    def add_timeseries(self, df, meta=False, year_lim=(None, None)):
        """Add data to the TimeSeries.

        Parameters
        ----------
        df : :class:`pandas.DataFrame`
            Data to add. `df` must have the following columns:

            - `region` or `node`
            - `variable`
            - `unit`

            Additional column names may be either of:

            - `year` and `value`—long, or 'tabular', format.
            - one or more specific years—wide, or 'IAMC' format.

            To support subannual temporal resolution of timeseries data, a
            column `subannual` is optional in `df`. The entries in this column
            must have been defined in the Platform instance using
            :meth:`add_timeslice` beforehand. If no column `subannual` is
            included in `df`, the data is assumed to contain yearly values.
            See :meth:`timeslices` for a detailed description of the feature.

        meta : bool, optional
            If :obj:`True`, store `df` as metadata. Metadata is treated
            specially when :meth:`Scenario.clone` is called for Scenarios
            created with ``scheme='MESSAGE'``.

        year_lim : tuple of (int or None, int or None`), optional
            Respectively, minimum and maximum years to add from *df*; data for
            other years is ignored.
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

        # Discard non-numeric columns, e.g. 'model', 'scenario',
        # write warning about non-expected cols to log
        year_cols = year_list(df.columns)
        other_cols = [
            i for i in df.columns if i not in ["model", "scenario"] + year_cols
        ]
        if len(other_cols) > 0:
            logger().warning(f"dropping index columns {other_cols} from data")

        df = df.loc[:, year_cols]

        # Columns (year) as integer
        df.columns = df.columns.astype(int)

        # Identify columns to drop
        to_drop = set()
        if year_lim[0]:
            to_drop |= set(filter(lambda y: y < year_lim[0], df.columns))
        if year_lim[1]:
            to_drop |= set(filter(lambda y: y > year_lim[1], df.columns))

        df.drop(to_drop, axis=1, inplace=True)

        # Add one time series per row
        for (r, v, u, sa), data in df.iterrows():
            # Values as float; exclude NA
            self._backend("set_data", r, v, data.astype(float).dropna(), u, sa, meta)

    def timeseries(
        self,
        region=None,
        variable=None,
        unit=None,
        year=None,
        iamc=False,
        subannual="auto",
    ):
        """Retrieve timeseries data.

        Parameters
        ----------
        iamc : bool, optional
            Return data in wide/'IAMC' format. If :obj:`False`, return data in
            long/'tabular' format; see :meth:`add_timeseries`.
        region : str or list of str, optional
            Regions to include in returned data.
        variable : str or list of str, optional
            Variables to include in returned data.
        unit : str or list of str, optional
            Units to include in returned data.
        year : str or int or list of (str or int), optional
            Years to include in returned data.
        subannual : bool or 'auto', optional
            Whether to include column for sub-annual specification (if
            :class:`bool`); if 'auto', include column if sub-annual data (other
            than 'Year') exists in returned dataframe.

        Raises
        ------
        ValueError
            If `subannual` is :obj:`False` but Scenario has (filtered)
            sub-annual data.

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
                as_str_list(year) or [],
            ),
            columns=FIELDS["ts_get"],
        )
        df["model"] = self.model
        df["scenario"] = self.scenario

        # drop `subannual` column if not requested (False) or required ('auto')
        if subannual is not True:
            has_subannual = not all(df["subannual"] == "Year")
            if subannual is False and has_subannual:
                msg = (
                    "timeseries data has subannual values, ",
                    "use `subannual=True or 'auto'`",
                )
                raise ValueError(msg)
            if not has_subannual:
                df.drop("subannual", axis=1, inplace=True)

        if iamc:
            # Convert to wide format
            index = IAMC_IDX
            if "subannual" in df.columns:
                index = index + ["subannual"]
            df = df.pivot_table(index=index, columns="year")["value"].reset_index()
            df.columns.names = [None]

        return df

    def remove_timeseries(self, df):
        """Remove timeseries data from the TimeSeries instance.

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

    def add_geodata(self, df):
        """Add geodata (layers) to the TimeSeries.

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

    def remove_geodata(self, df):
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

    def get_geodata(self):
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

    def read_file(self, path, firstyear=None, lastyear=None):
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


class Scenario(TimeSeries):
    """Collection of model-related data.

    See :class:`TimeSeries` for the meaning of parameters `mp`, `model`,
    `scenario`, `version`, and `annotation`.

    Parameters
    ----------
    scheme : str, optional
        Use an explicit scheme to initialize the new scenario. The
        :meth:`~.base.Model.initialize` method of the corresponding
        :class:`.Model` subclass in :data:`.MODELS` is used to initialize items
        in the Scenario.
    """

    #: Scheme of the Scenario.
    scheme = None

    def __init__(
        self,
        mp,
        model,
        scenario,
        version=None,
        scheme=None,
        annotation=None,
        **model_init_args,
    ):
        # Check arguments
        if version == "new" and scheme is None:
            log.info(f"No scheme for new Scenario {model}/{scenario}")
            scheme = ""

        if "cache" in model_init_args:
            warn(
                "Scenario(..., cache=...) is deprecated; use Platform(..., "
                "cache=...) instead",
                DeprecationWarning,
            )
            model_init_args.pop("cache")

        # Call the parent constructor
        super().__init__(
            mp=mp,
            model=model,
            scenario=scenario,
            version=version,
            scheme=scheme,
            annotation=annotation,
        )

        if self.scheme == "MESSAGE" and self.__class__ is Scenario:
            # Loaded scenario has an improper scheme
            raise RuntimeError(
                f"{model}/{scenario} is a MESSAGE-scheme "
                "scenario; use message_ix.Scenario()."
            )

        # Retrieve the Model class correlating to the *scheme*
        model_class = get_model(self.scheme).__class__

        # Use the model class to initialize the Scenario
        model_class.initialize(self, **model_init_args)

    @classmethod
    def from_url(cls, url, errors="warn"):
        """Instantiate a Scenario given an ixmp-scheme URL.

        The following are equivalent::

            from ixmp import Platform, Scenario
            mp = Platform(name='example')
            scen = Scenario(mp 'model', 'scenario', version=42)

        and::

            from ixmp import Scenario
            scen, mp = Scenario.from_url('ixmp://example/model/scenario#42')

        Parameters
        ----------
        url : str
            See :meth:`parse_url <ixmp.utils.parse_url>`.
        errors : 'warn' or 'raise'
            If 'warn', a failure to load the Scenario is logged as a warning,
            and the platform is still returned. If 'raise', the exception
            is raised.

        Returns
        -------
        scenario, platform : 2-tuple of (Scenario, :class:`Platform`)
            The Scenario and Platform referred to by the URL.
        """
        assert errors in ("warn", "raise"), "errors= must be 'warn' or 'raise'"

        platform_info, scenario_info = parse_url(url)
        platform = Platform(**platform_info)

        try:
            scenario = cls(platform, **scenario_info)
        except Exception as e:
            if errors == "warn":
                log.warning(
                    f"{e.__class__.__name__}: {e.args[0]}\n"
                    f"when loading Scenario from url: {repr(url)}"
                )
                return None, platform
            else:
                raise
        else:
            return scenario, platform

    def check_out(self, timeseries_only=False):
        """Check out the Scenario.

        Raises
        ------
        ValueError
            If :meth:`has_solution` is :obj:`True`.

        See Also
        --------
        TimeSeries.check_out
        utils.maybe_check_out
        """
        if not timeseries_only and self.has_solution():
            raise ValueError(
                "This Scenario has a solution, "
                "use `Scenario.remove_solution()` or "
                "`Scenario.clone(..., keep_solution=False)`"
            )
        super().check_out(timeseries_only)

    def load_scenario_data(self):
        """Load all Scenario data into memory.

        Raises
        ------
        ValueError
            If the Scenario was instantiated with ``cache=False``.
        """
        if not getattr(self.platform._backend, "cache_enabled", False):
            raise ValueError("Cache must be enabled to load scenario data")

        for ix_type in "equ", "par", "set", "var":
            logger().info("Caching {} data".format(ix_type))
            get_func = getattr(self, ix_type)
            for name in getattr(self, "{}_list".format(ix_type))():
                get_func(name)

    def idx_sets(self, name):
        """Return the list of index sets for an item (set, par, var, equ).

        Parameters
        ----------
        name : str
            name of the item
        """
        return self._backend("item_index", name, "sets")

    def idx_names(self, name):
        """Return the list of index names for an item (set, par, var, equ).

        Parameters
        ----------
        name : str
            name of the item
        """
        return self._backend("item_index", name, "names")

    def _keys(self, name, key_or_keys):
        if isinstance(key_or_keys, (list, pd.Series)):
            return as_str_list(key_or_keys)
        elif isinstance(key_or_keys, (pd.DataFrame, dict)):
            if isinstance(key_or_keys, dict):
                key_or_keys = pd.DataFrame.from_dict(key_or_keys, orient="columns")
            idx_names = self.idx_names(name)
            return [as_str_list(row, idx_names) for _, row in key_or_keys.iterrows()]
        else:
            return [str(key_or_keys)]

    def set_list(self):
        """List all defined sets."""
        return self._backend("list_items", "set")

    def has_set(self, name):
        """Check whether the scenario has a set *name*."""
        return name in self.set_list()

    def init_set(self, name, idx_sets=None, idx_names=None):
        """Initialize a new set.

        Parameters
        ----------
        name : str
            Name of the set.
        idx_sets : sequence of str or str, optional
            Names of other sets that index this set.
        idx_names : sequence of str or str, optional
            Names of the dimensions indexed by `idx_sets`.

        Raises
        ------
        ValueError
            If the set (or another object with the same *name*) already exists.
        RuntimeError
            If the Scenario is not checked out (see
            :meth:`~TimeSeries.check_out`).
        """
        idx_sets = as_str_list(idx_sets) or []
        idx_names = as_str_list(idx_names)
        return self._backend("init_item", "set", name, idx_sets, idx_names)

    def set(self, name, filters=None, **kwargs):
        """Return the (filtered) elements of a set.

        Parameters
        ----------
        name : str
            Name of the set.
        filters : dict
            Mapping of `dimension_name` → `elements`, where `dimension_name`
            is one of the `idx_names` given when the set was initialized (see
            :meth:`init_set`), and `elements` is an iterable of labels to
            include in the return value.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        return self._backend("item_get_elements", "set", name, filters)

    def add_set(self, name, key, comment=None):
        """Add elements to an existing set.

        Parameters
        ----------
        name : str
            Name of the set.
        key : str or iterable of str or dict or :class:`pandas.DataFrame`
            Element(s) to be added. If *name* exists, the elements are
            appended to existing elements.
        comment : str or iterable of str, optional
            Comment describing the element(s). If given, there must be the
            same number of comments as elements.

        Raises
        ------
        KeyError
            If the set *name* does not exist. :meth:`init_set` must be called
            before :meth:`add_set`.
        ValueError
            For invalid forms or combinations of *key* and *comment*.
        """
        # TODO expand docstring (here or in doc/source/api.rst) with examples,
        #      per test_core.test_add_set.

        if isinstance(key, list) and len(key) == 0:
            return  # No elements to add

        # Get index names for set *name*, may raise KeyError
        idx_names = self.idx_names(name)

        # Check arguments and convert to two lists: keys and comments
        if len(idx_names) == 0:
            # Basic set. Keys must be strings.
            if isinstance(key, (dict, pd.DataFrame)):
                raise ValueError(
                    "dict, DataFrame keys invalid for basic set {repr(name)}"
                )

            # Ensure keys is a list of str
            keys = as_str_list(key)
        else:
            # Set defined over 1+ other sets

            # Check for ambiguous arguments
            if comment and isinstance(key, (dict, pd.DataFrame)) and "comment" in key:
                raise ValueError("ambiguous; both key['comment'] and comment " "given")

            if isinstance(key, pd.DataFrame):
                # DataFrame of key values and perhaps comments
                try:
                    # Pop a 'comment' column off the DataFrame, convert to list
                    comment = key.pop("comment").to_list()
                except KeyError:
                    pass

                # Convert key to list of list of key values
                keys = []
                for row in key.to_dict(orient="records"):
                    keys.append(as_str_list(row, idx_names=idx_names))
            elif isinstance(key, dict):
                # Dict of lists of key values

                # Pop a 'comment' list from the dict
                comment = key.pop("comment", None)

                # Convert to list of list of key values
                keys = list(map(as_str_list, zip(*[key[i] for i in idx_names])))
            elif isinstance(key[0], str):
                # List of key values; wrap
                keys = [as_str_list(key)]
            elif isinstance(key[0], list):
                # List of lists of key values; convert to list of list of str
                keys = list(map(as_str_list, key))
            elif isinstance(key, str) and len(idx_names) == 1:
                # Bare key given for a 1D set; wrap for convenience
                keys = [[key]]
            else:
                # Other, invalid value
                raise ValueError(key)

        # Process comments to a list of str, or let them all be None
        comments = as_str_list(comment) if comment else repeat(None, len(keys))

        # Combine iterators to tuples. If the lengths are mismatched, the
        # sentinel value 'False' is filled in
        to_add = list(zip_longest(keys, comments, fillvalue=False))

        # Check processed arguments
        for e, c in to_add:
            # Check for sentinel values
            if e is False:
                raise ValueError(f"Comment {repr(c)} without matching key")
            elif c is False:
                raise ValueError(f"Key {repr(e)} without matching comment")
            elif len(idx_names) and len(idx_names) != len(e):
                raise ValueError(
                    f"{len(e)}-D key {repr(e)} invalid for "
                    f"{len(idx_names)}-D set {name}{repr(idx_names)}"
                )

        # Send to backend
        elements = ((kc[0], None, None, kc[1]) for kc in to_add)
        self._backend("item_set_elements", "set", name, elements)

    def remove_set(self, name, key=None):
        """Delete set elements or an entire set.

        Parameters
        ----------
        name : str
            Name of the set to remove (if `key` is :obj:`None`) or from which
            to remove elements.
        key : :class:`pandas.DataFrame` or list of str, optional
            Elements to be removed from set `name`.
        """
        if key is None:
            self._backend("delete_item", "set", name)
        else:
            self._backend("item_delete_elements", "set", name, self._keys(name, key))

    def par_list(self):
        """List all defined parameters."""
        return self._backend("list_items", "par")

    def has_par(self, name):
        """Check whether the scenario has a parameter with that name."""
        return name in self.par_list()

    def init_par(self, name, idx_sets, idx_names=None):
        """Initialize a new parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        idx_sets : sequence of str or str, optional
            Names of sets that index this parameter.
        idx_names : sequence of str or str, optional
            Names of the dimensions indexed by `idx_sets`.
        """
        idx_sets = as_str_list(idx_sets) or []
        idx_names = as_str_list(idx_names)
        return self._backend("init_item", "par", name, idx_sets, idx_names)

    def par(self, name, filters=None, **kwargs):
        """Return parameter data.

        If *filters* is provided, only a subset of data, matching the filters,
        is returned.

        Parameters
        ----------
        name : str
            Name of the parameter
        filters : dict (str -> list of str), optional
            Index names mapped to lists of index set elements. Elements not
            appearing in the respective index set(s) are silently ignored.
        """
        if len(kwargs):
            raise DeprecationWarning(
                "ignored kwargs to Scenario.par(); will raise TypeError in 4.0"
            )
        return self._backend("item_get_elements", "par", name, filters)

    def items(self, type=ItemType.PAR, filters=None):
        """Iterate over model data items.

        Parameters
        ----------
        type : ItemType, optional
            Types of items to iterate, e.g. :data:`ItemType.PAR` for
            parameters, the only value currently supported.
        filters : dict, optional
            Filters for values along dimensions; same as the `filters` argument
            to :meth:`par`.

        Yields
        ------
        (str, object)
            Tuples of item name and data.
        """
        if type != ItemType.PAR:
            raise NotImplementedError(
                f"Scenario.items(type={type}); only ItemType.PAR is supported"
            )

        filters = filters or dict()

        names = sorted(self.par_list())

        for name in sorted(names):
            idx_names = set(self.idx_names(name))
            if len(filters) and not set(filters.keys()) & idx_names:
                # No overlap between the filters and this item's dimensions
                continue

            # Retrieve the data, reducing the filters to only the dimensions of
            # the item
            yield name, self.par(
                name, filters={k: v for k, v in filters.items() if k in idx_names}
            )

    def add_par(
        self,
        name: str,
        key_or_data=None,
        value=None,
        unit: str = None,
        comment: str = None,
    ):
        """Set the values of a parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key_or_data : str or iterable of str or range or dict or
                      :class:`pandas.DataFrame`
            Element(s) to be added.
        value : numeric or iterable of numeric, optional
            Values.
        unit : str or iterable of str, optional
            Unit symbols.
        comment : str or iterable of str, optional
            Comment(s) for the added values.
        """
        # Number of dimensions in the index of *name*
        idx_names = self.idx_names(name)
        N_dim = len(idx_names)

        # Convert valid forms of arguments to pd.DataFrame
        if isinstance(key_or_data, dict):
            # dict containing data
            data = pd.DataFrame.from_dict(key_or_data, orient="columns")
        elif isinstance(key_or_data, pd.DataFrame):
            data = key_or_data.copy()
            if value is not None:
                if "value" in data.columns:
                    raise ValueError("both key_or_data.value and value supplied")
                else:
                    data["value"] = value
        else:
            # One or more keys; convert to a list of strings
            if isinstance(key_or_data, range):
                key_or_data = list(key_or_data)
            keys = self._keys(name, key_or_data)

            # Check the type of value
            if isinstance(value, (float, int)):
                # Single value

                if N_dim > 1 and len(keys) == N_dim:
                    # Ambiguous case: ._key() above returns ['dim_0', 'dim_1'],
                    # when we really want [['dim_0', 'dim_1']]
                    keys = [keys]

                # Use the same value for all keys
                values = [float(value)] * len(keys)
            else:
                # Multiple values
                values = value

            data = pd.DataFrame(zip(keys, values), columns=["key", "value"])
            if data.isna().any(axis=None):
                raise ValueError("Length mismatch between keys and values")

        # Column types
        types = {
            "key": str if N_dim == 1 else object,
            "value": float,
            "unit": str,
            "comment": str,
        }

        # Further handle each column
        if "key" not in data.columns:
            # Form the 'key' column from other columns
            if N_dim > 1 and len(data):
                data["key"] = data.apply(
                    partial(as_str_list, idx_names=idx_names), axis=1
                )
            else:
                data["key"] = data[idx_names[0]]

        if "value" not in data.columns:
            raise ValueError("no parameter values supplied")

        if "unit" not in data.columns:
            # Broadcast single unit across all values. pandas raises ValueError
            # if *unit* is iterable but the wrong length
            data["unit"] = unit or "???"

        if "comment" not in data.columns:
            if comment:
                # Broadcast single comment across all values. pandas raises
                # ValueError if *comment* is iterable but the wrong length
                data["comment"] = comment
            else:
                # Store a 'None' comment
                data["comment"] = None
                types.pop("comment")

        # Convert types, generate tuples
        elements = map(
            lambda e: (e.key, e.value, e.unit, e.comment),
            data.astype(types).itertuples(),
        )

        # Store
        self._backend("item_set_elements", "par", name, elements)

    def init_scalar(self, name, val, unit, comment=None):
        """Initialize a new scalar.

        Parameters
        ----------
        name : str
            Name of the scalar
        val : number
            Initial value of the scalar.
        unit : str
            Unit of the scalar.
        comment : str, optional
            Description of the scalar.
        """
        self.init_par(name, [], [])
        self.change_scalar(name, val, unit, comment)

    def scalar(self, name):
        """Return the value and unit of a scalar.

        Parameters
        ----------
        name : str
            Name of the scalar.

        Returns
        -------
        {'value': value, 'unit': unit}
        """
        return self._backend("item_get_elements", "par", name, None)

    def change_scalar(self, name, val, unit, comment=None):
        """Set the value and unit of a scalar.

        Parameters
        ----------
        name : str
            Name of the scalar.
        val : number
            New value of the scalar.
        unit : str
            New unit of the scalar.
        comment : str, optional
            Description of the change.
        """
        self._backend(
            "item_set_elements", "par", name, [(None, float(val), unit, comment)]
        )

    def remove_par(self, name, key=None):
        """Remove parameter values or an entire parameter.

        Parameters
        ----------
        name : str
            Name of the parameter.
        key : dataframe or key list or concatenated string, optional
            elements to be removed
        """
        if key is None:
            self._backend("delete_item", "par", name)
        else:
            self._backend("item_delete_elements", "par", name, self._keys(name, key))

    def var_list(self):
        """List all defined variables."""
        return self._backend("list_items", "var")

    def has_var(self, name):
        """Check whether the scenario has a variable with that name."""
        return name in self.var_list()

    def init_var(self, name, idx_sets=None, idx_names=None):
        """Initialize a new variable.

        Parameters
        ----------
        name : str
            Name of the variable.
        idx_sets : sequence of str or str, optional
            Name(s) of index sets for a 1+-dimensional variable.
        idx_names : sequence of str or str, optional
            Names of the dimensions indexed by `idx_sets`.
        """
        idx_sets = as_str_list(idx_sets) or []
        idx_names = as_str_list(idx_names)
        return self._backend("init_item", "var", name, idx_sets, idx_names)

    def var(self, name, filters=None, **kwargs):
        """Return a dataframe of (filtered) elements for a specific variable.

        Parameters
        ----------
        name : str
            name of the variable
        filters : dict
            index names mapped list of index set elements
        """
        return self._backend("item_get_elements", "var", name, filters)

    def equ_list(self):
        """List all defined equations."""
        return self._backend("list_items", "equ")

    def init_equ(self, name, idx_sets=None, idx_names=None):
        """Initialize a new equation.

        Parameters
        ----------
        name : str
            Name of the equation.
        idx_sets : sequence of str or str, optional
            Name(s) of index sets for a 1+-dimensional variable.
        idx_names : sequence of str or str, optional
            Names of the dimensions indexed by `idx_sets`.
        """
        idx_sets = as_str_list(idx_sets) or []
        idx_names = as_str_list(idx_names)
        return self._backend("init_item", "equ", name, idx_sets, idx_names)

    def has_equ(self, name):
        """Check whether the scenario has an equation with that name."""
        return name in self.equ_list()

    def equ(self, name, filters=None, **kwargs):
        """Return a dataframe of (filtered) elements for a specific equation.

        Parameters
        ----------
        name : str
            name of the equation
        filters : dict
            index names mapped list of index set elements
        """
        return self._backend("item_get_elements", "equ", name, filters)

    def clone(
        self,
        model=None,
        scenario=None,
        annotation=None,
        keep_solution=True,
        shift_first_model_year=None,
        platform=None,
    ):
        """Clone the current scenario and return the clone.

        If the (`model`, `scenario`) given already exist on the
        :class:`Platform`, the `version` for the cloned Scenario follows the
        last existing version. Otherwise, the `version` for the cloned Scenario
        is 1.

        .. note::
            :meth:`clone` does not set or alter default versions. This means
            that a clone to new (`model`, `scenario`) names has no default
            version, and will not be returned by
            :meth:`Platform.scenario_list` unless `default=False` is given.

        Parameters
        ----------
        model : str, optional
            New model name. If not given, use the existing model name.
        scenario : str, optional
            New scenario name. If not given, use the existing scenario name.
        annotation : str, optional
            Explanatory comment for the clone commit message to the database.
        keep_solution : bool, optional
            If :py:const:`True`, include all timeseries data and the solution
            (vars and equs) from the source scenario in the clone.
            If :py:const:`False`, only include timeseries data marked
            `meta=True` (see :meth:`TimeSeries.add_timeseries`).
        shift_first_model_year: int, optional
            If given, all timeseries data in the Scenario is omitted from the
            clone for years from `first_model_year` onwards. Timeseries data
            with the `meta` flag (see :meth:`TimeSeries.add_timeseries`) are
            cloned for all years.
        platform : :class:`Platform`, optional
            Platform to clone to (default: current platform)
        """
        if shift_first_model_year is not None:
            if keep_solution:
                logger().warning(
                    "Overriding keep_solution=True for " "shift_first_model_year"
                )
                keep_solution = False

        platform = platform or self.platform
        model = model or self.model
        scenario = scenario or self.scenario

        args = [platform, model, scenario, annotation, keep_solution]
        if check_year(shift_first_model_year, "first_model_year"):
            args.append(shift_first_model_year)

        return self._backend("clone", *args)

    def has_solution(self):
        """Return :obj:`True` if the Scenario has been solved.

        If ``has_solution() == True``, model solution data exists in the db.
        """
        return self._backend("has_solution")

    def remove_solution(self, first_model_year=None):
        """Remove the solution from the scenario

        This function removes the solution (variables and equations) and
        timeseries data marked as `meta=False` from the scenario
        (see :meth:`TimeSeries.add_timeseries`).

        Parameters
        ----------
        first_model_year: int, optional
            If given, timeseries data marked as `meta=False` is removed
            only for years from `first_model_year` onwards.

        Raises
        ------
        ValueError
            If Scenario has no solution or if `first_model_year` is not `int`.
        """
        if self.has_solution():
            check_year(first_model_year, "first_model_year")
            self._backend("clear_solution", first_model_year)
        else:
            raise ValueError("This Scenario does not have a solution!")

    def solve(self, model=None, callback=None, cb_kwargs={}, **model_options):
        """Solve the model and store output.

        ixmp 'solves' a model by invoking the run() method of a :class:`.Model`
        subclass—for instance, :meth:`.GAMSModel.run`. Depending on the
        underlying model code, different steps are taken; see each model class
        for details. In general:

        1. Data from the Scenario are written to a **model input file**.
        2. Code or an external program is invoked to perform calculations or
           optimizations, **solving the model**.
        3. Data representing the model outputs or solution are read from a
           **model output file** and stored in the Scenario.

        If the optional argument `callback` is given, then additional steps are
        performed:

        4. Execute the `callback` with the Scenario as an argument. The
           Scenario has an `iteration` attribute that stores the number of
           times the underlying model has been solved (#2).
        5. If the `callback` returns :obj:`False` or similar, iterate by
           repeating from step #1. Otherwise, exit.

        Parameters
        ----------
        model : str
            model (e.g., MESSAGE) or GAMS file name (excluding '.gms')
        callback : callable, optional
            Method to execute arbitrary non-model code. Must accept a single
            argument: the Scenario. Must return a non-:obj:`False` value to
            indicate convergence.
        cb_kwargs : dict, optional
            Keyword arguments to pass to `callback`.
        model_options :
            Keyword arguments specific to the `model`. See :class:`.GAMSModel`.

        Warns
        -----
        UserWarning
            If `callback` is given and returns :obj:`None`. This may indicate
            that the user has forgotten a ``return`` statement, in which case
            the iteration will continue indefinitely.

        Raises
        ------
        ValueError
            If the Scenario has already been solved.
        """
        if self.has_solution():
            raise ValueError(
                "This Scenario has already been solved, ",
                "use `remove_solution()` first!",
            )

        # Instantiate a model
        model = get_model(model or self.scheme, **model_options)

        # Validate *callback* argument
        if callback is not None and not callable(callback):
            raise ValueError(f"callback={repr(callback)} is not callable")
        elif callback is None:
            # Make the callback a no-op
            def callback(scenario, **kwargs):
                return True

        # Flag to warn if the *callback* appears not to return anything
        warn_none = True

        # Iterate until convergence
        while True:
            model.run(self)

            # Store an iteration number to help the callback
            if not hasattr(self, "iteration"):
                self.iteration = 0

            self.iteration += 1

            # Invoke the callback
            cb_result = callback(self, **cb_kwargs)

            if cb_result is None and warn_none:
                warn(
                    "solve(callback=...) argument returned None; will loop "
                    "indefinitely unless True is returned."
                )
                # Don't repeat the warning
                warn_none = False

            if cb_result:
                # Callback indicates convergence is reached
                break

    def get_meta(self, name=None):
        """Get scenario meta.

        Parameters
        ----------
        name : str, optional
            meta category name
        """
        all_meta = self.platform._backend.get_meta(
            self.model, self.scenario, self.version
        )
        return all_meta[name] if name else all_meta

    def set_meta(self, name_or_dict, value=None):
        """Set scenario meta.

        Parameters
        ----------
        name_or_dict : str or dict
            If the argument is dict, it used as a mapping of meta
            categories (names) to values. Otherwise, use the argument
            as the meta category name.
        value : str or number or bool, optional
            Meta category value.
        """
        if not isinstance(name_or_dict, dict):
            if isinstance(name_or_dict, str):
                name_or_dict = {name_or_dict: value}
            else:
                msg = (
                    "Unsupported parameter type of name_or_dict: %s. "
                    "Supported parameter types for name_or_dict are "
                    "String and Dictionary"
                ) % type(name_or_dict)
                raise ValueError(msg)
        self.platform._backend.set_meta(
            name_or_dict, self.model, self.scenario, self.version
        )

    def delete_meta(self, *args, **kwargs):
        """Remove scenario meta.

        .. deprecated:: 3.1

           Use :meth:`remove_meta()`.

        Parameters
        ----------
        name : str or list of str
            Either single meta key or list of keys.
        """
        warn("Scenario.delete_meta(); use remove_meta()", DeprecationWarning)
        self.remove_meta(*args, **kwargs)

    def remove_meta(self, name):
        """Remove scenario meta.

        Parameters
        ----------
        name : str or list of str
            Either single meta key or list of keys.
        """
        if isinstance(name, str):
            name = [name]
        self.platform._backend.remove_meta(
            name, self.model, self.scenario, self.version
        )

    # Input and output
    def to_excel(
        self, path, items=ItemType.SET | ItemType.PAR, filters=None, max_row=None
    ):
        """Write Scenario to a Microsoft Excel file.

        Parameters
        ----------
        path : os.PathLike
            File to write. Must have suffix :file:`.xlsx`.
        items : ItemType, optional
            Types of items to write. Either :attr:`.SET` | :attr:`.PAR` (i.e.
            only sets and parameters), or :attr:`.MODEL` (also variables and
            equations, i.e. model solution data).
        filters : dict, optional
            Filters for values along dimensions; same as the `filters` argument
            to :meth:`par`.
        max_row: int, optional
            Maximum number of rows in each sheet. If the number of elements in
            an item exceeds this number or :data:`.EXCEL_MAX_ROWS`, then an
            item is written to multiple sheets named, e.g. 'foo', 'foo(2)',
            'foo(3)', etc.

        See also
        --------
        :ref:`excel-data-format`
        read_excel
        """
        # Default filters: empty dict
        filters = filters or dict()

        # Select the current scenario
        filters["scenario"] = self

        # Invoke the backend method
        self.platform._backend.write_file(
            Path(path), items, filters=filters, max_row=max_row
        )

    def read_excel(self, path, add_units=False, init_items=False, commit_steps=False):
        """Read a Microsoft Excel file into the Scenario.

        Parameters
        ----------
        path : os.PathLike
            File to read. Must have suffix '.xlsx'.
        add_units : bool, optional
            Add missing units, if any, to the Platform instance.
        init_items : bool, optional
            Initialize sets and parameters that do not already exist in the
            Scenario.
        commit_steps : bool, optional
            Commit changes after every data addition.

        See also
        --------
        :ref:`excel-data-format`
        .TimeSeries.read_file
        to_excel
        """
        self.platform._backend.read_file(
            Path(path),
            ItemType.MODEL,
            filters=dict(scenario=self),
            add_units=add_units,
            init_items=init_items,
            commit_steps=commit_steps,
        )


def to_iamc_layout(df):
    """Transform *df* to a standard IAMC layout.

    The returned object has:

    - Any (Multi)Index levels reset as columns.
    - Lower-case column names 'region', 'variable', 'subannual', and 'unit'.
    - If not present in *df*, the value 'Year' in the 'subannual' column.

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
