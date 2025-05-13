import logging
from collections.abc import Sequence
from os import PathLike
from typing import TYPE_CHECKING, Literal, Optional, Union

import numpy as np
import pandas as pd

from ixmp._config import config
from ixmp.backend.common import FIELDS, ItemType
from ixmp.util import as_str_list
from ixmp.util.ixmp4 import WriteFiltersKwargs

if TYPE_CHECKING:
    from ixmp.backend.base import Backend


log = logging.getLogger(__name__)


class Platform:
    """Instance of the modeling platform.

    A Platform connects two key components:

    1. A **back end** for storing data such as model inputs and outputs.
    2. One or more **model(s)**; codes in Python or other languages or frameworks that
       run, via :meth:`Scenario.solve`, on the data stored in the Platform.

    The Platform parameters control these components. :class:`.TimeSeries` and
    :class:`.Scenario` objects tied to a single Platform; to move data between
    platforms, see :meth:`Scenario.clone`.

    Parameters
    ----------
    name : str
        Name of a specific :ref:`configured <configuration>` backend.
    backend
        Storage backend type. 'jdbc' corresponds to the built-in :class:`.JDBCBackend`;
        see :func:`~.backend.get_class`.
    backend_args
        Keyword arguments to specific to the `backend`. See :class:`.JDBCBackend`.
    """

    # Storage back end for the platform
    _backend: "Backend"

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

    _units_to_warn_about: Optional[list[str]] = None

    def __init__(
        self,
        name: Optional[str] = None,
        backend: Union[Literal["ixmp4", "jdbc"], str, None] = None,
        **backend_args,
    ):
        from ixmp.backend import get_class

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

        # Retrieve a Backend subclass
        backend_class = get_class(kwargs.pop("class"))

        # Instantiate the backend
        self._backend = backend_class(**kwargs)

    def __getattr__(self, name):
        """Convenience for methods of Backend."""
        if name in self._backend_direct:
            return getattr(self._backend, name)
        else:
            raise AttributeError(name)

    def set_log_level(self, level: Union[str, int]) -> None:
        """Set log level for the Platform and its storage :class:`.Backend`.

        Parameters
        ----------
        level : str
            Name of a :py:ref:`Python logging level <levels>`.
        """
        if isinstance(level, str):
            try:
                level = getattr(logging, level)
            except AttributeError:
                pass

        if not isinstance(level, int):
            raise ValueError(
                f"{repr(level)} is not a valid Python logger level, see "
                "https://docs.python.org/3/library/logging.html#logging-level"
            )

        # Set the level for the 'ixmp' logger
        # NB this may produce unexpected results when multiple Platforms exist and
        #    different log levels are set. To fix, could use a sub-logger per Platform
        #    instance.
        logging.getLogger("ixmp").setLevel(level)

        # Set the level for the 'ixmp.backend.*' logger. For JDBCBackend, this also has
        # the effect of setting the level for Java log messages that are printed to
        # stdout.
        self._backend.set_log_level(level)

    def get_log_level(self) -> str:
        """Return log level of the storage :class:`.Backend`, if any.

        Returns
        -------
        str
            Name of a :py:ref:`Python logging level <levels>`.
        """
        return self._backend.get_log_level()

    def scenario_list(
        self,
        default: bool = True,
        model: Optional[str] = None,
        scen: Optional[str] = None,
    ) -> pd.DataFrame:
        """Return information about TimeSeries and Scenarios on the Platform.

        Parameters
        ----------
        default : bool, optional
            Return *only* the default version of each TimeSeries/Scenario (see
            :meth:`TimeSeries.set_as_default`). Any (`model`, `scenario`) without a
            default version is omitted. If :obj:`False`, return all versions.
        model : str, optional
            A model name. If given, only return information for *model*.
        scen : str, optional
            A scenario name. If given, only return information for *scen*.

        Returns
        -------
        :class:`pandas.DataFrame`
            Scenario information, with the columns:

            - ``model``, ``scenario``, ``version``, and ``scheme``—Scenario identifiers;
              see :class:`.TimeSeries` and :class:`.Scenario`.
            - ``is_default``—:obj:`True` if the ``version`` is the default version for
              the (``model``, ``scenario``).
            - ``is_locked``—:obj:`True` if the Scenario has been locked for use.
            - ``cre_user``, ``cre_date``—database user that created the Scenario, and
              creation time.
            - ``upd_user``, ``upd_date``—user and time for last modification of the
              Scenario.
            - ``lock_user``, ``lock_date``—user that locked the Scenario and lock time.
            - ``annotation``: description of the Scenario or changelog.
        """
        return pd.DataFrame(
            self._backend.get_scenarios(default, model, scen),
            columns=FIELDS["get_scenarios"],
        )

    def export_timeseries_data(
        self,
        path: PathLike,
        default: bool = True,
        model: Optional[str] = None,
        scenario: Optional[str] = None,
        variable=None,
        unit=None,
        region=None,
        export_all_runs: bool = False,
    ) -> None:
        """Export time series data to CSV file across multiple :class:`.TimeSeries`.

        Refer :meth:`.TimeSeries.add_timeseries` about adding time series data.

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
        export_all_runs: bool, optional
            Export all existing model+scenario run combinations.
        """
        if export_all_runs and (model or scenario):
            raise ValueError(
                "Invalid arguments: export_all_runs cannot be used when providing a "
                "model or scenario."
            )
        filters = WriteFiltersKwargs(
            scenario=as_str_list(scenario),
            model=as_str_list(model),
            variable=as_str_list(variable),
            unit=as_str_list(unit),
            region=as_str_list(region),
            default=default,
            export_all_runs=export_all_runs,
        )

        self._backend.write_file(path, ItemType.TS, filters=filters)

    def add_unit(self, unit: str, comment: str = "None") -> None:
        """Define a unit.

        Parameters
        ----------
        unit : str
            Name of the unit.
        comment : str, optional
            Annotation describing the unit or why it was added. The current database
            user and timestamp are appended automatically.
        """
        if unit in self.units():
            log.info(f"unit `{unit}` is already defined in the platform instance")
            return

        self._backend.set_unit(unit, comment)

    def units(self) -> list[str]:
        """Return all units defined on the Platform.

        Returns
        -------
        list of str
        """
        return self._backend.get_units()

    def regions(self) -> pd.DataFrame:
        """Return all regions defined time series data, including synonyms.

        Returns
        -------
        :class:`pandas.DataFrame`
        """
        return pd.DataFrame(self._backend.get_nodes(), columns=FIELDS["get_nodes"])

    def _existing_node(self, name):
        """Check whether the node `name` has been defined.

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

    def add_region(self, region: str, hierarchy: str, parent: str = "World") -> None:
        """Define a region including a hierarchy level and a 'parent' region.

        .. tip::
           On a :class:`Platform` backed by a shared database, a region may already
           exist with a different spelling. Use :meth:`regions` first to check, and
           consider calling :meth:`add_region_synonym` instead.

        Parameters
        ----------
        region : str
            Name of the region.
        hierarchy : str
            Hierarchy level of the region (e.g., country, R11, basin)
        parent : str, optional
            Assign a 'parent' region.
        """
        if not self._existing_node(region):
            self._backend.set_node(region, parent, hierarchy)

    def add_region_synonym(self, region: str, mapped_to: str) -> None:
        """Define a synonym for a `region`.

        When adding timeseries data using the synonym in the region column, it will be
        converted to `mapped_to`.

        Parameters
        ----------
        region : str
            Name of the region synonym.
        mapped_to : str
            Name of the region to which the synonym should be mapped.
        """
        if not self._existing_node(region):
            self._backend.set_node(region, synonym=mapped_to)

    def timeslices(self) -> pd.DataFrame:
        """Return all subannual time slices defined in this Platform instance.

        See the :ref:`Data model <data-timeslice>` documentation for further details.

        The category and duration do not have any functional relevance within the ixmp
        framework, but they may be useful for pre- or post-processing.  For example,
        they can be used to filter all timeslices of a certain category (e.g., all
        months) from the :class:`pandas.DataFrame` returned by this function or to
        aggregate subannual data to full-year results.

        Returns
        -------
        :class:`pandas.DataFrame`
            Data frame with columns 'timeslice', 'category', and 'duration'.

        See also
        --------
        add_timeslice
        """
        return pd.DataFrame(
            self._backend.get_timeslices(), columns=FIELDS["get_timeslices"]
        )

    def add_timeslice(self, name: str, category: str, duration: float) -> None:
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
            log.info(msg.format(name, duration))
        else:
            self._backend.set_timeslice(name, category, duration)

    def check_access(
        self, user: str, models: Union[str, Sequence[str]], access: str = "view"
    ) -> Union[bool, dict[str, bool]]:
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
        if not models_list:
            raise ValueError("must supply at least 1 model name")
        result = self._backend.get_auth(user, models_list, access)
        if isinstance(models, str):
            return result[models]
        else:
            return {model: result.get(model) == 1 for model in models_list}
