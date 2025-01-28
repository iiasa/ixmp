import logging
from collections.abc import Generator, Iterable, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

import pandas as pd

from ixmp.backend.base import CachingBackend
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries

if TYPE_CHECKING:
    import ixmp4
    from ixmp4.data.backend.base import Backend as ixmp4_backend

log = logging.getLogger(__name__)


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`."""

    _platform: "ixmp4.Platform"
    _backend: "ixmp4_backend"

    # Mapping from ixmp.TimeSeries object to the underlying ixmp4.Run object (or
    # subclasses of either)
    index: dict[TimeSeries, "ixmp4.Run"] = {}

    def __init__(self, _backend: Optional["ixmp4_backend"] = None) -> None:
        from ixmp4 import Platform
        from ixmp4.conf.base import PlatformInfo
        from ixmp4.data.backend import SqliteTestBackend

        # Create a default backend if None is provided
        if not _backend:
            log.warning("Falling back to default SqliteBackend 'ixmp4-local'")
            sqlite = SqliteTestBackend(
                PlatformInfo(name="ixmp4-local", dsn="sqlite:///:memory:")
            )
            sqlite.setup()

        self._backend = _backend if _backend else sqlite

        # Instantiate and store
        self._platform = Platform(_backend=self._backend)

    # def __del__(self) -> None:
    #     self.close_db()

    # Platform methods
    @classmethod
    def handle_config(cls, args: Sequence, kwargs: MutableMapping) -> dict[str, Any]:
        msg = "Unhandled {} args to Backend.handle_config(): {!r}"
        if len(args):
            raise ValueError(msg.format("positional", args))

        info: dict[str, Any] = {}
        try:
            info["_backend"] = kwargs["_backend"]
        except KeyError:
            raise ValueError(f"Missing key '_backend' for backend=ixmp4; got {kwargs}")

        return info

    # def close_db(self) -> None:
    #     self._backend.close()

    # Modifying the Platform

    def add_scenario_name(self, name: str) -> None:
        self._platform.scenarios.create(name=name)

    # TODO clarify: ixmp4.Run doesn't have a name, but is the new ixmp.Scenario
    # should it have a name or are these scenario names okay?
    def get_scenario_names(self) -> Generator[str, None, None]:
        for scenario in self._platform.scenarios.list():
            yield scenario.name

    def add_model_name(self, name: str) -> None:
        self._platform.models.create(name=name)

    def get_model_names(self) -> Generator[str, None, None]:
        for model in self._platform.models.list():
            yield model.name

    def get_scenarios(self, default, model, scenario):
        return self._platform.runs.list()

    def set_unit(self, name: str, comment: str) -> None:
        self._platform.units.create(name=name).docs = comment

    def get_units(self) -> list[str]:
        return [unit.name for unit in self._platform.units.list()]

    def set_node(
        self,
        name: str,
        parent: Optional[str] = None,
        hierarchy: Optional[str] = None,
        synonym: Optional[str] = None,
    ) -> None:
        if parent:
            log.warning(f"Discarding parent parameter {parent}; unused in ixmp4.")
        if synonym:
            log.warning(f"Discarding synonym parameter {synonym}; unused in ixmp4.")
        if hierarchy is None:
            raise ValueError("IXMP4Backend.set_node() requires to specify 'hierarchy'!")
        self._platform.regions.create(name=name, hierarchy=hierarchy)

    def get_nodes(self) -> list[tuple[str, str, None, str]]:
        return [
            (region.name, region.name, None, region.hierarchy)
            for region in self._platform.regions.list()
        ]

    # Modifying the Run object

    def _index_and_set_attrs(self, run: "ixmp4.Run", ts: TimeSeries) -> None:
        """Add *run* to index and update attributes of *ts*.

        Helper for init and get.
        """
        # Add to index
        self.index[ts] = run

        # Retrieve the version of the ixmp4.Run object
        v = run.version
        if ts.version is None:
            # The default version was requested; update the attribute
            ts.version = v
        elif v != ts.version:  # pragma: no cover
            # Something went wrong on the ixmp4 side
            raise RuntimeError(f"got version {v} instead of {ts.version}")

    def init(self, ts: TimeSeries, annotation: str) -> None:
        run = self._platform.runs.create(model=ts.model, scenario=ts.scenario)
        # TODO either do log.warning that annotation is unused or
        # run.docs = annotation
        self._index_and_set_attrs(run, ts)

    def get(self, ts: TimeSeries) -> None:
        v = int(ts.version) if ts.version else None
        run = self._platform.runs.get(model=ts.model, scenario=ts.scenario, version=v)
        self._index_and_set_attrs(run, ts)

    def check_out(self, ts: TimeSeries, timeseries_only: bool) -> None:
        log.warning("ixmp4 backed Scenarios/Runs don't need to be checked out!")

    def discard_changes(self, ts: TimeSeries) -> None:
        log.warning(
            "ixmp4 backed Scenarios/Runs are changed immediately, can't "
            "discard changes. To avoid the need, be sure to start work on "
            "fresh clones."
        )

    def commit(self, ts: TimeSeries, comment: str) -> None:
        log.warning(
            "ixmp4 backed Scenarios/Runs are changed immediately, no need for "
            "a commit!"
        )

    def clear_solution(self, s: Scenario, from_year: Optional[int] = None) -> None:
        if from_year:
            log.warning(
                "ixmp4 does not support removing the solution only after a "
                "certain year"
            )
        self.index[s].optimization.remove_solution()

    def set_as_default(self, ts: TimeSeries) -> None:
        # TODO are we okay with always loading a Run for this or should we adapt
        # TimeSeries to have a `_run` attribute that has the Run loaded?
        self.index[ts].set_as_default()

    # Information about the Run

    def run_id(self, ts: TimeSeries) -> int:
        # TODO is the Run.version really what this function is after?
        return self.index[ts].version

    def is_default(self, ts: TimeSeries) -> bool:
        return self.index[ts].is_default()

    # Handle various items

    # TODO: type hints
    def _get_repo(
        self,
        s: Scenario,
        type: Literal["scalar", "indexset", "set", "par", "equ", "var"],
    ):
        if type == "scalar":
            return self.index[s].optimization.scalars
        if type == "indexset":
            return self.index[s].optimization.indexsets
        if type == "set":
            return self.index[s].optimization.tables
        elif type == "par":
            return self.index[s].optimization.parameters
        elif type == "equ":
            return self.index[s].optimization.equations
        else:  # "var"
            return self.index[s].optimization.variables

    def init_item(
        self,
        s: Scenario,
        type: Literal["set", "par", "equ", "var"],
        name: str,
        idx_sets: Sequence[str],
        idx_names: Optional[Sequence[str]],
    ) -> None:
        # TODO how are scalars created? Scalars require a value in ixmp4
        # In base::item_get_elements, it sounds like "equ" and "var" can also target
        # scalars, whereas below, inspired from jdbc, I'm only linking "par" to scalars
        if type == "set" and len(idx_sets) == 0:
            repo = self._get_repo(s=s, type="indexset")
            repo.create(name=name)
        else:
            repo = self._get_repo(s=s, type=type)
            repo.create(
                name=name, constrained_to_indexsets=idx_sets, column_names=idx_names
            )

    def list_items(self, s: Scenario, type: Literal["set", "par", "equ"]) -> list[str]:
        if type == "set":
            indexset_repo = self._get_repo(s=s, type="indexset")
            set_repo = self._get_repo(s=s, type=type)
            return [item.name for item in indexset_repo.list()] + [
                item.name for item in set_repo.list()
            ]
        else:
            repo = self._get_repo(s=s, type=type)
            return [item.name for item in repo.list()]

    # TODO type hints
    def _find_item(self, s: Scenario, name: str):
        # NOTE this currently assumes that `name` will only be present once among
        # Tables, Parameters, Equations, Variables. This is in line with the assumption
        # made in the Java backend, but ixmp4 enforces no such constraint.
        for type in ("scalar", "indexset", "set", "par", "equ", "var"):
            repo = self._get_repo(s=s, type=type)
            item_list = repo.list(name=name)
            if (
                len(item_list) == 1
            ):  # ixmp4 enforces names to be unique among individual item classes
                return (type, item_list[0])

    def _get_item(
        self,
        s: Scenario,
        name: str,
        type: Literal["scalar", "indexset", "set", "par", "equ", "var"],
    ):
        # TODO add try except here to always return using IndexError from repo.list()[0]
        repo = self._get_repo(s=s, type=type)
        item_list = repo.list(name=name)
        if (
            len(item_list) == 1
        ):  # ixmp4 enforces names to be unique among individual item classes
            return (type, item_list[0])

    # TODO mypy says this function is missing a return statement. But why?
    # AFAICT, items.items() will always contain something, so we will always return
    # something (unless we raise). Am I missing something?
    def item_index(  # type: ignore[return]
        self, s: Scenario, name: str, sets_or_names: Literal["sets", "names"]
    ) -> list[str]:
        type, item = self._find_item(s=s, name=name)
        if item is None:
            raise LookupError(f"No item called {name} found on this Scenario!")
        if type == "indexset":
            return cast(list[str], [])
        else:
            return (
                [column.name for column in item.columns]
                if sets_or_names == "names"
                else item.constrained_to_indexsets
            )

    def _add_data_to_set(
        self, s: Scenario, name: str, key: str | list[str], comment: Optional[str]
    ) -> None:
        if comment:
            log.warning(
                "`comment` currently unused with ixmp4 when adding data to Tables."
            )
        # Assumption: if key is just one value, we're dealing with an IndexSet
        if isinstance(key, str):
            self.index[s].optimization.indexsets.get(name=name).add(key)
        else:
            table = self.index[s].optimization.tables.get(name=name)
            data_to_add = {
                table.constrained_to_indexsets[i]: key[i] for i in range(len(key))
            }
            table.add(data=data_to_add)

    def _create_scalar(
        self,
        s: Scenario,
        name: str,
        value: float,
        unit: Optional[str],
        comment: Optional[str],
    ) -> None:
        scalar = self.index[s].optimization.scalars.create(
            name=name, value=value, unit=unit
        )
        if comment:
            scalar.docs = comment

    ### TODO
    ###
    ### Currently, tests fail because they expect 'cases' to be predefined on the test_mp

    def _add_data_to_parameter(
        self,
        s: Scenario,
        name: str,
        key: str | list[str],
        value: float,
        unit: str,
        comment: Optional[str],
    ) -> None:
        if comment:
            log.warning(
                "`comment` currently unused with ixmp4 when adding data to "
                "Parameters."
            )
        parameter = self.index[s].optimization.parameters.get(name=name)
        # TODO there's got to be a better way for handling possible lists
        if isinstance(key, str):
            key = [key]
        data_to_add: dict[str, Union[list[float], list[str]]] = {
            parameter.constrained_to_indexsets[i]: [key[i]] for i in range(len(key))
        }
        data_to_add["values"] = [value]
        data_to_add["units"] = [unit]
        parameter.add(data=data_to_add)

    def item_set_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        elements: Iterable[tuple[Any, Optional[float], Optional[str], Optional[str]]],
    ) -> None:
        for key, value, unit, comment in elements:
            if type == "set":
                self._add_data_to_set(s=s, name=name, key=key, comment=comment)
            else:
                if key is None:
                    assert value, "Creating a Scalar requires a value!"
                    self._create_scalar(
                        s=s, name=name, value=value, unit=unit, comment=comment
                    )
                else:
                    assert value, "Adding data to a Parameter requires a value!"
                    assert unit, "Adding data to a Parameter requires a unit!"
                    self._add_data_to_parameter(
                        s=s, name=name, key=key, value=value, unit=unit, comment=comment
                    )

    def _get_set_data(
        self, s: Scenario, name: str, filters: Optional[dict[str, list[Any]]] = None
    ) -> Union[pd.Series, pd.DataFrame]:
        # TODO handle filters
        from ixmp4.core import IndexSet, Table
        from ixmp4.core.exceptions import NotFound

        item: Union[IndexSet, Table]
        try:
            repo = self._get_repo(s=s, type="indexset")
            item = cast(IndexSet, repo.get(name=name))
        except NotFound:
            repo = self._get_repo(s=s, type="set")
            item = cast(Table, repo.get(name=name))

        return (
            pd.DataFrame(item.data) if isinstance(item, Table) else pd.Series(item.data)
        )

    def item_get_elements(
        self,
        s: Scenario,
        type: Literal["equ", "par", "set", "var"],
        name: str,
        filters: Optional[dict[str, list[Any]]] = None,
    ) -> Union[dict[str, Any], pd.Series, pd.DataFrame]:
        # TODO handle filters
        if type == "set":
            return self._get_set_data(s=s, name=name, filters=filters)
        # TODO this is not handling scalars at the moment, but maybe try with type,
        # except NotFound, try scalar?
        else:
            # TODO this can really only be Equation, Parameter, or Variable, so cast
            # as such?
            _, item = self._get_item(s=s, name=name, type=type)
            data = pd.DataFrame(item.data)
            print(data)
            if type == "par":
                data.rename(columns={"values": "value", "units": "unit"}, inplace=True)
            else:
                data.rename(
                    columns={"levels": "level", "marginals": "marginal"}, inplace=True
                )
            return data

    # The below methods of base.Backend are not yet implemented
    def _ni(self, *args, **kwargs):
        raise NotImplementedError

    cat_get_elements = _ni
    cat_list = _ni
    cat_set_elements = _ni
    clone = _ni
    delete = _ni
    delete_geo = _ni
    delete_item = _ni
    delete_meta = _ni
    get_data = _ni
    get_doc = _ni
    get_geo = _ni
    get_meta = _ni
    get_timeslices = _ni
    has_solution = _ni
    item_delete_elements = _ni
    last_update = _ni
    remove_meta = _ni
    set_data = _ni
    set_doc = _ni
    set_geo = _ni
    set_meta = _ni
    set_timeslice = _ni
