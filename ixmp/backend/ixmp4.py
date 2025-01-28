import logging
from collections.abc import Generator, Iterable, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast

import pandas as pd

from ixmp.backend.base import CachingBackend
from ixmp.core.platform import Platform
from ixmp.core.scenario import Scenario
from ixmp.core.timeseries import TimeSeries

if TYPE_CHECKING:
    from ixmp4 import Platform as ixmp4_platform
    from ixmp4.core import Run
    from ixmp4.core.optimization.equation import Equation
    from ixmp4.core.optimization.indexset import IndexSet
    from ixmp4.core.optimization.parameter import Parameter
    from ixmp4.core.optimization.scalar import Scalar
    from ixmp4.core.optimization.table import Table
    from ixmp4.core.optimization.variable import Variable
    from ixmp4.data.backend.base import Backend as ixmp4_backend

log = logging.getLogger(__name__)


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`."""

    _platform: "ixmp4_platform"
    _backend: "ixmp4_backend"

    # Mapping from ixmp.TimeSeries object to the underlying ixmp4.Run object (or
    # subclasses of either)
    index: dict[TimeSeries, "Run"] = {}

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

    def set_log_level(self, level: int) -> None:
        # Set the level of the 'ixmp.backend.ixmp4' logger. Messages are handled by the
        # 'ixmp' logger; see ixmp/__init__.py.
        log.setLevel(level)

    # def get_log_level(self):
    #     return super().get_log_level()

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
            log.warning(
                "IXMP4Backend.set_node() requires to specify 'hierarchy'! "
                "Using 'None' as the meaningsless default."
            )
            hierarchy = "None"
        self._platform.regions.create(name=name, hierarchy=hierarchy)

    def get_nodes(self) -> list[tuple[str, None, str, str]]:
        return [
            (region.name, None, region.name, region.hierarchy)
            for region in self._platform.regions.list()
        ]

    # Modifying the Run object

    def _index_and_set_attrs(self, run: "Run", ts: TimeSeries) -> None:
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

    def clone(
        self,
        s: Scenario,
        platform_dest: Platform,
        model: str,
        scenario: str,
        annotation: str,
        keep_solution: bool,
        first_model_year: Optional[int] = None,
    ) -> Scenario:
        # TODO either do log.warning that annotation is unused or
        # run.docs = annotation
        # TODO Should this be supported?
        if first_model_year:
            log.warning(
                "ixmp4-backed Scenarios don't support cloning from "
                "`first_model_year` only!"
            )
        # TODO Is this enough? ixmp4 doesn't support cloning to a different platform at
        # the moment, but maybe we can imitate this here? (Access
        # platform_dest.backend._platform to create a new Run?)
        cloned_s = Scenario(
            mp=platform_dest, model=model, scenario=scenario, annotation=annotation
        )
        cloned_run = self.index[s].clone(
            model=model, scenario=scenario, keep_solution=keep_solution
        )
        self._index_and_set_attrs(cloned_run, cloned_s)
        return cloned_s

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
            "ixmp4 backed Scenarios/Runs are changed immediately, no need for a commit!"
        )

    def clear_solution(self, s: Scenario, from_year: Optional[int] = None) -> None:
        if from_year:
            log.warning(
                "ixmp4 does not support removing the solution only after a certain year"
            )
        self.index[s].optimization.remove_solution()

    def set_as_default(self, ts: TimeSeries) -> None:
        self.index[ts].set_as_default()

    # Information about the Run

    def run_id(self, ts: TimeSeries) -> int:
        # TODO is the Run.version really what this function is after?
        return self.index[ts].version

    def is_default(self, ts: TimeSeries) -> bool:
        return self.index[ts].is_default()

    def has_solution(self, s: Scenario) -> bool:
        return self.index[s].optimization.has_solution()

    # Handle optimization items

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

    def _find_item(
        self, s: Scenario, name: str
    ) -> tuple[
        str,
        Union[
            "Scalar",
            "IndexSet",
            "Table",
            "Equation",
            "Variable",
            "Parameter",
        ],
    ]:
        # NOTE this currently assumes that `name` will only be present once among
        # Tables, Parameters, Equations, Variables. This is in line with the assumption
        # made in the Java backend, but ixmp4 enforces no such constraint.
        _type: Optional[str] = None
        _item: Optional[
            Union[
                "Scalar",
                "IndexSet",
                "Table",
                "Equation",
                "Variable",
                "Parameter",
            ]
        ] = None
        for type in ("scalar", "indexset", "set", "par", "equ", "var"):
            repo = self._get_repo(s=s, type=type)
            item_list = repo.list(name=name)
            if (
                len(item_list) == 1
            ):  # ixmp4 enforces names to be unique among individual item classes
                _type = type
                _item = item_list[0]
                break

        if _item is None or _type is None:
            raise KeyError(f"No item called {name} found on this Scenario!")
        else:
            return (_type, _item)

    def _get_item(
        self,
        s: Scenario,
        name: str,
        type: Literal["scalar", "indexset", "set", "par", "equ", "var"],
    ) -> Union[
        "Scalar",
        "IndexSet",
        "Table",
        "Equation",
        "Variable",
        "Parameter",
    ]:
        return self._get_repo(s=s, type=type).get(name=name)

    def _get_indexset_or_table(
        self, s: Scenario, name: str
    ) -> Union["IndexSet", "Table"]:
        from ixmp4.core import IndexSet, Table
        from ixmp4.core.exceptions import NotFound

        try:
            repo = self._get_repo(s=s, type="indexset")
            return cast(IndexSet, repo.get(name=name))
        except NotFound:
            repo = self._get_repo(s=s, type="set")
            return cast(Table, repo.get(name=name))

    def item_index(
        self, s: Scenario, name: str, sets_or_names: Literal["sets", "names"]
    ) -> list[str]:
        _, item = self._find_item(s=s, name=name)
        # NOTE Using isinstance allows adequate attribute access
        if (
            isinstance(item, IndexSet)
            or isinstance(item, Scalar)
            or (
                isinstance(item, Variable)
                and sets_or_names == "names"
                and item.column_names is None
            )
        ):
            return cast(list[str], [])
        else:
            index_names = (
                item.column_names if sets_or_names == "names" else item.indexsets
            )
            assert index_names, (
                f"Requested {sets_or_names}, but these are None for item {item.name}"
            )
            return index_names

    def _add_data_to_set(
        self, s: Scenario, name: str, key: Union[str, list[str]], comment: Optional[str]
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
            # TODO should we enforce in ixmp4 that when constrained_to_indexsets
            # contains duplicate values, column_names must be provided?
            keys = table.column_names if table.column_names else table.indexsets
            data_to_add = {keys[i]: [key[i]] for i in range(len(key))}
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

    def _add_data_to_parameter(
        self,
        s: Scenario,
        name: str,
        key: Union[str, list[str]],
        value: float,
        unit: str,
        comment: Optional[str],
    ) -> None:
        if comment:
            log.warning(
                "`comment` currently unused with ixmp4 when adding data to Parameters."
            )
        parameter = self.index[s].optimization.parameters.get(name=name)
        # TODO there's got to be a better way for handling possible lists
        if isinstance(key, str):
            key = [key]
        keys = parameter.column_names if parameter.column_names else parameter.indexsets
        data_to_add: dict[str, Union[list[float], list[str]]] = {
            keys[i]: [key[i]] for i in range(len(key))
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

        item = self._get_indexset_or_table(s=s, name=name)

        if isinstance(item, Table):
            columns = item.column_names if item.column_names else item.indexsets
            return pd.DataFrame(item.data, columns=columns)
        else:
            return pd.Series(item.data)

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
            item = self._get_item(s=s, name=name, type=type)
            # TODO if item.data can be empty, set columns explicitly as for table above
            data = pd.DataFrame(item.data)  # type: ignore[union-attr]
            if type == "par":
                data.rename(columns={"values": "value", "units": "unit"}, inplace=True)
            else:
                data.rename(
                    columns={"levels": "level", "marginals": "marginal"}, inplace=True
                )
            return data

    def item_delete_elements(
        self,
        s: Scenario,
        type: Literal["par", "set"],
        name: str,
        keys: Iterable[Sequence[str]],
    ) -> None:
        if type == "set":
            item = self._get_indexset_or_table(s=s, name=name)

            if isinstance(item, IndexSet):
                # NOTE We might have to expose IndexSet._data_type to cast correctly
                data = pd.DataFrame(keys, columns=[item.name])
                item.remove(data=cast(list[str], data[item.name].to_list()))
            else:
                # TODO can we assume that keys follow same order as indexsets/columns?
                columns = item.column_names if item.column_names else item.indexsets
                data = pd.DataFrame(keys, columns=columns)
                item.remove(data=data)
        else:
            parameter = cast(Parameter, self._get_item(s=s, name=name, type="par"))
            columns = (
                parameter.column_names
                if parameter.column_names
                else parameter.indexsets
            )
            data = pd.DataFrame(keys, columns=columns)
            parameter.remove(data=data)

    def cat_set_elements(
        self,
        ms: Scenario,
        name: str,
        cat: str,
        keys: Union[str, Sequence[str]],
        is_unique: bool,
    ) -> None:
        """Add data to a category mapping.

        For the ixmp4.Table or IndexSet `name`, define a category as a new IndexSet
        called 'type_`name`' (if it doesn't exist already) and add `cat` to it. Then,
        define a new Table 'category_`name`' storing one column for `keys` and one for
        'categories'.

        Parameters
        ----------
        name : str
            Name of the category mapping Table.
        cat : str
            Name of the category within `name`.
        keys : iterable of str or list of str
            Keys to add to `cat`.
        is_unique : bool
            If :obj:`True`:

            - `keys` **must** contain only one key.
            - The Backend **must** remove any existing member of `cat`, so that it has
              only one element.
        """
        from ixmp4.core.exceptions import NotFound

        # Assume for now that only IndexSets are requested to be mapped to cats
        indexset = self.index[ms].optimization.indexsets.get(name=name)

        # Get or create the 'type_name' indexset and 'category_name' table
        try:
            category_indexset = self.index[ms].optimization.indexsets.get(
                name=f"type_{name}"
            )
        except NotFound:
            category_indexset = self.index[ms].optimization.indexsets.create(
                name=f"type_{name}"
            )

        try:
            category_table = self.index[ms].optimization.tables.get(
                name=f"category_{name}"
            )
        except NotFound:
            category_table = self.index[ms].optimization.tables.create(
                name=f"category_{name}",
                constrained_to_indexsets=[indexset.name, category_indexset.name],
            )

        # Convert for convenience
        if isinstance(keys, str):
            keys = [keys]

        # Ensure proper treatment when is_unique is True
        if is_unique:
            if len(keys) > 1:
                raise ValueError("One can only add one element to a unique category!")
            # Ensure data contains no data except that which we're going to add
            # NOTE if category_table contains data linked to elements existing now, this
            # will lead to DataValidationErrors when adding data to the table. Also,
            # ixmp4 might safeguard against this when implementing remove() functions
            if category_indexset.data:
                # TODO remove data once ixmp4 implements that
                log.warning("Can't remove data from ixmp4 objects (categories) yet!")

        # Add data to both objects
        category_indexset.add(data=cat)
        data = {indexset.name: keys, category_indexset.name: [cat] * len(keys)}
        category_table.add(data=data)

    def cat_get_elements(self, ms: Scenario, name: str, cat: str) -> list[str]:
        data = pd.DataFrame(
            self.index[ms].optimization.tables.get(name=f"category_{name}").data
        )
        # This assumes there are only two columns
        columns = data.columns.to_list()
        columns.remove(f"type_{name}")
        return data[data[f"type_{name}"] == cat, columns[0]].astype(str).to_list()

    def cat_list(self, ms: Scenario, name: str) -> list[str]:
        category_indexset = self.index[ms].optimization.indexsets.get(f"type_{name}")
        return cast(list[str], category_indexset.data)

    # Handle timeslices

    # def set_timeslice(self, name: str, category: str, duration: float) -> None:
    #     return super().set_timeslice(name, category, duration)

    # The below methods of base.Backend are not yet implemented
    def _ni(self, *args, **kwargs):
        raise NotImplementedError

    delete = _ni
    delete_geo = _ni
    delete_item = _ni
    delete_meta = _ni
    get_data = _ni
    get_doc = _ni
    get_geo = _ni
    get_meta = _ni
    get_timeslices = _ni
    last_update = _ni
    remove_meta = _ni
    set_data = _ni
    set_doc = _ni
    set_geo = _ni
    set_meta = _ni
    set_timeslice = _ni
