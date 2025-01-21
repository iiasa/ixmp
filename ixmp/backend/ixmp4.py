import logging
from collections.abc import Generator, MutableMapping, Sequence
from typing import TYPE_CHECKING, Any, Optional

from ixmp4 import Platform
from ixmp4.conf.base import PlatformInfo
from ixmp4.data.backend import SqliteTestBackend
from ixmp4.data.backend.base import Backend as ixmp4_backend

from ixmp.backend.base import CachingBackend

if TYPE_CHECKING:
    import ixmp4

log = logging.getLogger(__name__)


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`."""

    _platform: "ixmp4.Platform"
    _backend: ixmp4_backend

    def __init__(self, _backend: Optional[ixmp4_backend] = None) -> None:
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

    def add_scenario_name(self, name: str) -> None:
        self._platform.scenarios.create(name)

    # TODO clarify: ixmp4.Run doesn't have a name, but is the new ixmp.Scenario
    # should it have a name or are these scenario names okay?
    def get_scenario_names(self) -> Generator[str, None, None]:
        for scenario in self._platform.scenarios.list():
            yield scenario.name

    def add_model_name(self, name: str) -> None:
        self._platform.models.create(name)

    def get_model_names(self) -> Generator[str, None, None]:
        for model in self._platform.models.list():
            yield model.name

    def get_scenarios(self, default, model, scenario):
        return self._platform.runs.list()

    def set_unit(self, name: str, comment: str) -> None:
        self._platform.units.create(name).docs = comment

    def get_units(self) -> list[str]:
        return [unit.name for unit in self._platform.units.list()]

    # The below methods of base.Backend are not yet implemented
    def _ni(self, *args, **kwargs):
        raise NotImplementedError

    cat_get_elements = _ni
    cat_list = _ni
    cat_set_elements = _ni
    check_out = _ni
    clear_solution = _ni
    clone = _ni
    commit = _ni
    delete = _ni
    delete_geo = _ni
    delete_item = _ni
    delete_meta = _ni
    discard_changes = _ni
    get = _ni
    get_data = _ni
    get_doc = _ni
    get_geo = _ni
    get_meta = _ni
    get_nodes = _ni
    get_timeslices = _ni
    has_solution = _ni
    init = _ni
    init_item = _ni
    is_default = _ni
    item_delete_elements = _ni
    item_get_elements = _ni
    item_index = _ni
    item_set_elements = _ni
    last_update = _ni
    list_items = _ni
    remove_meta = _ni
    run_id = _ni
    set_as_default = _ni
    set_data = _ni
    set_doc = _ni
    set_geo = _ni
    set_meta = _ni
    set_node = _ni
    set_timeslice = _ni
