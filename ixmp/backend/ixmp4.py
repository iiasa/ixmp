from typing import TYPE_CHECKING

from ixmp.backend.base import CachingBackend

if TYPE_CHECKING:
    import ixmp4


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`."""

    _platform: "ixmp4.Platform"

    def __init__(self):
        import ixmp4

        # TODO Obtain `name` from the ixmp.Platform creating this Backend
        name = "test"

        # Add an ixmp4.Platform using ixmp4's own configuration code
        # TODO Move this to a test fixture
        # NB ixmp.tests.conftest.test_sqlite_mp exists, but is not importable (missing
        #    __init__.py)
        import ixmp4.conf

        dsn = "sqlite:///:memory:"
        try:
            ixmp4.conf.settings.toml.add_platform(name, dsn)
        except ixmp4.core.exceptions.PlatformNotUnique:
            pass

        # Instantiate and store
        self._platform = ixmp4.Platform(name)

    def get_scenarios(self, default, model, scenario):
        # Current fails with:
        # sqlalchemy.exc.OperationalError: (sqlite3.OperationalError) no such table: run
        # [SQL: SELECT DISTINCT run.model__id, run.scenario__id, run.version,
        # run.is_default, run.id
        # FROM run
        # WHERE run.is_default = 1 ORDER BY run.id ASC]
        return self._platform.runs.list()

    # The below methods of base.Backend are not yet implemented
    def _ni(self, *args, **kwargs):
        raise NotImplementedError

    add_model_name = _ni
    add_scenario_name = _ni
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
    get_model_names = _ni
    get_nodes = _ni
    get_scenario_names = _ni
    get_timeslices = _ni
    get_units = _ni
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
    set_unit = _ni
