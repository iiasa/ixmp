from ixmp.backend.base import CachingBackend


class IXMP4Backend(CachingBackend):
    """Backend using :mod:`ixmp4`."""

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
    get_scenarios = _ni
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
