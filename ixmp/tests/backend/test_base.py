from pathlib import Path

import pytest

from ixmp.backend import ItemType
from ixmp.backend.base import Backend, CachingBackend
from ixmp.testing import make_dantzig


def test_class():
    # An incomplete Backend subclass can't be instantiated
    class BE1(Backend):
        pass

    with pytest.raises(
        TypeError, match="Can't instantiate abstract class BE1 " "with abstract methods"
    ):
        BE1()

    # A complete subclass
    def noop(self, *args, **kwargs):
        pass

    class BE2(Backend):
        add_model_name = noop
        add_scenario_name = noop
        cat_get_elements = noop
        cat_list = noop
        cat_set_elements = noop
        check_out = noop
        clear_solution = noop
        clone = noop
        commit = noop
        delete = noop
        delete_geo = noop
        delete_item = noop
        delete_meta = noop
        discard_changes = noop
        get = noop
        get_data = noop
        get_doc = noop
        get_geo = noop
        get_meta = noop
        get_model_names = noop
        get_nodes = noop
        get_scenarios = noop
        get_scenario_names = noop
        get_timeslices = noop
        get_units = noop
        has_solution = noop
        init = noop
        init_item = noop
        is_default = noop
        item_delete_elements = noop
        item_get_elements = noop
        item_index = noop
        item_set_elements = noop
        last_update = noop
        list_items = noop
        remove_meta = noop
        run_id = noop
        set_as_default = noop
        set_data = noop
        set_doc = noop
        set_geo = noop
        set_meta = noop
        set_node = noop
        set_timeslice = noop
        set_unit = noop

    # Complete subclass can be instantiated
    be = BE2()

    # Methods with a default implementation can be called
    with pytest.raises(NotImplementedError):
        be.read_file(Path("foo"), ItemType.VAR)

    with pytest.raises(NotImplementedError):
        be.write_file(Path("foo"), ItemType.VAR)


def test_cache_non_hashable():
    filters = {"s": ["foo", 42, object()]}

    # _cache_key() cannot handle non-hashable object()
    # NB exception message contains single quotes on 'object' on Windows/py3.6
    with pytest.raises(
        TypeError, match="Object of type .?object.? is not JSON" " serializable"
    ):
        CachingBackend._cache_key(object(), "par", "p", filters)


def test_cache_del_ts(test_mp):
    """Test CachingBackend.del_ts()."""
    # Since CachingBackend is an abstract class, test it via JDBCBackend
    backend = test_mp._backend
    cache_size_pre = len(backend._cache)

    # Load data, thereby adding to the cache
    s = make_dantzig(test_mp)
    s.par("d")

    # Cache size has increased
    assert len(backend._cache) == cache_size_pre + 1

    # Delete the object; associated cache is freed
    del s

    # Objects were invalidated/removed from cache
    assert len(backend._cache) == cache_size_pre
