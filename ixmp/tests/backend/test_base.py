import sys
from pathlib import Path
from typing import Any

import pytest

from ixmp import Platform, TimeSeries
from ixmp.backend.base import Backend, CachingBackend
from ixmp.backend.common import ItemType
from ixmp.testing import make_dantzig


class BE1(Backend):
    """Incomplete subclass."""

    def __init__(self) -> None:
        super().__init__()


def noop(self: Backend, *args: Any, **kwargs: Any) -> Any:
    pass


class BE2(Backend):
    """Complete subclass."""

    def __init__(self) -> None:
        super().__init__()

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


def test_class() -> None:
    # An incomplete Backend subclass can't be instantiated
    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class BE1 with(out an implementation for)? "
        "abstract methods",
    ):
        # NOTE Triggering the error on purpose
        BE1()  # type: ignore[abstract]

    # Complete subclass can be instantiated
    BE2()


class TestBackend:
    @pytest.fixture
    def be(self) -> BE2:
        return BE2()

    # Methods with a default implementation
    def test_get_auth(self, be: BE2) -> None:
        assert dict(foo=True, bar=True, baz=True) == be.get_auth(
            "user", "foo bar baz".split(), "access"
        )

    def test_get_log_level(self, be: BE2) -> None:
        # The value may differ according to the the test environment, so only check type
        assert isinstance(be.get_log_level(), str)

    def test_read_file(self, be: BE2) -> None:
        with pytest.raises(NotImplementedError):
            be.read_file(Path("foo"), ItemType.VAR)

    def test_write_file(self, be: BE2) -> None:
        with pytest.raises(NotImplementedError):
            be.write_file(Path("foo"), ItemType.VAR)


@pytest.mark.parametrize(
    "args, kwargs",
    (
        (tuple(), dict()),
        # Invalid
        pytest.param(("foo",), dict(), marks=pytest.mark.xfail(raises=ValueError)),
        pytest.param(tuple(), dict(bar=""), marks=pytest.mark.xfail(raises=ValueError)),
    ),
)
def test_handle_config(args: tuple[str, ...], kwargs: dict[str, str]) -> None:
    """Test :meth:`JDBCBackend.handle_config`."""
    assert dict() == Backend.handle_config(args, kwargs)


class TestCachingBackend:
    def test_cache_non_hashable(self) -> None:
        filters = {"s": ["foo", 42, object()]}

        # _cache_key() cannot handle non-hashable object()
        with pytest.raises(
            TypeError, match="Object of type object is not JSON serializable"
        ):
            # NOTE Triggering the error on purpose
            CachingBackend._cache_key(object(), "par", "p", filters)  # type: ignore[arg-type]

    @pytest.mark.ixmp4_209
    def test_cache_invalidate(self, test_mp: Platform) -> None:
        backend = test_mp._backend

        ts = TimeSeries(test_mp, model="foo", scenario="bar", version="new")

        backend.cache_invalidate(ts, "par", "baz", dict(x=["x1", "x2"], y=["y1", "y2"]))

    @pytest.mark.ixmp4_209
    def test_del_ts(self, test_mp: Platform, request: pytest.FixtureRequest) -> None:
        """Test CachingBackend.del_ts()."""
        # Since CachingBackend is an abstract class, test it via JDBCBackend
        backend = test_mp._backend
        cache_size_pre = len(backend._cache)

        # Load data, thereby adding to the cache
        s = make_dantzig(test_mp, request=request)
        s.par("d")

        # Cache size has increased
        assert cache_size_pre + 1 == len(backend._cache)

        # JPype ≥ 1.4.1 with Python ≤ 3.10 produces danging traceback/frame references
        # to `s` that prevent it being GC'd at "del s" below. See
        # https://github.com/iiasa/ixmp/issues/463 and test_jdbc.test_del_ts
        if sys.version_info.minor <= 10:
            s.__del__()  # Force deletion of cached objects associated with `s`

        # Delete the object; associated cache is freed
        backend.del_ts(s)

        # Objects were invalidated/removed from cache
        assert cache_size_pre == len(backend._cache)
