import pytest

from ixmp.backend.base import CachingBackend


def test_cache_non_hashable():
    filters = {'s': ['foo', 42, object()]}

    # _cache_key() cannot handle non-hashable object()
    with pytest.raises(TypeError,
                       match='Object of type object is not JSON serializable'):
        CachingBackend._cache_key(object(), 'par', 'p', filters)
