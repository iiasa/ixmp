from ixmp.backend.base import CachingBackend


def test_cache_non_hashable():
    filters = {'s': ['foo', 42, object()]}

    # _cache_key() can handle non-hashable object()
    CachingBackend._cache_key(object(), 'par', 'p', filters)
