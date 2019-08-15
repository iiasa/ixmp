from .jdbc import JDBCBackend


#: Mapping from names to available backends
BACKENDS = {
    'jdbc': JDBCBackend,
}
