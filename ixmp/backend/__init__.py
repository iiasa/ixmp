from enum import IntFlag


#: Lists of field names for tuples returned by Backend API methods.
FIELDS = {
    'get_nodes': ('region', 'mapped_to', 'parent', 'hierarchy'),
    'get_scenarios': ('model', 'scenario', 'scheme', 'is_default',
                      'is_locked', 'cre_user', 'cre_date', 'upd_user',
                      'upd_date', 'lock_user', 'lock_date', 'annotation',
                      'version'),
    'ts_get': ('region', 'variable', 'unit', 'year', 'value'),
    'ts_get_geo': ('region', 'variable', 'time', 'year', 'value', 'unit',
                   'meta'),
}


#: Mapping from names to available backends. To register additional backends,
#: add elements to this variable.
BACKENDS = {}


#: Type of data items in ixmp Platforms, TimeSeries, and Scenarios.
class ItemType(IntFlag):
    TS = 1
    SET = 2
    PAR = 4
    VAR = 8
    EQU = 16

    MODEL = SET + PAR + VAR + EQU
    ALL = TS + MODEL
