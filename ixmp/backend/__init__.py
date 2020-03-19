from enum import IntFlag


#: Lists of field names for tuples returned by Backend API methods.
FIELDS = {
    'get_nodes': ('region', 'mapped_to', 'parent', 'hierarchy'),
    'get_timeslices': ('name', 'category', 'duration'),
    'get_scenarios': ('model', 'scenario', 'scheme', 'is_default',
                      'is_locked', 'cre_user', 'cre_date', 'upd_user',
                      'upd_date', 'lock_user', 'lock_date', 'annotation',
                      'version'),
    'ts_get': ('region', 'variable', 'unit', 'subannual', 'year', 'value'),
    'ts_get_geo': ('region', 'variable', 'subannual', 'year', 'value', 'unit',
                   'meta'),
}


#: Mapping from names to available backends. To register additional backends,
#: add elements to this variable.
BACKENDS = {}


class ItemType(IntFlag):
    """Type of data items."""
    #: Time series data variable.
    TS = 1
    #: Set.
    SET = 2
    #: Parameter.
    PAR = 4
    #: Model variable.
    VAR = 8
    #: Equation.
    EQU = 16

    #: All kinds of model-related data, i.e. :attr:`SET`, :attr:`PAR`,
    #: :attr:`VAR` and :attr:`EQU`.
    MODEL = SET + PAR + VAR + EQU

    #: All data, i.e. :attr:`MODEL` and :attr:`TS`.
    ALL = TS + MODEL
