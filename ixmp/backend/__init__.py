from enum import Enum, IntFlag, auto


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


class CodeList(Enum):
    """Lists of codes in the ixmp data model."""
    #: Annotation IDs. See :meth:`.set_anno`
    metadata = auto()
    #: Model names appearing as :attr:`.TimeSeries.model` attributes.
    model = auto()
    #: Region names. See :meth:`.set_node`.
    region = auto()
    #: Values of :meth:`.TimeSeries.run_id`, which uniquely identifies
    #: combinations of (model, scenario, version).
    run = auto()
    #: Scenario names appearing as :attr:`.TimeSeries.scenario` attributes.
    scenario = auto()
    #: Time slice names. See :meth:`.set_timeslice`.
    timeslice = auto()
    #: Units of measurement. See :meth:`.set_unit`.
    unit = auto()
    #: 'Variable' dimension of time series data. See :meth:`.set_data`.
    variable = auto()


class ItemType(IntFlag):
    """Type of data items in :class:`.TimeSeries` and :class:`.Scenario`."""
    # NB the docstring comments ('#:') are placed as they are to ensure the
    #    output is readable.

    TS = 1
    #: Time series data variable.
    T = TS

    SET = 2
    #: Set.
    S = SET

    PAR = 4
    #: Parameter.
    P = PAR

    VAR = 8
    #: Model variable.
    V = VAR

    EQU = 16
    #: Equation.
    E = EQU

    MODEL = SET + PAR + VAR + EQU
    #: All kinds of model-related data, i.e. :attr:`SET`, :attr:`PAR`,
    #: :attr:`VAR` and :attr:`EQU`.
    M = MODEL

    #: Model solution data, i.e. :attr:`VAR` and :attr:`EQU`.
    SOLUTION = VAR + EQU

    ALL = TS + MODEL
    #: All data, i.e. :attr:`MODEL` and :attr:`TS`.
    A = ALL
