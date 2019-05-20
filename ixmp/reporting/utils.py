from copy import deepcopy
from functools import partial, reduce
from itertools import compress
from logging import getLogger
from math import ceil
from operator import mul

import pandas as pd
import pint
import sparse
import xarray as xr

log = getLogger(__name__)

ureg = pint.UnitRegistry()



def combo_partition(iterable):
    """Yield pairs of lists with all possible subsets of *iterable*."""
    # Format string for binary conversion, e.g. '04b'
    fmt = '0{}b'.format(len(iterable))
    for n in range(2 ** len(iterable) - 1):
        # Two binary lists
        a, b = zip(*[(v, not v) for v in map(int, format(n, fmt))])
        yield list(compress(iterable, a)), list(compress(iterable, b))


class Key:
    """A hashable key for a quantity that includes its dimensionality."""
    # TODO cache repr() and only recompute when name/dims changed
    def __init__(self, name, dims=[], tag=None):
        self._name = name
        self._dims = list(dims)
        self._tag = tag

    @classmethod
    def from_str_or_key(cls, value):
        if isinstance(value, cls):
            return deepcopy(value)
        else:
            name, dims = value.split(':')
            return cls(name, dims.split('-'))

    def __repr__(self):
        """Representation of the Key, e.g. name:dim1-dim2-dim3."""
        return ':'.join([self._name, '-'.join(self._dims)] +
                        ([self._tag] if self._tag is not None else []))

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == other

    def __lt__(self, other):
        if isinstance(other, (self.__class__, str)):
            return repr(self) < repr(other)

    def __gt__(self, other):
        if isinstance(other, (self.__class__, str)):
            return repr(self) > repr(other)

    def aggregates(self):
        """Yield (key, task) for all possible aggregations of the Key."""
        from .computations import aggregate

        for agg_dims, others in combo_partition(self._dims):
            yield Key(self._name, agg_dims), \
                (partial(aggregate, dimensions=others), self)


def clean_units(input_string):
    """Tolerate messy strings for units.

    Handles two specific cases found in |MESSAGEix| test cases:

    - Dimensions enclosed in '[]' have these characters stripped.
    - The '%' symbol cannot be supported by pint, because it is a Python
      operator; it is translated to 'percent'.

    """
    input_string = input_string.strip('[]').replace('%', 'percent')
    # For MESSAGE-GLOBIOM
    # TODO make this configurable
    input_string = input_string.replace('???', '')
    return input_string


def collect_units(*args):
    for arg in args:
        if '_unit' in arg.attrs:
            # Convert units if necessary
            if isinstance(arg.attrs['_unit'], str):
                arg.attrs['_unit'] = ureg.parse_units(arg.attrs['_unit'])
        else:
            log.debug('assuming {} is unitless'.format(arg))
            arg.attrs['_unit'] = ureg.parse_units('')

    return [arg.attrs['_unit'] for arg in args]


# Mapping from raw → preferred dimension names
rename_dims = {}


def _find_dims(data, ix_type):
    # List of the dimensions
    dims = data.columns.tolist()

    # Remove columns containing values or units; dimensions are the remainder
    for col in 'value', 'lvl', 'mrg', 'unit':
        try:
            dims.remove(col)
        except ValueError:
            continue

    # Rename dimensions
    return [rename_dims.get(d, d) for d in dims]


def keys_for_quantity(ix_type, name, scenario, aggregates=True):
    """Iterate over keys for *name* in *scenario."""
    # Retrieve at least one row of the data
    # TODO use the low-level/Java API to avoid retrieving all values at this
    # point
    data = scenario.element(ix_type, name)

    if isinstance(data, dict):
        # ixmp/GAMS scalar is not returned as pd.DataFrame
        data = pd.DataFrame.from_records([data])

    # List of the dimensions
    dims = _find_dims(data, ix_type)

    # Data not used further
    del data

    # Column for retrieving data
    column = 'value' if ix_type == 'par' else 'lvl'

    # A computation to retrieve the data
    key = Key(name, dims)
    yield (key, (partial(data_for_quantity, ix_type, name, column),
                 'scenario'))

    if ix_type == 'equ':
        # Add the marginal values at full resolution, but no aggregates
        mrg_key = Key('{}-margin'.format(name), dims)
        yield (mrg_key, (partial(data_for_quantity, ix_type, name, 'mrg'),
                        'scenario'))

    if aggregates:
        # Aggregates
        yield from key.aggregates()


def _parse_units(units_series):
    unit = pd.unique(units_series)

    if len(unit) > 1:
        log.info(f'Mixed units {unit} discarded')
        unit = ['']

    # Parse units
    try:
        unit = clean_units(unit[0])
        unit = ureg.parse_units(unit)
    except IndexError:
        # Quantity has no unit
        unit = ureg.parse_units('')
    except pint.UndefinedUnitError:
        # Units do not exist; define them in the UnitRegistry
        definition = f'{unit} = [{unit}]'
        log.info(f'Add unit definition: {definition}')
        ureg.define(definition)
        unit = ureg.parse_units(unit)

    return unit


def data_for_quantity(ix_type, name, column, scenario):
    """Retrieve *name* from *scenario* as a :class:`xarray.Dataset`.

    Parameters
    ----------
    scenario : ixmp.Scenario
        Source
    kind : 'par' or 'equ'
        Type of quantity to be retrieved.

    Returns
    -------
    dict of xarray.DataArray
        Dictionary keys are 'value' (kind='par') or ('lvl', 'mrg')
        (kind='equ').
    """
    # NB this could be moved to ixmp.Scenario
    data = scenario.element(ix_type, name)

    if isinstance(data, dict):
        # ixmp/GAMS scalar is not returned as pd.DataFrame
        data = pd.DataFrame.from_records([data])

    # List of the dimensions
    dims = _find_dims(data, ix_type)

    # Remove the unit from the DataFrame
    try:
        attrs = {'_unit': _parse_units(data.pop('unit'))}
    except KeyError:
        # 'equ' are returned without units
        attrs = {}

    # Set index if 1 or more dimensions
    if len(dims):
        # First rename, then set index
        data.rename(columns=rename_dims, inplace=True)
        data.set_index(dims, inplace=True)

    # Convert to a Dataset, assign attrbutes and name
    ds = xr.Dataset.from_dataframe(data)[column] \
           .assign_attrs(attrs) \
           .rename(name + ('-margin' if column == 'mrg' else ''))
    try:
        # Remove length-1 dimensions for scalars
        ds = ds.squeeze('index', drop=True)
    except KeyError:
        pass

    return ds
