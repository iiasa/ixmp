from functools import partial, reduce
from itertools import compress
import logging
from operator import mul

import pandas as pd
import pint
import xarray as xr

log = logging.getLogger(__name__)

ureg = pint.UnitRegistry()


# Replacements to apply to quantity units before parsing by pint
replace_units = {
    '%': 'percent',
}


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
        self._tag = tag if isinstance(tag, str) and len(tag) else None

    @classmethod
    def from_str_or_key(cls, value, drop=None, append=None, tag=None):
        """Return a new Key from *value*."""
        if isinstance(value, cls):
            name = value._name
            dims = value._dims.copy()
            _tag = value._tag
        else:
            name, *dims = value.split(':')
            _tag = dims[1] if len(dims) == 2 else None
            dims = dims[0].split('-')
        if drop:
            dims = list(filter(lambda d: d not in drop, dims))
        if append:
            dims.append(append)
        tag = '+'.join(filter(None, [_tag, tag]))
        return cls(name, dims, tag)

    @classmethod
    def product(cls, new_name, *keys):
        """Return a new key that has the union of dimensions on *keys*.

        Dimensions are ordered by their first appearance.
        """
        # Dimensions of first key appear first
        base_dims = keys[0]._dims

        # Accumulate additional dimensions from subsequent keys
        new_dims = []
        for key in keys[1:]:
            new_dims.extend(set(key._dims) - set(base_dims) - set(new_dims))

        # Return new key
        return cls(new_name, base_dims + new_dims)

    def __repr__(self):
        """Representation of the Key, e.g. name:dim1-dim2-dim3."""
        return ':'.join([self._name, '-'.join(self._dims)]
                        + ([self._tag] if self._tag is not None else []))

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

    def iter_sums(self):
        """Yield (key, task) for all possible partial sums of the Key."""
        from . import computations

        for agg_dims, others in combo_partition(self._dims):
            yield Key(self._name, agg_dims), \
                (partial(computations.sum, dimensions=others, weights=None),
                 self)


def clean_units(input_string):
    """Tolerate messy strings for units.

    Handles two specific cases found in |MESSAGEix| test cases:

    - Dimensions enclosed in '[]' have these characters stripped.
    - The '%' symbol cannot be supported by pint, because it is a Python
      operator; it is translated to 'percent'.

    """
    input_string = input_string.strip('[]')
    for old, new in replace_units.items():
        input_string = input_string.replace(old, new)
    return input_string


def collect_units(*args):
    """Return an list of '_unit' attributes for *args*."""
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


def _find_dims(data):
    """Return the list of dimensions for *data*."""
    if isinstance(data, pd.DataFrame):
        # List of the dimensions
        dims = data.columns.tolist()
    else:
        dims = list(data)

    # Remove columns containing values or units; dimensions are the remainder
    for col in 'value', 'lvl', 'mrg', 'unit':
        try:
            dims.remove(col)
        except ValueError:
            continue

    # Rename dimensions
    return [rename_dims.get(d, d) for d in dims]


def keys_for_quantity(ix_type, name, scenario):
    """Iterate over keys for *name* in *scenario*."""
    # Retrieve names of the indices of the low-level/Java object
    # NB this is used instead of .getIdxSets, since the same set may index more
    #    than one dimension of the same variable.
    dims = _find_dims(scenario.item(ix_type, name).getIdxNames().toArray())

    # Column for retrieving data
    column = 'value' if ix_type == 'par' else 'lvl'

    # A computation to retrieve the data
    key = Key(name, dims)
    yield (key, (partial(data_for_quantity, ix_type, name, column),
                 'scenario'))

    # Add the marginal values at full resolution, but no aggregates
    if ix_type == 'equ':
        yield (Key('{}-margin'.format(name), dims),
               (partial(data_for_quantity, ix_type, name, 'mrg'), 'scenario'))

    # Partial sums
    yield from key.iter_sums()


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
    """Retrieve data from *scenario*.

    Parameters
    ----------
    ix_type : 'equ' or 'par' or 'var'
        Type of the ixmp object.
    name : str
        Name of the ixmp object.
    column : 'mrg' or 'lvl' or 'value'
        Data to retrieve. 'mrg' and 'lvl' are valid only for ix_type='equ', and
        'level' otherwise.
    scenario : ixmp.Scenario
        Scenario containing data to be retrieved

    Returns
    -------
    xr.DataArray
    """
    log.debug('Retrieving data for {}'.format(name))
    # Retrieve quantity data
    data = scenario.element(ix_type, name)

    # ixmp/GAMS scalar is not returned as pd.DataFrame
    if isinstance(data, dict):
        data = pd.DataFrame.from_records([data])

    # List of the dimensions
    dims = _find_dims(data)

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

    # Check sparseness
    try:
        shape = list(map(len, data.index.levels))
    except AttributeError:
        shape = [data.index.size]
    size = reduce(mul, shape)
    filled = 100 * len(data) / size
    need_to_chunk = size > 1e7 and filled < 1
    info = (name, shape, filled, size, need_to_chunk)
    log.debug(' '.join(map(str, info)))

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
