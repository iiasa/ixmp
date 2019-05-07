from copy import deepcopy
from functools import partial
from itertools import compress
from logging import getLogger
from math import ceil

import pandas as pd
import pint
import xarray as xr

log = getLogger(__name__)

ureg = pint.UnitRegistry()



def combo_partition(iterable):
    """Yield pairs of lists with all possible subsets of *iterable*."""
    # Format string for binary conversion, e.g. '04b'
    fmt = '0{}b'.format(ceil(len(iterable) ** 0.5))
    for n in range(2 ** len(iterable) - 1):
        # Two binary lists
        a, b = zip(*[(v, not v) for v in map(int, format(n, fmt))])
        yield list(compress(iterable, a)), list(compress(iterable, b))


class Key:
    """A hashable key for a quantity that includes its dimensionality."""
    # TODO cache repr() and only recompute when name/dims changed
    def __init__(self, name, dims=[], tag=None):
        self._name = name
        self._dims = dims
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
    return input_string.strip('[]').replace('%', 'percent')


def collect_units(*args):
    for arg in args:
        if '_unit' in arg.attrs:
            # Convert units if necessary
            if isinstance(arg.attrs['_unit'], str):
                arg.attrs['_unit'] = ureg.parse_units(arg.attrs['_unit'])
        else:
            log.info('assuming {} is unitless'.format(arg))
            arg.attrs['_unit'] = ureg.parse_units('')

    return [arg.attrs['_unit'] for arg in args]


# Mapping from raw → preferred dimension names
rename_dims = {}


def quantity_as_xr(scenario, name, kind='par'):
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
    data = getattr(scenario, kind)(name)

    if isinstance(data, dict):
        # ixmp/GAMS scalar is not returned as pd.DataFrame
        data = pd.DataFrame.from_records([data])

    # Remove the unit from the DataFrame
    try:
        # Ensure there is only one type of unit defined
        unit = pd.unique(data.pop('unit'))
        assert len(unit) <= 1
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
            log.info(f'Definining units for quantity {name}: {definition}')
            ureg.define(definition)
            unit = ureg.parse_units(unit)

        # Store
        attrs = {'_unit': unit}
    except KeyError:
        # 'equ' are returned without units
        attrs = {}

    # List of the dimensions
    dims = data.columns.tolist()

    # Columns containing values
    value_columns = {
        'par': ['value'],
        'equ': ['lvl', 'mrg'],
        'var': ['lvl', 'mrg'],
    }[kind]

    # Dimension columns are the remainder
    [dims.remove(col) for col in value_columns]

    # Rename dimensions
    dims = [rename_dims.get(d, d) for d in dims]

    # Set index if 1 or more dimensions
    if len(dims):
        # First rename, then set index
        data.rename(columns=rename_dims, inplace=True)
        data.set_index(dims, inplace=True)

    # Convert to a series, then Dataset
    ds = xr.Dataset.from_dataframe(data)
    try:
        # Remove length-1 dimensions for scalars
        ds = ds.squeeze('index', drop=True)
    except KeyError:
        pass

    # Assign attributes (units) and name to each xr.DataArray individually
    return {col: ds[col].assign_attrs(attrs).rename(name)
            for col in value_columns}
