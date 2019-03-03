from copy import deepcopy
from itertools import compress

from .computations import aggregate


def combo_partition(iterable):
    """Yield pairs of lists with all possible subsets of *iterable*."""
    # Format string for binary conversion, e.g. '04b'
    fmt = '0' + str(2 + int(len(iterable) ** 0.5)) + 'b'
    for n in range(2 ** len(iterable) - 1):
        # Two binary lists
        a, b = zip(*[(v, not v) for v in map(int, format(n, fmt))])
        yield compress(iterable, a), compress(iterable, b)


class Key:
    """A hashable key for a quantity that includes its dimensionality.

    Quantities in `ixmp.Scenario` can be indexed by one or more dimensions:

    >>> scenario.init_par('foo', ['a', 'b', 'c'], ['apple', 'bird', 'car'])

    Reporting computations for this `scenario` might use the quantity `foo`:
    1. in its full resolution, i.e. indexed by a, b, and c;
    2. aggregated over any one dimension, e.g. aggregated over c and thus
       indexed by a and b;
    3. aggregated over any two dimensions; etc.

    A Key for (1) will hash, display, and evaluate as equal to 'foo:a-b-c'. A
    key for (2) corresponds to `foo:a-b`, and so forth.

    Keys may be generated concisely by defining a convenience method:

    >>> def foo(dims):
    >>>     return Key('foo', dims.split(''))
    >>> foo('a b')
    foo:a-b

    """
    # TODO add 'method' attribute to describe the method used to perform
    # (dis)aggregation, other manipulation
    # TODO add tags or other information to describe quantities computed
    # multiple ways
    def __init__(self, name, dims=[]):
        self._name = name
        self._dims = dims

    @classmethod
    def from_str_or_key(cls, value):
        if isinstance(value, cls):
            return deepcopy(value)
        else:
            name, dims = value.split(':')
            return cls(name, dims.split('-'))

    def __repr__(self):
        """Representation of the Key, e.g. name:dim1-dim2-dim3."""
        return ':'.join([self._name, '-'.join(self._dims)])

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return repr(self) == other

    def aggregates(self):
        """Yield (key, task) for all possible aggregations of the Key."""
        for agg_dims, others in combo_partition(self._dims):
            yield Key(self._name, agg_dims), \
                (aggregate, hash(self), list(others))
