"""Scenario reporting."""
# The implementation uses the dask graph specfication; see
# http://docs.dask.org/en/latest/spec.html
#
# TODO meet the requirements:
# A8iii. Read CLI arguments for subset reporting.
# A9. Handle units for quantities.
# A11. Callable through `retixmp`.

from copy import deepcopy
from functools import partial
from itertools import compress

from dask.threaded import get as dask_get


# Utility
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


# Computations
def aggregate(var, dims):
    return var.sum(dim=dims)


def disaggregate_shares(var, shares):
    return var * shares


def load_file(path):
    # TODO automatically parse common file formats: yaml, csv, xls(x)
    return open(path).read()


def make_dataframe(*vars):
    """Concatenate *vars* into a single pd.DataFrame."""
    # TODO also rename
    raise NotImplementedError


class Reporter(object):
    """Reporter.

    A Reporter is used to postprocess data from from one or more
    :class:`ixmp.Scenario` objects. :meth:`get` can be used to:
    - Generate an entire *report* composed of multiple quantities. Generating a
      report may trigger output to file(s) or a database.
    - Retrieve individual quantities from a Scenario.

    """
    # TODO meet the requirements:
    # A2. Use exogenous data: given programmatically; from file.
    # A3i. Weighted sums.
    # A3iii. Interpolation.
    # A6. Duplicate or clone existing operations for multiple other sets of
    #     inputs or outputs. [Sub-graph manipulations.]
    # A7. Renaming of outputs.
    # A10. Provide description of how quantities are computed.

    def __init__(self):
        self.graph = {}

    @classmethod
    def from_scenario(cls, scenario):
        """Create a Reporter by introspecting *scenario*.

        The reporter will contain:
        - Every parameter in the *scenario* and all possible aggregations
          across different dimensions.
        """
        # New Reporter
        rep = cls()

        for par in scenario.par_list():
            # TODO retrieve parameter name, dims, and data
            name = NotImplementedError
            dims = NotImplementedError
            data = NotImplementedError

            # Add the parameter itself
            base_key = Key(name, dims)
            cls.add(base_key, data)

            # Add aggregates
            cls.graph.update(base_key.aggregates())

        # TODO add sets, scalars, and equations

        return rep

    # Generic graph manipulations
    def add(self, key, computation, strict=False):
        """Add *computation* to the Reporter under *key*.

        :meth:`add` may be used to:
        - Provide an alias from one *key* to another:

          >>> r.add('aliased name', 'original name')

        - Define an arbitrarily complex computation that operates directly on
          the :class:`ismp.Scenario` being reported:

          >>> def my_report(scenario):
          >>>     # many lines of code
          >>> r.add('my report', (my_report, 'scenario'))
          >>> r.finalize(scenario)
          >>> r.get('my report')

        Parameters
        ----------
        key: hashable
            A string, Key, or other value identifying the output of *task*.
        computation: object
            One of:
            1. any existing *key* in the Reporter.
            2. any other literal value or constant.
            3. a task, i.e. a tuple with a callable followed by one or more
               computations.
            4. A list containing one or more of #1, #2, and/or #3.
        strict : bool, optional
            If True (default), *key* must not already exist in the Reporter.
        """
        if strict and key in self.graph:
            raise KeyError(key)
        self.graph[key] = computation

    def get(self, key):
        """Execute and return the result of the computation *key*.

        Only *key* and its dependencies are computed.
        """
        return dask_get(self.graph, key)

    def finalize(self, scenario):
        """Prepare the Reporter to act on *scenario*."""
        self.graph['scenario'] = scenario

    # ixmp data model manipulations
    def disaggregate(self, var, new_dim, method='shares', args=[]):
        """Add a computation that disaggregates *var* using *method*.

        Parameters
        ----------
        var: hashable
            Key of the variable to be disaggregated.
        new_dim: str
            Name of the new dimension of the disaggregated variable.
        method: callable or str
            Disaggregation method. If a callable, then it is applied to *var*
            with any extra *args*. If then a method named
            'disaggregate_{method}' is used.
        args: list, optional
            Additional arguments to the *method*. The first element should be
            the key for a quantity giving shares for disaggregation.
        """
        # Compute the new key
        key = Key.from_str_or_key(var)
        key._dims.append(new_dim)

        # Get the method
        if isinstance(method, str):
            try:
                method = globals()['disaggregate_{}'.format(method)]
            except KeyError:
                raise ValueError("No disaggregation method 'disaggregate_{}'"
                                 .format(method))
        if not callable(method):
            raise ValueError(method)

        self.graph[key] = tuple([method, var] + args)

    # Convenience methods
    def add_file(self, path):
        self.add('file:{}'.format(path), (partial(load_file, path),))
