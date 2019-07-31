# Scenario reporting.
#
# Implementation notes:
#
# The core design pattern uses dask graphs; see
# http://docs.dask.org/en/latest/spec.html
# - Reporter.graph is a dictionary where:
#   - keys are strings or ixmp.reporting.util.Key objects (which compare/hash
#     equal to their str() representation), and
#   - values are 'computations' (the Reporter.add() docstring repeats the
#     definition of computations from the above URL).
# - The results of 'internal' computations are ixmp.reporting.utils.Quantity
#   objects.
#   - These resemble xarray.DataArray, but currently are ixmp.reporting.utils.
#     AttrSeries, which duck-types DataArray. This is because many ixmp/
#     message_ix quantities are large and sparse, and creating sparse
#     DataArrays is non-trivial; see https://stackoverflow.com/q/56396122/
#   - Internal computations have .attr['_unit'] describing the units of the
#     quantity, to carry these through calculations.

from functools import partial
from itertools import chain, repeat
import logging
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
from warnings import warn

import dask

# FIXME this causes JPype to segfault
# from dask.threaded import get as dask_get
from dask import get as dask_get
from dask.optimization import cull

import yaml

from .utils import Key, keys_for_quantity, rename_dims, replace_units, ureg
from . import computations
from .describe import describe_recursive


log = logging.getLogger(__name__)


class Reporter:
    """Class for generating reports on :class:`ixmp.Scenario` objects."""
    # TODO meet the requirements:
    # A3iii. Interpolation.
    # A7. Renaming of outputs.

    #: A dask-format :doc:`graph <graphs>`.
    graph = {'filters': None}

    #: The default reporting key.
    default_key = None

    # An index of ixmp names -> full keys
    _index = {}

    def __init__(self, **kwargs):
        self.graph = {'filters': None}
        self.configure(**kwargs)

    @classmethod
    def from_scenario(cls, scenario, **kwargs):
        """Create a Reporter by introspecting *scenario*.

        Parameters
        ----------
        scenario : ixmp.Scenario
            Scenario to introspect in creating the Reporter.
        kwargs : optional
            Passed to :meth:`Scenario.configure`.

        Returns
        -------
        :class:`Reporter <ixmp.reporting.Reporter>`
            A Reporter instance containing:

            - A 'scenario' key referring to the *scenario* object.
            - Each parameter, equation, and variable in the *scenario*.
            - All possible aggregations across different sets of dimensions.
            - Each set in the *scenario*.
        """
        # New Reporter
        rep = cls(**kwargs)

        # List of top-level keys
        all_keys = []

        # List of parameters, equations, and variables
        quantities = chain(
            zip(repeat('par'), sorted(scenario.par_list())),
            zip(repeat('equ'), sorted(scenario.equ_list())),
            zip(repeat('var'), sorted(scenario.var_list())),
        )

        for ix_type, name in quantities:
            # List of computations for the quantity and its partial sums
            comps = list(keys_for_quantity(ix_type, name, scenario))

            # Add to the graph
            rep.graph.update(comps)

            # Add first 1 or 2 (equ marginals) keys to the list of all
            # quantities
            full_comps = comps[:(2 if ix_type == 'equ' else 1)]
            all_keys.extend(c[0] for c in full_comps)

            # Index the variable name
            rep._index[name] = full_comps[0][0]

        # Add a key which simply collects all quantities
        rep.add('all', sorted(all_keys))

        # Add sets
        for name in scenario.set_list():
            elements = scenario.set(name)
            try:
                elements = elements.tolist()
            except AttributeError:
                # pd.DataFrame for a multidimensional set; store as-is
                pass
            rep.add(name, elements)

        # Add the scenario itself
        rep.add('scenario', scenario)

        return rep

    def read_config(self, path):
        """Configure the Reporter with information from a YAML file at *path*.

        See :meth:`configure`.
        """
        path = Path(path)
        with open(path, 'r') as f:
            self.configure(config_dir=path.parent, **yaml.load(f))

    def configure(self, path=None, **config):
        """Configure the Reporter.

        Accepts a *path* to a configuration file and/or keyword arguments.
        Configuration keys loaded from file are replaced by keyword arguments.

        Valid configuration keys include:

        - *default*: the default reporting key; sets :attr:`default_key`.
        - *filters*: a :class:`dict`, passed to :meth:`set_filters`.
        - *files*: a :class:`dict` mapping keys to file paths.
        - *alias*: a :class:`dict` mapping aliases to original keys.

        Warns
        -----
        UserWarning
            If *config* contains unrecognized keys.
        """
        sections = {'default', 'files', 'alias', 'filters'}
        config = _config_args(path, config, sections=sections)

        config_dir = config.pop('config_dir', '.')

        # Read sections

        # Default report
        try:
            self.default_key = config['default']
        except KeyError:
            pass

        # Files with exogenous data
        for key, path in config.get('files', {}).items():
            path = Path(path)
            if not path.is_absolute():
                # Resolve relative paths relative to the directory containing
                # the configuration file
                path = config_dir / path
            self.add_file(path, key)

        # Aliases
        for alias, original in config.get('alias', {}).items():
            self.add(alias, original)

        # Filters
        self.set_filters(**config.pop('filters', {}))

        return self  # to allow chaining

    # Generic graph manipulations
    def add(self, key, computation, strict=False):
        """Add *computation* to the Reporter under *key*.

        Parameters
        ----------
        key: hashable
            A string, Key, or other value identifying the output of *task*.
        computation: object
            One of:

            1. any existing key in the Reporter.
            2. any other literal value or constant.
            3. a task, i.e. a tuple with a callable followed by one or more
               computations.
            4. A list containing one or more of #1, #2, and/or #3.
        strict : bool, optional
            If True, *key* must not already exist in the Reporter, and
            any keys referred to by *computation* must exist.

        Raises
        ------
        KeyError
            If `key` is already in the Reporter, or any key referred to by
            `computation` does not exist.
        """
        if strict:
            # Key already exists in graph
            if key in self.graph:
                raise KeyError(key)

            # Check that keys used in *computation* are in the graph
            keylike = filter(lambda e: isinstance(e, (str, Key)), computation)
            self.check_keys(*keylike)

        self.graph[key] = computation

        return key

    def apply(self, generator, *keys):
        """Add computations from `generator` applied to `key`.

        Parameters
        ----------
        generator : callable
            Function to apply to `keys`. Must yield a sequence (0 or more) of
            (`key`, `computation`), which are added to the :attr:`graph`.
        keys : hashable
            The starting key(s)
        """
        keys = self.check_keys(*keys)
        try:
            self.graph.update(generator(*keys))
        except TypeError as e:
            if e.args[0] == "'NoneType' object is not iterable":
                pass
            else:
                raise

    def get(self, key=None):
        """Execute and return the result of the computation *key*.

        Only *key* and its dependencies are computed.

        Parameters
        ----------
        key : str, optional
            If not provided, :attr:`default_key` is used.

        Raises
        ------
        ValueError
            If `key` and :attr:`default_key` are both :obj:`None`.
        """
        if key is None:
            if self.default_key is not None:
                key = self.default_key
            else:
                raise ValueError('no default reporting key set')

        # Cull the graph, leaving only those needed to compute *key*
        dsk, deps = cull(self.graph, key)
        log.debug('Cull {} -> {} keys'.format(len(self.graph), len(dsk)))

        return dask_get(dsk, key)

    def keys(self):
        return self.graph.keys()

    def full_key(self, name):
        """Return the full-dimensionality key for *name*.

        An ixmp variable 'foo' indexed by a, c, n, q, and x is available in the
        Reporter at ``'foo:a-c-n-q-x'``. ``full_key('foo')`` retrieves this
        :class:`Key <ixmp.reporting.utils.Key>`.
        """
        return self._index[name]

    def check_keys(self, *keys):
        """Check that *keys* are in the Reporter.

        If any of *keys* is not in the Reporter, KeyError is raised.
        Otherwise, a list is returned with either the key from *keys*, or the
        corresponding :meth:`full_key`.
        """
        result = []
        missing = []

        # Process all keys to produce more useful error messages
        for key in keys:
            # Add the key directly if it is in the graph
            if key in self.graph:
                result.append(key)
                continue

            # Try adding the full key
            try:
                result.append(self._index[key])
            except KeyError:
                missing.append(key)

        if len(missing):
            raise KeyError(missing)

        return result

    def __contains__(self, name):
        return name in self.graph

    def finalize(self, scenario):
        """Prepare the Reporter to act on *scenario*.

        The :class:`Scenario <message_ix.Scenario>` object *scenario* is
        associated with the key ``'scenario'``. All subsequent processing will
        act on data from this *scenario*.
        """
        self.graph['scenario'] = scenario

    def set_filters(self, **filters):
        """Apply *filters* ex ante (before computations occur).

        *filters* has the same form as the argument of the same name to
        :meth:`ixmp.Scenario.par` and analogous methods. A value of
        :const:`None` will clear the filter for the named dimension.
        """
        if self.graph['filters'] is None:
            self.graph['filters'] = {}

        # Update
        self.graph['filters'].update(filters)

        # Clear
        for key, value in filters.items():
            if value is None:
                self.graph['filters'].pop(key, None)

    # ixmp data model manipulations
    def add_product(self, name, *quantities, sums=True):
        """Add a computation that takes the product of *quantities*.

        Parameters
        ----------
        name : str
            Name of the new quantity.
        sums : bool, optional
            If :obj:`True`, all partial sums of the new quantity are also
            added.

        Returns
        -------
        Key
            The full key of the new quantity.
        """
        # Fetch the full key for each quantity
        base_keys = self.check_keys(*quantities)

        # Compute a key for the result
        key = Key.product(name, *base_keys)

        # Add the basic product to the graph and index
        self.add(key, tuple([computations.product] + base_keys))
        self._index[name] = key

        if sums:
            # Add partial sums of the product
            self.apply(key.iter_sums)

        return key

    def aggregate(self, qty, tag, dims_or_groups, weights=None, keep=True):
        """Add a computation that aggregates *qty*.

        Parameters
        ----------
        qty: :class:`Key` or str
            Key of the quantity to be disaggregated.
        tag: str
            Additional string to add to the end the key for the aggregated
            quantity.
        dims_or_groups: str or iterable of str or dict
            Name(s) of the dimension(s) to sum over, or nested dict.
        weights : xr.DataArray
            Weights for weighted aggregation.

        Returns
        -------
        :class:`Key`
            The key of the newly-added node.
        """
        if isinstance(dims_or_groups, dict):
            groups = dims_or_groups
            if len(groups) > 1:
                raise NotImplementedError('aggregate() along >1 dimension')

            key = Key.from_str_or_key(qty, tag=tag)
            comp = (computations.aggregate, qty, groups, keep)
        else:
            dims = dims_or_groups
            if isinstance(dims, str):
                dims = [dims]

            key = Key.from_str_or_key(qty, drop=dims, tag=tag)
            comp = (partial(computations.sum, dimensions=dims), qty, weights)

        return self.add(key, comp, True)

    def disaggregate(self, qty, new_dim, method='shares', args=[]):
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

        Returns
        -------
        Key
            The key of the newly-added node.
        """
        # Compute the new key
        key = Key.from_str_or_key(qty, append=new_dim)

        # Get the method
        if isinstance(method, str):
            try:
                method = getattr(computations,
                                 'disaggregate_{}'.format(method))
            except AttributeError:
                raise ValueError("No disaggregation method 'disaggregate_{}'"
                                 .format(method))
        if not callable(method):
            raise ValueError(method)

        return self.add(key, tuple([method, qty] + args), True)

    # Convenience methods
    def add_file(self, path, key=None):
        """Add exogenous quantities from *path*.

        A file at a path like '/path/to/foo.ext' is added at the key
        ``'file:foo.ext'``.

        See also
        --------
        ixmp.reporting.computations.load_file
        """
        key = key if key else 'file:{}'.format(path.name)
        return self.add(key, (partial(computations.load_file, path),), True)

    def describe(self, key=None):
        """Return a string describing the computations that produce *key*.

        If *key* is not provided, all keys in the Reporter are described.
        """
        # TODO allow key to be an iterable of keys
        if key is None:
            # Sort with 'all' at the end
            key = tuple(sorted(filter(lambda k: k != 'all',
                                      self.graph.keys())) + ['all'])
        else:
            key = (key,)
        return describe_recursive(self.graph, key) + '\n'

    def visualize(self, filename, **kwargs):
        """Generate an image describing the reporting structure.

        This is a shorthand for :meth:`dask.visualize`. Requires
        `graphviz <https://pypi.org/project/graphviz/>`__.
        """
        return dask.visualize(self.graph, filename=filename, **kwargs)

    def write(self, key, path):
        """Write the report *key* to the file *path*."""
        # Call the method directly without adding it to the graph
        computations.write_report(self.get(key), path)


def configure(path=None, **config):
    """Configure reporting globally.

    Valid configuration keys include:

    - *units*:

      - *define*: a string, passed to :meth:`pint.UnitRegistry.define`.
      - *replace*: a mapping from str to str, used to replace units before they
        are parsed by pints

    Warns
    -----
    UserWarning
        If *config* contains unrecognized keys.
    """
    config = _config_args(path, config)

    # Units
    units = config.get('units', {})

    # Define units
    if 'define' in units:
        ureg.define(units['define'].strip())

    # Add replacements
    for old, new in units.get('replace', {}).items():
        replace_units[old] = new

    # Dimensions to be renamed
    rename_dims.update(config.get('rename_dims', {}))


def _config_args(path, keys, sections={}):
    """Handle configuration arguments."""
    if path:
        path = Path(path)
        with open(path, 'r') as f:
            result = yaml.load(f)

        # Also store the directory where the configuration file was located
        result['config_dir'] = path.parent
    else:
        result = {}

    result.update(keys)

    if sections:
        extra_sections = set(result.keys()) - sections - {'config_dir'}
        if len(extra_sections):
            warn(('Unrecognized sections {!r} in reporting configuration will '
                  'have no effect').format(extra_sections))

    return result
