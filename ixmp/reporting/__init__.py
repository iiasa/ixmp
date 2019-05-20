# Scenario reporting.
#
# Implementation notes:
#
# The core design pattern uses dask graphs; see
# http://docs.dask.org/en/latest/spec.html
# - Reporter.graph is a dictionary where:
#   - keys are strings or ixmp.reporting.Key objects, and
#   - values are 'computations' (the Reporter.add() docstring repeats the
#     definition of computations from the above URL).
# - The results of many internal computations are xarray.DataArray objects,
#   with:
#   - an optional attribute 'unit' describing the units of the object.
#
# TODO meet the requirements:
# A11. Callable through `retixmp`.

from functools import partial
from itertools import chain, repeat
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
from warnings import warn

import dask

# FIXME this causes JPype to segfault
# from dask.threaded import get as dask_get
from dask import get as dask_get

import yaml
import xarray as xr

from .utils import Key, keys_for_quantity, data_for_quantity, ureg
from . import computations
from .computations import (   # noqa:F401
    aggregate,
    disaggregate_shares,
    load_file,
    write_report,
)
from .describe import describe_recursive


class Reporter(object):
    """Class for generating reports on :class:`ixmp.Scenario` objects."""
    # TODO meet the requirements:
    # A3iii. Interpolation.
    # A7. Renaming of outputs.

    #: A dask-format :doc:`graph <graphs>`.
    graph = {}

    #: The default reporting key.
    default_key = None

    def __init__(self, **kwargs):
        self.graph = {}
        self.configure(**kwargs)

    @classmethod
    def from_scenario(cls, scenario, **kwargs):
        """Create a Reporter by introspecting *scenario*.

        Returns
        -------
        Reporter
          …containing:

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
            # List of computations for the quantity and its aggregates
            comps = list(keys_for_quantity(ix_type, name, scenario))

            # Add to the graph
            rep.graph.update(comps)

            # Add first 1 or 2 (equ marginals) keys to the list of all
            # quantities
            full_comps = comps[:(2 if ix_type == 'equ' else 1)]
            all_keys.extend(c[0] for c in full_comps)

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
        - *files*: a :class:`dict` mapping keys to file paths.
        - *alias*: a :class:`dict` mapping aliases to original keys.

        Warns
        -----
        UserWarning
            If *config* contains unrecognized keys.
        """
        config = _config_args(path, config,
                              sections={'default', 'files', 'alias'})

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
            If True (default), *key* must not already exist in the Reporter.

        Raises
        ------
        KeyError
            If `key` is already in the Reporter.
        """
        if strict and key in self.graph:
            raise KeyError(key)
        self.graph[key] = computation

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
        return dask_get(self.graph, key)

    def keys(self):
        return self.graph.keys()

    def finalize(self, scenario):
        """Prepare the Reporter to act on *scenario*.

        The :class:`Scenario <message_ix.Scenario>` object *scenario* is
        associated with the key ``'scenario'``. All subsequent processing will
        act on data from this *scenario*.
        """
        self.graph['scenario'] = scenario

    # ixmp data model manipulations
    def aggregate(self, var, dim_or_dims, tag, weights):
        """Add a computation that aggregates *var* using *weights*.

        Parameters
        ----------
        var: hashable
            Key of the variable to be disaggregated.
        dim_or_dims: str or iterable of str
            Name(s) of the dimension(s) to sum over.
        tag: str
            Additional key tag to add to the end of the variable key.
        weights : xr.DataArray
            Weights for weighted aggregation.

        Returns
        -------
        Key
            The key of the newly-added node.
        """
        dims = [dim_or_dims] if isinstance(dim_or_dims, str) else dim_or_dims

        # Compute the new key
        key = Key.from_str_or_key(var)
        key._dims = list(filter(lambda d: d not in dims, key._dims))
        key._tag = tag

        self.graph[key] = tuple([
            partial(aggregate, dimensions=dims),
            var,
            weights])

        return key

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

        Returns
        -------
        Key
            The key of the newly-added node.
        """
        # Compute the new key
        key = Key.from_str_or_key(var)
        key._dims.append(new_dim)

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

        self.graph[key] = tuple([method, var] + args)

        return key

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
        self.add(key, (partial(load_file, path),), strict=True)
        return key

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
        write_report(self.get(key), path)


def configure(path=None, **config):
    """Configure reporting globally.

    Valid configuration keys include:

    - *units*: a string, passed to :meth:`pint.UnitRegistry.define`.

    Warns
    -----
    UserWarning
        If *config* contains unrecognized keys.
    """
    config = _config_args(path, config)

    # Units
    units = config.get('units', '').strip()
    if units:
        ureg.define(units)


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
