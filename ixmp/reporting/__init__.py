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
from dask.threaded import get as dask_get
import yaml
import xarray as xr

from .utils import quantity_as_xr, Key
from . import computations
from .computations import (   # noqa:F401
    disaggregate_shares,
    load_file,
    write_report,
)


class Reporter(object):
    """Class for generating reports on :class:`ixmp.Scenario` objects."""
    # TODO meet the requirements:
    # A3i. Weighted sums.
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

        all_keys = []

        # Add parameters, equations, and variables to the graph
        quantities = chain(
            zip(scenario.par_list(), repeat('par')),
            zip(scenario.equ_list(), repeat('equ')),
            zip(scenario.var_list(), repeat('var')),
        )

        for name, kind in quantities:
            # Retrieve data
            data = quantity_as_xr(scenario, name, kind)

            # Add the full-resolution quantity
            base_data = data['value' if kind == 'par' else 'lvl']
            base_key = Key(name, base_data.coords.keys())
            rep.add(base_key, base_data)

            all_keys.append(base_key)

            # Add aggregates
            rep.graph.update(base_key.aggregates())

            if kind == 'equ':
                # Add the marginal values at full resolution
                mrg_data = data['mrg'].rename('{}-margin'.format(name))
                mrg_key = Key('{}-margin'.format(name),
                              mrg_data.coords.keys())
                rep.add(mrg_key, mrg_data)

                all_keys.append(mrg_key)

                # (No aggregates for marginals)

        # Add a key which simply collects all quantities
        rep.add('all', all_keys)

        # Add sets
        for name in scenario.set_list():
            elements = scenario.set(name).tolist()
            rep.add(name, elements)

        return rep

    def read_config(self, path):
        """Configure the Reporter with information from a YAML file at *path*.

        See :meth:`configure`.
        """
        path = Path(path)
        with open(path, 'r') as f:
            self.configure(config_dir=path.parent, **yaml.load(f))

    def configure(self, **config):
        """Configure the Reporter.

        Valid configuration keys include:

        - *default*: the default reporting key; sets :attr:`default_key`.
        - *files*: a :class:`dict` mapping keys to file paths.
        - *alias*: a :class:`dict` mapping aliases to original keys.

        Warns
        -----
        UserWarning
            If *config* contains unrecognized keys.
        """
        config_dir = config.pop('config_dir', '.')

        # Read sections
        extra_sections = set(config.keys()) - {'default', 'files', 'alias'}
        if len(extra_sections):
            warn(('Unrecognized sections {!r} in reporting configuration will '
                  'have no effect').format(extra_sections))

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
                method = getattr(computations,
                                 'disaggregate_{}'.format(method))
            except AttributeError:
                raise ValueError("No disaggregation method 'disaggregate_{}'"
                                 .format(method))
        if not callable(method):
            raise ValueError(method)

        self.graph[key] = tuple([method, var] + args)

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
        return self._describe(key) + '\n'

    def _describe(self, comp, depth=0, seen=set()):
        """Recursive helper for :meth:`describe`.

        Parameters
        ----------
        comp :
            A dask computation.
        depth : int
            Recursion depth. Used for indentation.
        seen : set
            Keys that have already been described. Used to avoid
            double-printing.
        """
        comp = comp if isinstance(comp, tuple) else (comp,)

        indent = (' ' * 2 * (depth - 1)) + ('- ' if depth > 0 else '')

        result = []
        for arg in comp:
            # Don't fully reprint keys and their ancestors that have been seen
            try:
                if arg in seen:
                    if depth > 0:
                        # Don't print top-level items that have been seen
                        result.append(f"{indent}'{arg}' (above)")
                    continue
            except TypeError:
                pass

            # Convert various types of arguments to string
            if isinstance(arg, xr.DataArray):
                # DataArray → just the first line of the string representation
                item = str(arg).split('\n')[0]
            elif isinstance(arg, partial):
                # functools.partial → less verbose format
                fn_name = arg.func.__name__
                fn_args = ', '.join(chain(
                    map(str, arg.args),
                    map('{0[0]}={0[1]}'.format, arg.keywords)))
                item = f'{fn_name}({fn_args}, ...)'
            elif isinstance(arg, (str, Key)) and arg in self.graph:
                # key that exists in the graph → recurse
                item = "'{}':\n{}".format(
                    arg,
                    self._describe(self.graph[arg], depth + 1, seen))
                seen.add(arg)
            elif isinstance(arg, list) and arg[0] in self.graph:
                item = "list of:\n{}".format(
                    self._describe(tuple(arg), depth + 1, seen))
                seen |= set(arg)
            else:
                item = str(arg)

            result.append(indent + item)

        # Combine items
        return ('\n' if depth > 0 else '\n\n').join(result)

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


def parse_reporting_args(args):
    """Parse command-line arguments."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='reporting configuration file')
    parser.add_argument('--default', help='default reporting key')

    return parser.parse_known_args(args)
