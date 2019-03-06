"""Scenario reporting."""
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
# A8iii. Read CLI arguments for subset reporting.
# A9. Handle units for quantities.
# A11. Callable through `retixmp`.

from itertools import chain, repeat
from functools import partial

from dask.threaded import get as dask_get
import pandas as pd
import xarray as xr

from .utils import Key
from . import computations
from .computations import (   # noqa:F401
    disaggregate_shares,
    load_file,
    write_report,
)


def quantity_as_xr(scenario, name, kind='par'):
    """Retrieve quantity *name* from *scenario* as an xr.Dataset.

    Parameters
    ----------
    *kind* : 'par' or 'equ'
        Type of quantity to be retrieved.

    Returns
    -------
    dict of :class:'xarray.DataArray'
        Dictionary keys are 'level' (kind='par') or ('lvl', 'mrg')
        (kind='equ').
    """
    # NB this could be moved to ixmp.Scenario
    data = getattr(scenario, kind)(name)

    if isinstance(data, dict):
        # ixmp/GAMS scalar is not returned as pd.DataFrame
        data = pd.DataFrame.from_records([data])

    # Remove the unit from the DataFrame
    try:
        unit = pd.unique(data.pop('unit'))
        assert len(unit) == 1
        attrs = {'unit': unit[0]}
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

    [dims.remove(col) for col in value_columns]

    # Set index if 1 or more dimensions
    if len(dims):
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

        Returns
        -------
        Reporter
          …containing:

          - A 'scenario' key referring to the *scenario* object.
          - Each parameter, equation, and variable in the *scenario*.
          - All possible aggregations across different sets of dimensions.
        """
        # New Reporter
        rep = cls()

        all_keys = []

        # Iterate over parameters and equations together
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

        # TODO add sets

        # Add a target which simply collects all raw data.
        rep.add('all', all_keys)

        return rep

    def read_config(self, path):
        """Configure the Reporter with information from a file at *path*."""
        raise NotImplementedError

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
    def add_file(self, path):
        """Add exogenous quantities from *path*.

        A file at a path like '/path/to/foo.ext' is added at the key
        ``'file:foo.ext'``. See
        :meth:`load_file <ixmp.reporting.computations.load_file>`.
        """
        self.add('file:{}'.format(path.name), (partial(load_file, path),),
                 strict=True)

    def write(self, key, path):
        """Write the report *key* to the file *path*."""
        # Call the method directly without adding it to the graph
        write_report(self.get(key), path)
