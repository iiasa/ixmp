# Scenario reporting.
#
# Implementation notes:
#
# The core design pattern uses dask graphs; see
# http://docs.dask.org/en/latest/spec.html
# - Reporter.graph is a dictionary where:
#   - keys are strings or ixmp.reporting.key.Key objects (which compare/hash
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
#
# - Always call pint.get_application_registry() from *within* functions
#   (instead of in global scope); this allows downstream code to change which
#   registry is used.
#   - The top-level methods pint.Quantity() and pint.Unit() can also be used;
#     these use the application registry.

import logging
from functools import partial
from inspect import signature
from itertools import chain, repeat
from pathlib import Path
from typing import Dict, Union

import dask
import pint
import yaml
from dask import get as dask_get  # NB dask.threaded.get causes JPype to segfault
from dask.optimization import cull

from ixmp.utils import partial_split

from . import computations
from .describe import describe_recursive
from .exceptions import ComputationError
from .key import Key
from .quantity import Quantity
from .utils import RENAME_DIMS, REPLACE_UNITS, dims_for_qty

__all__ = [
    "Key",
    "Quantity",
    "Reporter",
    "configure",
]

log = logging.getLogger(__name__)


class KeyExistsError(KeyError):
    def __str__(self):
        return f"key {repr(self.args[0])} already exists"


class MissingKeyError(KeyError):
    def __str__(self):
        return f"required keys {repr(self.args)} not defined"


class Reporter:
    """Class for generating reports on :class:`ixmp.Scenario` objects."""

    # TODO meet the requirements:
    # A3iii. Interpolation.

    #: A dask-format :doc:`graph <graphs>`.
    graph: Dict[str, Union[str, dict]] = {"config": {}}

    #: The default reporting key.
    default_key = None

    # An index of ixmp names -> full keys
    _index: Dict[str, Key] = {}

    # Module containing pre-defined computations
    _computations = computations

    def __init__(self, **kwargs):
        self.graph = {"config": {}}
        self._index = {}
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

        # Add the scenario itself
        rep.add("scenario", scenario)

        # List of top-level keys
        all_keys = []

        # List of parameters, equations, and variables
        quantities = chain(
            zip(repeat("par"), sorted(scenario.par_list())),
            zip(repeat("equ"), sorted(scenario.equ_list())),
            zip(repeat("var"), sorted(scenario.var_list())),
        )

        for ix_type, name in quantities:
            # List of computations for the quantity and maybe its marginals
            comps = keys_for_quantity(ix_type, name, scenario)

            # Add to the graph and index, including sums
            rep.add(*comps[0], strict=True, index=True, sums=True)

            try:
                # Add any marginals, but without sums
                rep.add(*comps[1], strict=True, index=True)
            except IndexError:
                pass  # Not an equ/var with marginals

            # Add keys to the list of all quantities
            all_keys.extend(c[0] for c in comps)

        # Add a key which simply collects all quantities
        rep.add("all", sorted(all_keys))

        # Add sets
        for name in scenario.set_list():
            elements = scenario.set(name)
            try:
                # Convert Series to list; protect list so that dask schedulers
                # do not try to interpret its contents as further tasks
                elements = dask.core.quote(elements.tolist())
            except AttributeError:
                # pd.DataFrame for a multidimensional set; store as-is
                pass

            rep.add(RENAME_DIMS.get(name, name), elements)

        return rep

    def configure(self, path=None, **config):
        """Configure the Reporter.

        Accepts a *path* to a configuration file and/or keyword arguments.
        Configuration keys loaded from file are replaced by keyword arguments.

        Valid configuration keys include:

        - *default*: the default reporting key; sets :attr:`default_key`.
        - *filters*: a :class:`dict`, passed to :meth:`set_filters`.
        - *files*: a :class:`list` where every element is a :class:`dict`
          of keyword arguments to :meth:`add_file`.
        - *alias*: a :class:`dict` mapping aliases to original keys.

        Warns
        -----
        UserWarning
            If *config* contains unrecognized keys.
        """
        # Maybe load from a path
        config = _config_args(path, config)

        # Pass to global configuration
        configure(None, **config)

        # Store all configuration in the graph itself
        self.graph["config"] = config.copy()

        # Read sections

        # Default report
        try:
            self.default_key = config["default"]
        except KeyError:
            pass

        # Files with exogenous data
        for item in config.get("files", []):
            path = Path(item["path"])
            if not path.is_absolute():
                # Resolve relative paths relative to the directory containing
                # the configuration file
                path = config.get("config_dir", Path.cwd()) / path
            item["path"] = path

            self.add_file(**item)

        # Aliases
        for alias, original in config.get("alias", {}).items():
            self.add(alias, original)

        # Filters
        self.set_filters(**config.get("filters", {}))

        return self  # to allow chaining

    def add(self, data, *args, **kwargs):
        """General-purpose method to add computations.

        :meth:`add` can be called in several ways; its behaviour depends on
        `data`; see below. It chains to methods such as :meth:`add_single`,
        :meth:`add_queue`, and :meth:`apply`, which can also be called
        directly.

        Parameters
        ----------
        data, args : various

        Other parameters
        ----------------
        sums : bool, optional
            If :obj:`True`, all partial sums of the key `data` are also added
            to the Reporter.

        Returns
        -------
        list of Key-like
            Some or all of the keys added to the Reporter.

        Raises
        ------
        KeyError
            If a target key is already in the Reporter; any key referred to by
            a computation does not exist; or ``sums=True`` and the key for one
            of the partial sums of `key` is already in the Reporter.

        See also
        ---------
        add_single
        add_queue
        apply
        """
        if isinstance(data, list):
            # A list. Use add_queue to add
            return self.add_queue(data, *args, **kwargs)

        elif isinstance(data, str) and data in dir(self._computations):
            # *data* is the name of a pre-defined computation
            name = data

            if hasattr(self, f"add_{name}"):
                # Use a method on the current class to add. This invokes any
                # argument-handling conveniences, e.g. Reporter.add_product()
                # instead of using the bare product() computation directly.
                return getattr(self, f"add_{name}")(*args, **kwargs)
            else:
                # Get the function directly
                func = getattr(self._computations, name)
                # Rearrange arguments: key, computation function, args, …
                func, kwargs = partial_split(func, kwargs)
                return self.add(args[0], func, *args[1:], **kwargs)

        elif isinstance(data, str) and data in dir(self):
            # Name of another method, e.g. 'apply'
            return getattr(self, data)(*args, **kwargs)

        elif isinstance(data, (str, Key)):
            # *data* is a key, *args* are the computation
            key, computation = data, args

            if kwargs.pop("sums", False):
                # Convert *key* to a Key object in order to use .iter_sums()
                key = Key.from_str_or_key(key)

                # Iterable of computations
                # print((tuple([key] + list(computation)), kwargs))
                # print([(c, {}) for c in key.iter_sums()])
                to_add = chain(
                    # The original
                    [(tuple([key] + list(computation)), kwargs)],
                    # One entry for each sum
                    [(c, {}) for c in key.iter_sums()],
                )

                return self.add_queue(to_add)
            else:
                # Add a single computation (without converting to Key)
                return self.add_single(key, *computation, **kwargs)
        else:
            # Some other kind of import
            raise ValueError(data)

    def add_queue(self, queue, max_tries=1, fail="raise"):
        """Add tasks from a list or `queue`.

        Parameters
        ----------
        queue : list of 2-tuple
            The members of each tuple are the arguments (i.e. a list or tuple)
            and keyword arguments (i.e. a dict) to :meth:`add`.
        max_tries : int, optional
            Retry adding elements up to this many times.
        fail : 'raise' or log level, optional
            Action to take when a computation from `queue` cannot be added
            after `max_tries`.
        """
        # Elements to retry: list of (tries, args, kwargs)
        retry = []
        added = []

        # Iterate over elements from queue, then from retry. On the first pass,
        # count == 1; on subsequent passes, it is incremented.
        for count, (args, kwargs) in chain(zip(repeat(1), queue), retry):
            try:
                # Recurse
                added.append(self.add(*args, **kwargs))
            except KeyError as exc:
                # Adding failed

                # Information for debugging
                info = [
                    f"Failed {count} times to add:",
                    f"    ({repr(args)}, {repr(kwargs)})",
                    f"    with {repr(exc)}",
                ]

                def _log(level):
                    [log.log(level, i) for i in info]

                if count < max_tries:
                    _log(logging.DEBUG)
                    # This may only be due to items being out of order, so
                    # retry silently
                    retry.append((count + 1, (args, kwargs)))
                else:
                    # More than *max_tries* failures; something
                    if fail == "raise":
                        _log(logging.ERROR)
                        raise
                    else:
                        _log(getattr(logging, fail.upper()))

        return added

    # Generic graph manipulations
    def add_single(self, key, *computation, strict=False, index=False):
        """Add a single *computation* at *key*.

        Parameters
        ----------
        key : str or Key or hashable
            A string, Key, or other value identifying the output of *task*.
        computation : object
            Any dask computation, i.e. one of:

            1. any existing key in the Reporter.
            2. any other literal value or constant.
            3. a task, i.e. a tuple with a callable followed by one or more
               computations.
            4. A list containing one or more of #1, #2, and/or #3.

        strict : bool, optional
            If True, *key* must not already exist in the Reporter, and
            any keys referred to by *computation* must exist.
        index : bool, optional
            If True, *key* is added to the index as a full-resolution key, so
            it can be later retrieved with :meth:`full_key`.
        """
        if len(computation) == 1:
            # Unpack a length-1 tuple
            computation = computation[0]

        if strict:
            if key in self.graph:
                # Key already exists in graph
                raise KeyExistsError(key)

            # Check that keys used in *comp* are in the graph
            keylike = filter(lambda e: isinstance(e, (str, Key)), computation)
            self.check_keys(*keylike)

        if index:
            # String equivalent of *key* with all dimensions dropped, but name
            # and tag retained
            idx = str(Key.from_str_or_key(key, drop=True)).rstrip(":")

            # Add *key* to the index
            self._index[idx] = key

        # Add to the graph
        self.graph[key] = computation

        return key

    def apply(self, generator, *keys, **kwargs):
        """Add computations by applying `generator` to `keys`.

        Parameters
        ----------
        generator : callable
            Function to apply to `keys`.
        keys : hashable
            The starting key(s).
        kwargs
            Keyword arguments to `generator`.
        """
        args = self.check_keys(*keys)

        try:
            # Inspect the generator function
            par = signature(generator).parameters
            # Name of the first parameter
            par_0 = list(par.keys())[0]
        except IndexError:
            pass  # No parameters to generator
        else:
            if issubclass(par[par_0].annotation, Reporter):
                # First parameter wants a reference to the Reporter object
                args.insert(0, self)

        # Call the generator. Might return None, or yield some computations
        applied = generator(*args, **kwargs)

        if applied:
            # Update the graph with the computations
            self.graph.update(applied)

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
                raise ValueError("no default reporting key set")

        # Cull the graph, leaving only those needed to compute *key*
        dsk, deps = cull(self.graph, key)
        log.debug("Cull {} -> {} keys".format(len(self.graph), len(dsk)))

        try:
            # Protect 'config' dict, so that dask schedulers do not try to
            # interpret its contents as further tasks. Workaround for
            # https://github.com/dask/dask/issues/3523
            dsk["config"] = dask.core.quote(dsk["config"])
        except KeyError:
            pass

        try:
            return dask_get(dsk, key)
        except Exception as exc:
            raise ComputationError(exc) from None

    def keys(self):
        """Return the keys of :attr:`graph`."""
        return self.graph.keys()

    def full_key(self, name_or_key):
        """Return the full-dimensionality key for *name_or_key*.

        An ixmp variable 'foo' with dimensions (a, c, n, q, x) is available in
        the Reporter as ``'foo:a-c-n-q-x'``. This :class:`Key
        <ixmp.reporting.utils.Key>` can be retrieved with::

            rep.full_key('foo')
            rep.full_key('foo:c')
            # etc.
        """
        name = str(Key.from_str_or_key(name_or_key, drop=True)).rstrip(":")
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
            raise MissingKeyError(*missing)

        return result

    def __contains__(self, name):
        return name in self.graph

    def finalize(self, scenario):
        """Prepare the Reporter to act on *scenario*.

        The :class:`Scenario <message_ix.Scenario>` object *scenario* is
        associated with the key ``'scenario'``. All subsequent processing will
        act on data from this *scenario*.
        """
        self.graph["scenario"] = scenario

    def set_filters(self, **filters):
        """Apply *filters* ex ante (before computations occur).

        Filters are stored in the reporter at the ``'filters'`` key, and are
        passed to :meth:`ixmp.Scenario.par` and similar methods. All quantity
        values read from the Scenario are filtered *before* any other
        computations take place.

        Parameters
        ----------
        filters : mapping of str → (list of str or None)
            Argument names are dimension names; values are lists of allowable
            labels along the respective dimension, *or* None to clear any
            existing filters for the dimension.

            If no arguments are provided, *all* filters are cleared.
        """
        self.graph["config"].setdefault("filters", {})

        if len(filters) == 0:
            self.graph["config"]["filters"] = {}

        # Update
        self.graph["config"]["filters"].update(filters)

        # Clear
        for key, value in filters.items():
            if value is None:
                self.graph["config"]["filters"].pop(key, None)

    # ixmp data model manipulations
    def add_product(self, key, *quantities, sums=True):
        """Add a computation that takes the product of *quantities*.

        Parameters
        ----------
        key : str or Key
            Key of the new quantity. If a Key, any dimensions are ignored; the
            dimensions of the product are the union of the dimensions of
            *quantities*.
        sums : bool, optional
            If :obj:`True`, all partial sums of the new quantity are also
            added.

        Returns
        -------
        :class:`Key`
            The full key of the new quantity.
        """
        # Fetch the full key for each quantity
        base_keys = list(map(Key.from_str_or_key, self.check_keys(*quantities)))

        # Compute a key for the result
        # Parse the name and tag of the target
        key = Key.from_str_or_key(key)
        # New key with dimensions of the product
        key = Key.product(key.name, *base_keys, tag=key.tag)

        # Add the basic product to the graph and index
        keys = self.add(key, computations.product, *base_keys, sums=sums, index=True)

        return keys[0]

    def aggregate(self, qty, tag, dims_or_groups, weights=None, keep=True, sums=False):
        """Add a computation that aggregates *qty*.

        Parameters
        ----------
        qty: :class:`Key` or str
            Key of the quantity to be aggregated.
        tag: str
            Additional string to add to the end the key for the aggregated
            quantity.
        dims_or_groups: str or iterable of str or dict
            Name(s) of the dimension(s) to sum over, or nested dict.
        weights : :class:`xarray.DataArray`, optional
            Weights for weighted aggregation.
        keep : bool, optional
            Passed to :meth:`computations.aggregate
            <imxp.reporting.computations.aggregate>`.
        sums : bool, optional
            Passed to :meth:`add`.

        Returns
        -------
        :class:`Key`
            The key of the newly-added node.
        """
        # TODO maybe split this to two methods?
        if isinstance(dims_or_groups, dict):
            groups = dims_or_groups
            if len(groups) > 1:
                raise NotImplementedError("aggregate() along >1 dimension")

            key = Key.from_str_or_key(qty, tag=tag)
            comp = (computations.aggregate, qty, groups, keep)
        else:
            dims = dims_or_groups
            if isinstance(dims, str):
                dims = [dims]

            key = Key.from_str_or_key(qty, drop=dims, tag=tag)
            comp = (partial(computations.sum, dimensions=dims), qty, weights)

        return self.add(key, comp, strict=True, index=True, sums=sums)

    def disaggregate(self, qty, new_dim, method="shares", args=[]):
        """Add a computation that disaggregates *qty* using *method*.

        Parameters
        ----------
        qty: hashable
            Key of the quantity to be disaggregated.
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
        :class:`Key`
            The key of the newly-added node.
        """
        # Compute the new key
        key = Key.from_str_or_key(qty, append=new_dim)

        # Get the method
        if isinstance(method, str):
            try:
                method = getattr(computations, "disaggregate_{}".format(method))
            except AttributeError:
                raise ValueError(
                    "No disaggregation method 'disaggregate_{}'".format(method)
                )
        if not callable(method):
            raise ValueError(method)

        return self.add(key, tuple([method, qty] + args), strict=True)

    # Convenience methods
    def add_file(self, path, key=None, **kwargs):
        """Add exogenous quantities from *path*.

        Reporting the `key` or using it in other computations causes `path` to
        be loaded and converted to :class:`.Quantity`.

        Parameters
        ----------
        path : os.PathLike
            Path to the file, e.g. '/path/to/foo.ext'.
        key : str or .Key, optional
            Key for the quantity read from the file.

        Other parameters
        ----------------
        dims : dict or list or set
            Either a collection of names for dimensions of the quantity, or a
            mapping from names appearing in the input to dimensions.
        units : str or pint.Unit
            Units to apply to the loaded Quantity.

        Returns
        -------
        .Key
            Either `key` (if given) or e.g. ``file:foo.ext`` based on the
            `path` name, without directory components.

        See also
        --------
        ixmp.reporting.computations.load_file
        """
        path = Path(path)
        key = key if key else "file:{}".format(path.name)
        return self.add(
            key, (partial(computations.load_file, path, **kwargs),), strict=True
        )

    # Use add_file as a helper for computations.load_file
    add_load_file = add_file

    def describe(self, key=None, quiet=True):
        """Return a string describing the computations that produce *key*.

        If *key* is not provided, all keys in the Reporter are described.

        The string can be printed to the console, if not *quiet*.
        """
        if key is None:
            # Sort with 'all' at the end
            key = tuple(
                sorted(filter(lambda k: k != "all", self.graph.keys())) + ["all"]
            )
        else:
            key = (key,)

        result = describe_recursive(self.graph, key)
        if not quiet:
            print(result, end="\n")
        return result

    def visualize(self, filename, **kwargs):
        """Generate an image describing the reporting structure.

        This is a shorthand for :meth:`dask.visualize`. Requires
        `graphviz <https://pypi.org/project/graphviz/>`__.
        """
        return dask.visualize(self.graph, filename=filename, **kwargs)

    def write(self, key, path):
        """Write the report *key* to the file *path*."""
        # Call the method directly without adding it to the graph
        key = self.check_keys(key)[0]
        computations.write_report(self.get(key), path)

    @property
    def unit_registry(self):
        """The :meth:`pint.UnitRegistry` used by the Reporter."""
        return pint.get_application_registry()


def configure(path=None, **config):
    """Configure reporting globally.

    Modifies global variables that affect the behaviour of *all* Reporters and
    computations, namely :obj:`.RENAME_DIMS` and :obj:`.REPLACE_UNITS`.

    Valid configuration keys—passed as *config* keyword arguments—include:

    Other Parameters
    ----------------
    units : mapping
        Configuration for handling of units. Valid sub-keys include:

        - **replace** (mapping of str -> str): replace units before they are
          parsed by :doc:`pint <pint:index>`. Added to :obj:`.REPLACE_UNITS`.
        - **define** (:class:`str`): block of unit definitions, added to the
          :mod:`pint` application registry so that units are recognized. See
          the pint :ref:`documentation on defining units <pint:defining>`.

    rename_dims : mapping of str -> str
        Update :obj:`.RENAME_DIMS`.

    Warns
    -----
    UserWarning
        If *config* contains unrecognized keys.
    """
    config = _config_args(path, config)

    # Units
    units = config.get("units", {})

    # Define units
    registry = pint.get_application_registry()
    try:
        registry.define(units["define"].strip())
    except KeyError:
        pass
    except pint.DefinitionSyntaxError as e:
        log.warning(e)

    # Add replacements
    for old, new in units.get("replace", {}).items():
        REPLACE_UNITS[old] = new

    # Dimensions to be renamed
    RENAME_DIMS.update(config.get("rename_dims", {}))


def _config_args(path=None, keys={}):
    """Handle configuration arguments."""
    result = {}

    if path:
        # Load configuration from file
        path = Path(path)
        with open(path, "r") as f:
            result.update(yaml.safe_load(f))

        # Also store the directory where the configuration file was located
        result["config_dir"] = path.parent

    # Update with keys
    result.update(keys)

    return result


def keys_for_quantity(ix_type, name, scenario):
    """Return keys for *name* in *scenario*."""
    # Retrieve names of the indices of the ixmp item, without loading the data
    dims = dims_for_qty(scenario.idx_names(name))

    # Column for retrieving data
    column = "value" if ix_type == "par" else "lvl"

    # A computation to retrieve the data
    key = Key(name, dims)
    result = [
        (
            key,
            partial(computations.data_for_quantity, ix_type, name, column),
            "scenario",
            "config",
        )
    ]

    # Add the marginal values at full resolution, but no aggregates
    if ix_type == "equ":
        result.append(
            (
                Key("{}-margin".format(name), dims),
                partial(computations.data_for_quantity, ix_type, name, "mrg"),
                "scenario",
                "config",
            )
        )

    return result
