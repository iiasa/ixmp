import logging
from functools import partial
from itertools import chain, repeat

import dask

from genno.core.computer import Computer
from genno.core.key import Key

from . import computations as ixmp_computations
from .util import RENAME_DIMS, dims_for_qty

log = logging.getLogger(__name__)


class Reporter(Computer):
    """Class for describing and executing computations."""

    modules = list(Computer.modules) + [ixmp_computations]

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
        :class:`.Reporter`
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
            except AttributeError:  # pragma: no cover
                # pd.DataFrame for a multidimensional set; store as-is
                # TODO write tests for this downstream (in ixmp)
                pass

            rep.add(RENAME_DIMS.get(name, name), elements)

        return rep

    def configure(self, path=None, **config):
        """Configure the Reporter.

        Calls :meth:`set_filters` for a "filters" keyword argument.
        """
        super().configure(path, **config)

        # Handle filters configuration
        self.set_filters(**self.graph["config"].get("filters", {}))

        return self

    def finalize(self, scenario):
        """Prepare the Reporter to act on *scenario*.

        The :class:`Scenario <message_ix.Scenario>` object *scenario* is
        associated with the key ``'scenario'``. All subsequent processing will
        act on data from this *scenario*.
        """
        self.graph["scenario"] = scenario

    def set_filters(self, **filters):
        """Apply *filters* ex ante (before computations occur).

        Filters are stored in the reporter at the ``'filters'`` key, and are passed to
        :meth:`ixmp.Scenario.par` and similar methods. All quantity values read from
        the Scenario are filtered *before* any other computations take place.

        If no arguments are provided, *all* filters are cleared.

        Parameters
        ----------
        filters : mapping of str → (list of str or None)
            Argument names are dimension names; values are lists of allowable labels
            along the respective dimension, *or* None to clear any existing filters for
            the dimension.
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
            partial(ixmp_computations.data_for_quantity, ix_type, name, column),
            "scenario",
            "config",
        )
    ]

    # Add the marginal values at full resolution, but no aggregates
    if ix_type == "equ":
        result.append(
            (
                Key("{}-margin".format(name), dims),
                partial(ixmp_computations.data_for_quantity, ix_type, name, "mrg"),
                "scenario",
                "config",
            )
        )

    return result
