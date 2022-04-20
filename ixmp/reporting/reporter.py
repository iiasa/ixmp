from itertools import chain, repeat
from typing import List, Union, cast

import dask
import pandas as pd
from genno.core.computer import Computer, Key

from ixmp.core.scenario import Scenario
from ixmp.reporting import computations
from ixmp.reporting.util import RENAME_DIMS, keys_for_quantity


class Reporter(Computer):
    """Class for describing and executing computations."""

    # Append ixmp.reporting.computations to the modules in which the Computer will look
    # up computations names.
    modules = list(Computer.modules) + [computations]

    @classmethod
    def from_scenario(cls, scenario: Scenario, **kwargs) -> "Reporter":
        """Create a Reporter by introspecting `scenario`.

        Parameters
        ----------
        scenario : .Scenario
            Scenario to introspect in creating the Reporter.
        kwargs : optional
            Passed to :meth:`Scenario.configure`.

        Returns
        -------
        .Reporter
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
        all_keys: List[Union[str, Key]] = []

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
            rep.add(*comps[0], strict=True, sums=True)

            try:
                # Add any marginals, but without sums
                rep.add(*comps[1], strict=True)
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
                # Convert Series to list; protect list so that dask schedulers do not
                # try to interpret its contents as further tasks
                elements = dask.core.quote(cast(pd.Series, elements).tolist())
            except AttributeError:  # pragma: no cover
                # pd.DataFrame for a multidimensional set; store as-is
                # TODO write tests for this
                pass

            rep.add(RENAME_DIMS.get(name, name), elements)

        return rep

    def finalize(self, scenario: Scenario) -> None:
        """Prepare the Reporter to act on `scenario`.

        The :class:`.TimeSeries` (i.e. including :class:`ixmp.Scenario` and
        :class:`message_ix.Scenario`) object `scenario` is stored with the key
        ``'scenario'``. All subsequent processing will act on data from this Scenario.
        """
        self.graph["scenario"] = scenario

    def set_filters(self, **filters) -> None:
        """Apply `filters` ex ante (before computations occur).

        See the description of :func:`.filters` under :ref:`reporting-config`.
        """
        self.configure(filters=filters)
