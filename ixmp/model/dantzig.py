from collections import ChainMap
from pathlib import Path

import pandas as pd

from .gams import GAMSModel


ITEMS = (
    # Plants
    dict(ix_type='set', name='i'),
    # Markets
    dict(ix_type='set', name='j'),
    # Capacity of plant i in cases
    dict(ix_type='par', name='a', idx_sets=['i']),
    # Demand at market j in cases
    dict(ix_type='par', name='b', idx_sets=['j']),
    # Distance between plant i and market j
    dict(ix_type='par', name='d', idx_sets=['i', 'j']),
    # Transport cost per case per 1000 miles
    dict(ix_type='par', name='f', idx_sets=None),
    # Decision variables and equations
    dict(ix_type='var', name='x', idx_sets=['i', 'j']),
    dict(ix_type='var', name='z', idx_sets=None),
    dict(ix_type='equ', name='cost', idx_sets=None),
    dict(ix_type='equ', name='demand', idx_sets=['j']),
    dict(ix_type='equ', name='supply', idx_sets=['i']),
)


class DantzigModel(GAMSModel):
    """Dantzig's cannery/transport problem as a :class:`GAMSModel`.

    Provided for testing :mod:`ixmp` code.
    """
    name = 'dantzig'

    defaults = ChainMap({
        # Override keys from GAMSModel
        'model_file': Path(__file__).with_name('dantzig.gms'),
    }, GAMSModel.defaults)

    @classmethod
    def initialize(cls, scenario, with_data=False):
        """Initialize the problem.

        If *with_data* is :obj:`True` (default: :obj:`False`), the set and
        parameter values from the original problem are also populated.
        Otherwise, the sets and parameters are left empty.
        """
        # Initialize the ixmp items
        cls.initialize_items(scenario, ITEMS)

        if not with_data:
            return

        # Add set elements
        scenario.add_set('i', ['seattle', 'san-diego'])
        scenario.add_set('j', ['new-york', 'chicago', 'topeka'])

        # Add parameter values
        scenario.add_par('a', 'seattle', 350, 'cases')
        scenario.add_par('a', 'san-diego', 600, 'cases')

        scenario.add_par('b', pd.DataFrame([
            ['new-york', 325, 'cases'],
            ['chicago', 300, 'cases'],
            ['topeka', 275, 'cases'],
        ], columns='j value unit'.split()))

        scenario.add_par('d', pd.DataFrame([
            ['seattle', 'new-york', 2.5, 'km'],
            ['seattle', 'chicago', 1.7, 'km'],
            ['seattle', 'topeka', 1.8, 'km'],
            ['san-diego', 'new-york', 2.5, 'km'],
            ['san-diego', 'chicago', 1.8, 'km'],
            ['san-diego', 'topeka', 1.4, 'km'],
        ], columns='i j value unit'.split()))

        scenario.change_scalar('f', 90.0, 'USD_per_km')
