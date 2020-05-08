import logging

from pandas.testing import assert_series_equal
import pint
import pytest

import ixmp
from ixmp.reporting import Reporter, as_quantity, computations
from ixmp.testing import assert_logs

from . import add_test_data


@pytest.fixture(scope='function')
def data(test_mp, request):
    scen = ixmp.Scenario(test_mp, request.node.name, request.node.name, 'new')
    rep = Reporter.from_scenario(scen)
    yield [scen, rep] + list(add_test_data(scen))


def test_apply_units(data, caplog):
    # Unpack
    *_, x = data

    registry = pint.get_application_registry()

    # Brute-force replacement with incompatible units
    with assert_logs(caplog, "Replace 'kilogram' with incompatible 'liter'"):
        result = computations.apply_units(x, 'litres')
    assert result.attrs['_unit'] == registry.Unit('litre')
    # No change in values
    assert_series_equal(result.to_series(), x.to_series())

    # Compatible units: magnitudes are also converted
    with assert_logs(caplog, "Convert 'kilogram' to 'metric_ton'",
                     at_level=logging.DEBUG):
        result = computations.apply_units(x, 'tonne')
    assert result.attrs['_unit'] == registry.Unit('tonne')
    assert_series_equal(result.to_series(), x.to_series() * 0.001)

    # Remove unit
    x.attrs['_unit'] = registry.Unit('dimensionless')

    caplog.clear()
    result = computations.apply_units(x, 'kg')
    # Nothing logged when _unit attr is missing
    assert len(caplog.messages) == 0
    assert result.attrs['_unit'] == registry.Unit('kg')
    assert_series_equal(result.to_series(), x.to_series())


def test_select(data):
    # Unpack
    *_, t_foo, t_bar, x = data

    x = as_quantity(x)
    assert len(x) == 6 * 6

    # Selection with inverse=False
    indexers = {'t': t_foo[0:1] + t_bar[0:1]}
    result_0 = computations.select(x, indexers=indexers)
    assert len(result_0) == 2 * 6

    # Single indexer along one dimension results in 1D data
    indexers['y'] = '2010'
    result_1 = computations.select(x, indexers=indexers)
    assert len(result_1) == 2 * 1

    # Selection with inverse=True
    result_2 = computations.select(x, indexers=indexers, inverse=True)
    assert len(result_2) == 4 * 5
