import logging

import pytest

from ixmp import Scenario
from ixmp.testing import make_dantzig
from ixmp.model.dantzig import DantzigModel


@pytest.mark.parametrize('kwargs', [
    dict(comment=None),
    dict(equ_list=None, var_list=['x']),
    dict(equ_list=['demand', 'supply'], var_list=[]),
], ids=['null-comment', 'null-list', 'empty-list'])
def test_GAMSModel(test_mp, test_data_path, kwargs):
    s = make_dantzig(test_mp)
    s.solve(model='dantzig', **kwargs)


def test_model_initialize(test_mp, caplog):
    # Model.initialize runs on an empty Scenario
    s = make_dantzig(test_mp)
    b = s.par('b')
    assert len(b) == 3

    # TODO modify a value for 'b' and ensure it is not overwritten when
    #      initialize is called again.

    # Model.initialize runs on an already initialized Scenario
    DantzigModel.initialize(s, with_data=True)
    assert len(s.par('b')) == 3

    # Unrecognized Scenario(scheme=...) is initialized using the base method, a
    # no-op
    caplog.set_level(logging.DEBUG)
    Scenario(test_mp, model='model name', scenario='scenario name',
             version='new', scheme='unknown')
    assert caplog.records[-1].message == \
        "No initialization for 'unknown'-scheme Scenario"

    # TODO Keyword arguments to Scenario(...) that are not recognized by
    #      Model.initialize() raise an intelligible exception

    pass
