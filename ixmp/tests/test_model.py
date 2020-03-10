import logging

import pytest

from ixmp import Scenario
from ixmp.testing import make_dantzig
from ixmp.model.base import Model
from ixmp.model.dantzig import DantzigModel


def test_base_model():
    # An incomplete Backend subclass can't be instantiated
    class M1(Model):
        pass

    with pytest.raises(TypeError, match="Can't instantiate abstract class M1 "
                                        "with abstract methods"):
        M1()


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
    b1 = s.par('b')
    assert len(b1) == 3

    # Modify a value for 'b'
    s.check_out()
    s.add_par('b', 'chicago', 600, 'cases')
    s.commit('Overwrite b(chicago)')

    # Model.initialize runs on an already-initialized Scenario, without error
    DantzigModel.initialize(s, with_data=True)

    # Data has the same length...
    b2 = s.par('b')
    assert len(b2) == 3
    # ...but modified value(s) are not overwritten
    assert (b2.query("j == 'chicago'")['value'] == 600).all()

    # Unrecognized Scenario(scheme=...) is initialized using the base method, a
    # no-op
    caplog.set_level(logging.DEBUG)
    Scenario(test_mp, model='model name', scenario='scenario name',
             version='new', scheme='unknown')
    assert caplog.records[-1].message == \
        "No initialization for 'unknown'-scheme Scenario"

    # Keyword arguments to Scenario(...) that are not recognized by
    # Model.initialize() raise an intelligible exception
    with pytest.raises(TypeError,
                       match="unexpected keyword argument 'bad_arg1'"):
        Scenario(test_mp, model='model name', scenario='scenario name',
                 version='new', scheme='unknown', bad_arg1=111)

    with pytest.raises(TypeError,
                       match="unexpected keyword argument 'bad_arg2'"):
        Scenario(test_mp, model='model name', scenario='scenario name',
                 version='new', scheme='dantzig', with_data=True,
                 bad_arg2=222)
