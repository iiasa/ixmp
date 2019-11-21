from ixmp.testing import make_dantzig
import pytest


@pytest.mark.parametrize('kwargs', [
    dict(comment=None),
    dict(equ_list=None, var_list=['x']),
    dict(equ_list=['demand', 'supply'], var_list=[]),
], ids=['null-comment', 'null-list', 'empty-list'])
def test_GAMSModel(test_mp, test_data_path, kwargs):
    s = make_dantzig(test_mp)
    s.solve(model='dantzig', **kwargs)


def test_model_initialize():
    # TODO Model.initialize() runs on both a 'empty' and already-init'd
    #      Scenario

    # TODO Unrecognized Scenario(scheme=...) raises an intelligible exception

    # TODO Keyword arguments to Scenario(...) that are not recognized by
    #      Model.initialize() raise an intelligible exception

    pass
