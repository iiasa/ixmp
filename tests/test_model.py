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
