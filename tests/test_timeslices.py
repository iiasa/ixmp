# coding=utf-8

import pytest

def test_timeslices(test_mp):
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == 'Common']
    # result has all attributes of time slice
    assert all(obs.columns == ['name', 'category', 'duration'])
    # result contains pre-defined YEAR time slice
    assert all([list(obs.iloc[0]) == ['Year', 'Common', 1.0]])


def test_add_timeslice(test_mp):
    test_mp.add_timeslice('January, 1st', 'Days',
                          1.0 / 366)
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == 'Days']
    # return only added time slice
    assert len(obs) == 1
    # returned time slice attributes have expected values
    assert all([list(obs.iloc[0]) == ['January, 1st', 'Days',
                                      1.0 / 366]])


def test_add_timeslice_duplicate_raise(test_mp):
    test_mp.add_timeslice('foo_slice', 'foo_category', 0.2)
    # adding same name with different duration raises an error
    pytest.raises(ValueError, test_mp, add_timeslice,
                  'foo_slice', 'bar_category', 0.3)