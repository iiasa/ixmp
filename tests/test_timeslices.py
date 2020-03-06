# coding=utf-8


def test_timeslices(test_mp):
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == 'COMMON']
    # result has all attributes of time slice
    assert all(obs.columns == ['name', 'category', 'duration'])
    # result contains pre-defined YEAR time slice
    assert all([list(obs.iloc[0]) == ['YEAR', 'COMMON', 1.0]])


def test_add_timeslice(test_mp):
    test_mp.add_timeslice('LEAP_YEAR_FEBRUARY', 'LEAP_YEAR_MONTHS',
                          1.0 / 366 * 28)
    timeslices = test_mp.timeslices()
    obs = timeslices[timeslices.category == 'LEAP_YEAR_MONTHS']
    # return only added time slice
    assert len(obs) == 1
    # returned time slice attributes have expected values
    assert all([list(obs.iloc[0]) == ['LEAP_YEAR_FEBRUARY', 'LEAP_YEAR_MONTHS',
                                      1.0 / 366 * 28]])
