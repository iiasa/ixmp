# coding=utf-8


def test_timesteps(test_mp):
    timesteps = test_mp.timesteps()
    obs = timesteps[timesteps.category == 'YEARS']
    assert all(obs.columns == ['name', 'category', 'duration'])
    assert all([list(obs.iloc[0]) == ['YEAR', 'YEARS', 1.0]])


def test_add_timestep(test_mp):
    test_mp.set_timestep('LEAP_YEAR_FEBRUARY', 'LEAP_YEAR_MONTHS',
                         1.0 / 366 * 28)
    timesteps = test_mp.timesteps()
    obs = timesteps[timesteps.category == 'LEAP_YEAR_MONTHS']
    assert len(obs) == 1
    assert all([list(obs.iloc[0]) == ['LEAP_YEAR_FEBRUARY', 'LEAP_YEAR_MONTHS',
                                      1.0 / 366 * 28]])
