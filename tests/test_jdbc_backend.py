import ixmp


def save_par(mp, size=1000):
    scen = ixmp.Scenario(mp, model='test', scenario='scenario', version='new')
    scen.init_set('ii')
    ii = range(1, size, 2)
    scen.add_set('ii', ii)
    scen.init_par('new_par', idx_sets='ii')
    scen.add_par('new_par', ii, [1.2] * len(ii))
    scen.commit('init')
    return scen


def read_par(scen):
    return scen.par('new_par')


def init_platform():
    return ixmp.Platform(backend='jdbc', driver='org.hsqldb.jdbcDriver',
                         url='jdbc:hsqldb:mem:ixmptest',
                         user='ixmp', password='ixmp')


def test_save_par_1000(benchmark):
    mp = init_platform()
    benchmark(save_par, mp, 1000)


def test_read_par_100000(benchmark):
    mp = init_platform()
    scen = save_par(mp, 100000)
    benchmark(read_par, scen)
