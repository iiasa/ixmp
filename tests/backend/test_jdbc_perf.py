import ixmp


def _save_par(test_mp_mem, size=1000):
    scen = ixmp.Scenario(test_mp_mem,
                         model='test',
                         scenario='scenario',
                         version='new')
    scen.init_set('ii')
    ii = range(1, size, 2)
    scen.add_set('ii', ii)
    scen.init_par('new_par', idx_sets='ii')
    scen.add_par('new_par', ii, [1.2] * len(ii))
    scen.commit('init')


def test_save_par_1000(benchmark, test_mp_mem):
    benchmark(_save_par, test_mp_mem, 1000)


def test_read_par_100000(benchmark, test_mp_mem):
    def read_par(test_mp_mem):
        scen = ixmp.Scenario(test_mp_mem, model='test', scenario='scenario')
        return scen.par('new_par')

    _save_par(test_mp_mem, 100000)
    benchmark(read_par, test_mp_mem)
