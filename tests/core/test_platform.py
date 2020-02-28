import pytest

import ixmp


class TestPlatform:
    def test_init(self):
        with pytest.raises(ValueError, match="backend class 'foo' not among "
                                             r"\['jdbc'\]"):
            ixmp.Platform(backend='foo')

    def test_set_log_level(self, test_mp):
        test_mp.set_log_level('CRITICAL')
        test_mp.set_log_level('ERROR')
        test_mp.set_log_level('WARNING')
        test_mp.set_log_level('INFO')
        test_mp.set_log_level('DEBUG')
        test_mp.set_log_level('NOTSET')

        with pytest.raises(ValueError):
            test_mp.set_log_level(level='foo')

    def test_scenario_list(self, mp):
        scenario = mp.scenario_list(model='Douglas Adams')['scenario']
        assert scenario[0] == 'Hitchhiker'

    def test_export_timeseries_data(self, mp, tmp_path):
        path = tmp_path / 'export.csv'
        mp.export_timeseries_data(path, model='Douglas Adams')

        with open(path) as f:
            first_line = f.readline()
            assert first_line == ('MODEL,SCENARIO,VERSION,VARIABLE,UNIT,'
                                  'REGION,META,TIME,YEAR,VALUE\n')
            assert len(f.readlines()) == 2

    def test_unit_list(self, test_mp):
        units = test_mp.units()
        assert ('cases' in units) is True

    def test_add_unit(self, test_mp):
        test_mp.add_unit('test', 'just testing')
