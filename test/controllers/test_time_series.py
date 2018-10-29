import unittest
from test.helpers import new_test_service_context
from xcube_server.controllers.time_series import get_time_series_info


class TimeSeriesControllerTest(unittest.TestCase):

    def test_get_time_series_info(self):
        ctx = new_test_service_context()
        info = get_time_series_info(ctx)

        expected_dict = self._get_expected_dict()
        self.assertDictEqual(expected_dict, info)

    def _get_expected_dict(self):
        expected_dict = {'layers': []}
        bounds = {'xmin': -180.0, 'ymin': -90.0, 'xmax': 180.0, 'ymax': 90.0}
        demo_times = ['2017-01-16T10:09:21Z', '2017-01-25T09:35:51Z', '2017-01-26T10:50:16Z',
                      '2017-01-28T09:58:11Z', '2017-01-30T10:46:33Z']
        demoVariables = ['quality_flags', 'kd489', 'conc_tsm', 'conc_chl', 'c2rcc_flags']
        for demoVariable in demoVariables:
            dictVariable = {'name': 'demo.{}'.format(demoVariable), 'dates': demo_times, 'bounds': bounds}
            expected_dict['layers'].append(dictVariable)
        demo1w_times = ['2017-01-22T00:00:00Z', '2017-01-29T00:00:00Z', '2017-02-05T00:00:00Z']
        for demoVariable in demoVariables:
            dictVariable = {'name': 'demo-1w.{}'.format(demoVariable), 'dates': demo1w_times, 'bounds': bounds}
            expected_dict['layers'].append(dictVariable)
        return expected_dict
