import numpy as np
import unittest
from test.helpers import new_test_service_context, RequestParamsMock
from typing import Dict
from xcube_server.controllers.time_series import get_time_series_info, get_time_series_for_point
from xcube_server.errors import ServiceBadRequestError


class TimeSeriesControllerTest(unittest.TestCase):

    def test_get_time_series_info(self):
        ctx = new_test_service_context()
        info = get_time_series_info(ctx)

        expected_dict = self._get_expected_info_dict()
        self.assertDictEqual(expected_dict, info)

    def test_get_time_series_for_point_no_lat_or_lon(self):
        ctx = new_test_service_context()
        with self.assertRaises(ServiceBadRequestError) as error:
            get_time_series_for_point(ctx, 'demo', 'conc_tsm', RequestParamsMock())
        self.assertEqual(400, error.exception.status_code)
        self.assertEqual("lat and lon must be given as query parameters",
                         error.exception.reason)

    def test_get_time_series_for_point(self):
        ctx = new_test_service_context()
        time_series_for_point = get_time_series_for_point(ctx, 'demo', 'conc_tsm',
                                                          RequestParamsMock(lat=51.4, lon='2.1',
                                                                            startDate='2017-01-15',
                                                                            endDate='2017-01-29'))
        expected_dict = self._get_expected_time_series_point_dict()
        self.assertDictEqual(expected_dict, time_series_for_point)

    @staticmethod
    def _get_expected_info_dict():
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

    @staticmethod
    def _get_expected_time_series_point_dict():
        expected_dict = {'results': []}

        def _get_result(valid_count: int, average: float, date: str) -> Dict:
            stats_dict = {'totalCount': 1, 'validCount': valid_count, 'average': average}
            result_dict = {}
            result_dict['result'] = stats_dict
            result_dict['date'] = date
            return result_dict

        expected_dict['results'].append(_get_result(1, 3.534773588180542, '2017-01-16'))
        expected_dict['results'].append(_get_result(0, np.NAN, '2017-01-25'))
        expected_dict['results'].append(_get_result(0, np.NAN, '2017-01-26'))
        expected_dict['results'].append(_get_result(1, 20.12085723876953, '2017-01-28'))
        return expected_dict
