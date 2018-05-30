import os
import unittest
from typing import Dict, Optional

import xarray as xr

from test.helpers import get_res_test_dir, new_test_service_context
from xcts.context import ServiceContext
from xcts.errors import ServiceBadRequestError, ServiceResourceNotFoundError
from xcts.service import RequestParams


class RequestParamsMock(RequestParams):
    def __init__(self, **kvp):
        self.kvp = kvp

    def get_query_argument(self, name: str, default: Optional[str]) -> Optional[str]:
        return self.kvp.get(name, default)


class ServiceContextTest(unittest.TestCase):

    # TODO: test error cases!

    def test_get_capabilities(self):
        self.maxDiff = None
        with open(os.path.join(get_res_test_dir(), 'WMTSCapabilities.xml')) as fp:
            expected_capabilities = fp.read()
        ctx = new_test_service_context()
        capabilities = ctx.get_wmts_capabilities('text/xml', 'http://bibo')
        self.assertEqual(expected_capabilities, capabilities)

    def test_get_dataset_tile(self):
        ctx = new_test_service_context()
        tile = ctx.get_dataset_tile('demo', 'conc_tsm', '0', '0', '0', RequestParamsMock())
        self.assertIsInstance(tile, bytes)

    def test_get_dataset_tile_with_all_params(self):
        ctx = new_test_service_context()
        tile = ctx.get_dataset_tile('demo', 'conc_tsm', '0', '0', '0', RequestParamsMock(time='current', cbar='plasma',
                                                                                         vmin='0.1', vmax='0.3'))
        self.assertIsInstance(tile, bytes)

    def test_get_dataset_tile_with_time_dim(self):
        ctx = new_test_service_context()
        tile = ctx.get_dataset_tile('demo', 'conc_tsm', '0', '0', '0', RequestParamsMock(time='2017-01-26'))
        self.assertIsInstance(tile, bytes)

        with self.assertRaises(ServiceBadRequestError) as cm:
            ctx.get_dataset_tile('demo', 'conc_tsm', '0', '0', '0', RequestParamsMock(time='Gnaaark!'))
        self.assertEqual(400, cm.exception.status_code)
        self.assertEqual("'Gnaaark!' is not a valid value for "
                         "dimension 'time' of variable 'conc_tsm' of dataset 'demo'",
                         cm.exception.reason)

    def test_get_ne2_tile(self):
        ctx = new_test_service_context()
        tile = ctx.get_ne2_tile('0', '0', '0', RequestParamsMock())
        self.assertIsInstance(tile, bytes)

    def test_get_dataset_and_variable(self):
        ctx = new_test_service_context()
        ds, var = ctx.get_dataset_and_variable('demo', 'conc_tsm')
        self.assertIsInstance(ds, xr.Dataset)
        self.assertIsInstance(var, xr.DataArray)

        with self.assertRaises(ServiceResourceNotFoundError) as cm:
            ctx.get_dataset_and_variable('demox', 'conc_ys')
        self.assertEqual(404, cm.exception.status_code)
        self.assertEqual("Dataset 'demox' not found", cm.exception.reason)

        with self.assertRaises(ServiceResourceNotFoundError) as cm:
            ctx.get_dataset_and_variable('demo', 'conc_ys')
        self.assertEqual(404, cm.exception.status_code)
        self.assertEqual("Variable 'conc_ys' not found in dataset 'demo'", cm.exception.reason)

    def test_get_color_mapping(self):
        ctx = new_test_service_context()
        cm = ctx.get_color_mapping('demo', 'conc_chl')
        self.assertEqual(('plasma', 0., 24.), cm)
        cm = ctx.get_color_mapping('demo', 'conc_tsm')
        self.assertEqual(('PuBuGn', 0., 100.), cm)
        cm = ctx.get_color_mapping('demo', 'kd489')
        self.assertEqual(('jet', 0., 6.), cm)
        cm = ctx.get_color_mapping('demo', '_')
        self.assertEqual(('jet', 0., 1.), cm)

    def test_get_dataset_tile_grid(self):
        self.maxDiff = None

        ctx = new_test_service_context()
        tile_grid = ctx.get_dataset_tile_grid('demo', 'conc_chl', 'ol4.json', 'http://bibo')
        self.assertEqual({
            'url': 'http://bibo/xcts/tile/demo/conc_chl/{z}/{x}/{y}.png',
            'projection': 'EPSG:4326',
            'minZoom': 0,
            'maxZoom': 2,
            'tileGrid': {'extent': [2.168404344971009e-19, 50.0, 5.0, 52.5],
                         'origin': [2.168404344971009e-19, 52.5],
                         'resolutions': [0.01, 0.005, 0.0025],
                         'tileSize': [250, 250]},
        }, tile_grid)

        tile_grid = ctx.get_dataset_tile_grid('demo', 'conc_chl', 'cesium.json', 'http://bibo')
        self.assertEqual({
            'url': 'http://bibo/xcts/tile/demo/conc_chl/{z}/{x}/{y}.png',
            'rectangle': dict(west=2.168404344971009e-19, south=50.0, east=5.0, north=52.5),
            'minimumLevel': 0,
            'maximumLevel': 2,
            'tileWidth': 250,
            'tileHeight': 250,
            'tilingScheme': {'rectangle': dict(west=2.168404344971009e-19, south=50.0, east=5.0, north=52.5),
                             'numberOfLevelZeroTilesX': 2,
                             'numberOfLevelZeroTilesY': 1},
        }, tile_grid)

        with self.assertRaises(ServiceBadRequestError) as cm:
            ctx.get_dataset_tile_grid('demo', 'conc_chl', 'ol2.json', 'http://bibo')
        self.assertEqual(400, cm.exception.status_code)
        self.assertEqual("Unknown tile schema format 'ol2.json'", cm.exception.reason)

    def test_get_ne2_tile_grid(self):
        ctx = ServiceContext()
        tile_grid = ctx.get_ne2_tile_grid('ol4.json', 'http://bibo')
        self.assertEqual({
            'url': 'http://bibo/xcts/tile/ne2/{z}/{x}/{y}.jpg',
            'projection': 'EPSG:4326',
            'minZoom': 0,
            'maxZoom': 2,
            'tileGrid': {'extent': [-180.0, -90.0, 180.0, 90.0],
                         'origin': [-180.0, 90.0],
                         'resolutions': [0.703125, 0.3515625, 0.17578125],
                         'tileSize': [256, 256]},
        }, tile_grid)

        with self.assertRaises(ServiceBadRequestError) as cm:
            ctx.get_ne2_tile_grid('cesium', 'http://bibo')
        self.assertEqual(400, cm.exception.status_code)
        self.assertEqual("Unknown tile schema format 'cesium'", cm.exception.reason)

    def test_get_colorbars(self):
        ctx = ServiceContext()

        response = ctx.get_color_bars('text/json')
        self.assertIsInstance(response, str)
        self.assertTrue(len(response) > 40)
        self.assertEqual('[\n  [\n    "Perceptually Uniform Sequenti', response[0:40])

        response = ctx.get_color_bars('text/html')
        self.assertIsInstance(response, str)
        self.assertTrue(len(response) > 40)
        self.assertEqual('<!DOCTYPE html>\n<html lang="en">\n<head><', response[0:40])

        with self.assertRaises(ServiceBadRequestError) as cm:
            ctx.get_color_bars('text/xml')
        self.assertEqual(400, cm.exception.status_code)
        self.assertEqual("Format 'text/xml' not supported for color bars", cm.exception.reason)
