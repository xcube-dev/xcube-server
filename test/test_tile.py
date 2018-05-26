import unittest
import xarray as xr
import numpy as np

from xcts.im import TilingScheme
from xcts.tile import get_tiling_scheme


class TilingSchemeTest(unittest.TestCase):
    def test_get_tiling_scheme_pos_y(self):
        res = 5. / 2000.
        res05 = res / 2
        ts = get_tiling_scheme(xr.DataArray(np.zeros(shape=(3, 1000, 2000)),
                                            dims=dict(time=3, lat=1000, lon=2000),
                                            coords=dict(time=np.linspace(0, 3, 3),
                                                        lat=np.linspace(50. + res05, 52.5 - res05, 1000),
                                                        lon=np.linspace(0. + res05, 5. - res05, 2000))))
        self.assertIsInstance(ts, TilingScheme)
        self.assertEqual(3, ts.num_levels)
        self.assertEqual(2, ts.num_level_zero_tiles_x)
        self.assertEqual(1, ts.num_level_zero_tiles_y)
        self.assertEqual((250, 250), ts.tile_size)
        self.assertAlmostEqual(0., ts.geo_extent.west)
        self.assertAlmostEqual(5., ts.geo_extent.east)
        self.assertAlmostEqual(50., ts.geo_extent.south)
        self.assertAlmostEqual(52.5, ts.geo_extent.north)
        self.assertEqual(True, ts.geo_extent.inv_y)
        self.assertEqual(False, ts.geo_extent.crosses_antimeridian)

    def test_get_tiling_scheme_neg_y(self):
        res = 5. / 2000.
        res05 = res / 2
        ts = get_tiling_scheme(xr.DataArray(np.zeros(shape=(3, 1000, 2000)),
                                            dims=dict(time=3, lat=1000, lon=2000),
                                            coords=dict(time=np.linspace(0, 3, 3),
                                                        lat=np.linspace(52.5 - res05, 50. + res05, 1000),
                                                        lon=np.linspace(0. + res05, 5. - res05, 2000))))
        self.assertIsInstance(ts, TilingScheme)
        self.assertEqual(3, ts.num_levels)
        self.assertEqual(2, ts.num_level_zero_tiles_x)
        self.assertEqual(1, ts.num_level_zero_tiles_y)
        self.assertEqual((250, 250), ts.tile_size)
        self.assertAlmostEqual(0., ts.geo_extent.west)
        self.assertAlmostEqual(5., ts.geo_extent.east)
        self.assertAlmostEqual(50., ts.geo_extent.south)
        self.assertAlmostEqual(52.5, ts.geo_extent.north)
        self.assertEqual(False, ts.geo_extent.inv_y)
        self.assertEqual(False, ts.geo_extent.crosses_antimeridian)

    def test_get_tiling_scheme_antimeridian(self):
        res = 5. / 2000.
        res05 = res / 2
        lon1 = np.linspace(180 - 2.5 + res05, 180 - res05, 1000)
        lon2 = np.linspace(-180 + res05, -180 + 2.5 - res05, 1000)
        lon = np.concatenate((lon1, lon2))
        ts = get_tiling_scheme(xr.DataArray(np.zeros(shape=(3, 1000, 2000)),
                                            dims=dict(time=3, lat=1000, lon=2000),
                                            coords=dict(time=np.linspace(0, 3, 3),
                                                        lat=np.linspace(52.5 - res05, 50. + res05, 1000),
                                                        lon=lon)))
        self.assertIsInstance(ts, TilingScheme)
        self.assertEqual(3, ts.num_levels)
        self.assertEqual(2, ts.num_level_zero_tiles_x)
        self.assertEqual(1, ts.num_level_zero_tiles_y)
        self.assertEqual((250, 250), ts.tile_size)
        self.assertAlmostEqual(177.5, ts.geo_extent.west)
        self.assertAlmostEqual(-177.5, ts.geo_extent.east)
        self.assertAlmostEqual(50., ts.geo_extent.south)
        self.assertAlmostEqual(52.5, ts.geo_extent.north)
        self.assertEqual(False, ts.geo_extent.inv_y)
        self.assertEqual(True, ts.geo_extent.crosses_antimeridian)
