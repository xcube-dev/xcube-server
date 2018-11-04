import unittest

import numpy as np
import shapely.geometry
import xarray as xr

from xcube_server.im import TileGrid
from xcube_server.utils import compute_tile_grid, get_dataset_geometry, get_dataset_bounds, get_geometry_mask


class GetDatasetGeometryTest(unittest.TestCase):

    def test_nominal(self):
        ds1, ds2 = _get_nominal_datasets()
        bounds = get_dataset_geometry(ds1)
        self.assertEqual(shapely.geometry.box(-25.0, -15.0, 15.0, 15.0), bounds)
        bounds = get_dataset_geometry(ds2)
        self.assertEqual(shapely.geometry.box(-25.0, -15.0, 15.0, 15.0), bounds)

    def test_antimeridian(self):
        ds1, ds2 = _get_antimeridian_datasets()
        bounds = get_dataset_geometry(ds1)
        self.assertEqual(shapely.geometry.MultiPolygon(
            polygons=[
                shapely.geometry.box(165.0, -15.0, 180.0, 15.0),
                shapely.geometry.box(-180.0, -15.0, -155.0, 15.0)
            ]),
            bounds)
        bounds = get_dataset_geometry(ds2)
        self.assertEqual(shapely.geometry.MultiPolygon(
            polygons=[
                shapely.geometry.box(165.0, -15.0, 180.0, 15.0),
                shapely.geometry.box(-180.0, -15.0, -155.0, 15.0)
            ]),
            bounds)


class GetDatasetBoundsTest(unittest.TestCase):
    def test_nominal(self):
        ds1, ds2 = _get_nominal_datasets()
        bounds = get_dataset_bounds(ds1)
        self.assertEqual((-25.0, -15.0, 15.0, 15.0), bounds)
        bounds = get_dataset_bounds(ds2)
        self.assertEqual((-25.0, -15.0, 15.0, 15.0), bounds)

    def test_anti_meridian(self):
        ds1, ds2 = _get_antimeridian_datasets()
        bounds = get_dataset_bounds(ds1)
        self.assertEqual((165.0, -15.0, -155.0, 15.0), bounds)
        bounds = get_dataset_bounds(ds2)
        self.assertEqual((165.0, -15.0, -155.0, 15.0), bounds)


class ComputeTileGridTest(unittest.TestCase):
    def test_compute_tile_grid_pos_y(self):
        res = 5. / 2000.
        res05 = res / 2
        ts = compute_tile_grid(xr.DataArray(np.zeros(shape=(3, 1000, 2000)),
                                            dims=dict(time=3, lat=1000, lon=2000),
                                            coords=dict(time=np.linspace(0, 3, 3),
                                                        lat=np.linspace(50. + res05, 52.5 - res05, 1000),
                                                        lon=np.linspace(0. + res05, 5. - res05, 2000))))
        self.assertIsInstance(ts, TileGrid)
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

    def test_compute_tile_grid_neg_y(self):
        res = 5. / 2000.
        res05 = res / 2
        ts = compute_tile_grid(xr.DataArray(np.zeros(shape=(3, 1000, 2000)),
                                            dims=dict(time=3, lat=1000, lon=2000),
                                            coords=dict(time=np.linspace(0, 3, 3),
                                                        lat=np.linspace(52.5 - res05, 50. + res05, 1000),
                                                        lon=np.linspace(0. + res05, 5. - res05, 2000))))
        self.assertIsInstance(ts, TileGrid)
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

    def test_compute_tile_grid_antimeridian(self):
        res = 5. / 2000.
        res05 = res / 2
        lon1 = np.linspace(180 - 2.5 + res05, 180 - res05, 1000)
        lon2 = np.linspace(-180 + res05, -180 + 2.5 - res05, 1000)
        lon = np.concatenate((lon1, lon2))
        ts = compute_tile_grid(xr.DataArray(np.zeros(shape=(3, 1000, 2000)),
                                            dims=dict(time=3, lat=1000, lon=2000),
                                            coords=dict(time=np.linspace(0, 3, 3),
                                                        lat=np.linspace(52.5 - res05, 50. + res05, 1000),
                                                        lon=lon)))
        self.assertIsInstance(ts, TileGrid)
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


def _get_nominal_datasets():
    data_vars = dict(a=(("time", "lat", "lon"), np.random.rand(5, 3, 4)))

    coords = dict(time=(("time",), np.array(range(0, 5))),
                  lat=(("lat",), np.array([-10, 0., 10])),
                  lon=(("lon",), np.array([-20, -10, 0., 10])))
    ds1 = xr.Dataset(coords=coords, data_vars=data_vars)

    coords.update(dict(lat_bnds=(("lat", "bnds"), np.array([[-15, -5], [-5., 5], [5, 15]])),
                       lon_bnds=(
                           ("lon", "bnds"), np.array([[-25., -15.], [-15., -5.], [-5., 5.], [5., 15.]]))
                       ))
    ds2 = xr.Dataset(coords=coords, data_vars=data_vars)

    return ds1, ds2


def _get_antimeridian_datasets():
    ds1, ds2 = _get_nominal_datasets()
    ds1 = ds1.assign_coords(lon=(("lon",), np.array([170., 180., -170., -160.])))
    ds2 = ds2.assign_coords(
        lon_bnds=(("lon", 2), np.array([[165., 175], [175., -175.], [-175., -165], [-165., -155.]])))
    return ds1, ds2


class GetGeometryMaskTest(unittest.TestCase):
    def test_get_geometry_mask(self):
        w = 16
        h = 8
        res = 1.0
        lon_min = 0
        lat_min = 0
        lon_max = lon_min + w * res
        lat_max = lat_min + h * res

        triangle = shapely.geometry.Polygon(((lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max),
                                             (lon_min, lat_min)))

        actual_mask = get_geometry_mask(w, h, triangle, lon_min, lat_min, res)
        expected_mask = np.array([[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1],
                                  [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                                  [0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                                  [0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                                  [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]], dtype=np.byte)
        np.testing.assert_array_almost_equal(expected_mask, actual_mask.astype('byte'))

        smaller_triangle = triangle.buffer(-1.5 * res)
        actual_mask = get_geometry_mask(w, h, smaller_triangle, lon_min, lat_min, res)
        expected_mask = np.array([[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 0],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0],
                                  [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0],
                                  [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0],
                                  [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]], dtype=np.byte)
        np.testing.assert_array_almost_equal(expected_mask, actual_mask)
