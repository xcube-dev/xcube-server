import unittest

import xarray as xr

from test.helpers import new_test_service_context
from xcube_server.errors import ServiceResourceNotFoundError


class ServiceContextTest(unittest.TestCase):
    def test_config_and_dataset_cache(self):
        ctx = new_test_service_context()
        self.assertNotIn('demo', ctx.dataset_cache)

        ctx.get_dataset('demo')
        self.assertIn('demo', ctx.dataset_cache)

        ctx.config = dict(Datasets=[
            dict(Identifier='demo',
                 Path="../../../xcube_server/res/demo/cube.nc"),
            dict(Identifier='demo2',
                 Path="../../../xcube_server/res/demo/cube.nc"),
        ])
        self.assertIn('demo', ctx.dataset_cache)
        self.assertNotIn('demo2', ctx.dataset_cache)

        ctx.get_dataset('demo2')
        self.assertIn('demo', ctx.dataset_cache)
        self.assertIn('demo2', ctx.dataset_cache)

        ctx.config = dict(Datasets=[
            dict(Identifier='demo2',
                 Path="../../../xcube_server/res/demo/cube.nc"),
        ])
        self.assertNotIn('demo', ctx.dataset_cache)
        self.assertIn('demo2', ctx.dataset_cache)

        ctx.config = dict()
        self.assertNotIn('demo', ctx.dataset_cache)
        self.assertNotIn('demo2', ctx.dataset_cache)

    def test_get_dataset_and_variable(self):
        ctx = new_test_service_context()
        ds, var = ctx.get_dataset_and_variable('demo', 'conc_tsm')
        self.assertIsInstance(ds, xr.Dataset)
        self.assertIsInstance(var, xr.DataArray)

        with self.assertRaises(ServiceResourceNotFoundError) as cm:
            ctx.get_dataset_and_variable('demox', 'conc_ys')
        self.assertEqual(404, cm.exception.status_code)
        self.assertEqual('Dataset "demox" not found', cm.exception.reason)

        with self.assertRaises(ServiceResourceNotFoundError) as cm:
            ctx.get_dataset_and_variable('demo', 'conc_ys')
        self.assertEqual(404, cm.exception.status_code)
        self.assertEqual('Variable "conc_ys" not found in dataset "demo"', cm.exception.reason)

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

    def test_get_feature_collection(self):
        ctx = new_test_service_context()
        feature_collection = ctx.get_feature_collection()
        self.assertIsInstance(feature_collection, dict)
        self.assertIn("type", feature_collection)
        self.assertEqual("FeatureCollection", feature_collection["type"])
        self.assertIn("features", feature_collection)
        self.assertIsInstance(feature_collection["features"], list)
        self.assertEqual(6, len(feature_collection["features"]))
        self.assertIs(feature_collection, ctx.get_feature_collection())

    def test_get_feature_collection_by_name(self):
        ctx = new_test_service_context()
        feature_collection = ctx.get_feature_collection(collection_name="inside-cube")
        self.assertIsInstance(feature_collection, dict)
        self.assertIn("type", feature_collection)
        self.assertEqual("FeatureCollection", feature_collection["type"])
        self.assertIn("features", feature_collection)
        self.assertIsInstance(feature_collection["features"], list)
        self.assertEqual(3, len(feature_collection["features"]))
        self.assertIs(feature_collection, ctx.get_feature_collection(collection_name="inside-cube"))
        self.assertIsNot(feature_collection, ctx.get_feature_collection(collection_name="outside-cube"))

        with self.assertRaises(ServiceResourceNotFoundError) as cm:
            ctx.get_feature_collection(collection_name="bibo")
        self.assertEqual('HTTP 404: Unknown feature collection "bibo"', f"{cm.exception}")
