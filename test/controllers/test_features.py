import unittest

from test.helpers import new_test_service_context
from xcube_server.controllers.features import find_features, find_dataset_features
from xcube_server.errors import ServiceBadRequestError


class FeaturesControllerTest(unittest.TestCase):

    def test_find_features_all(self):
        ctx = new_test_service_context()
        feature_collection = find_features(ctx)
        self._assertFeatureCollection(feature_collection, 6, {'1', '2', '3', '4', '5', '6'})

    def test_find_features_by_box(self):
        ctx = new_test_service_context()
        feature_collection = find_features(ctx, box_coords="-1,49,2,55")
        self._assertFeatureCollection(feature_collection, 2, {'1', '4'})

        with self.assertRaises(ServiceBadRequestError) as cm:
            find_features(ctx, box_coords="-1,49,55")
        self.assertEqual("HTTP 400: Received invalid bounding box geometry.", f"{cm.exception}")

        with self.assertRaises(ServiceBadRequestError) as cm:
            find_features(ctx, box_coords="-1,49,x,55")
        self.assertEqual("HTTP 400: Received invalid bounding box geometry.", f"{cm.exception}")

    def test_find_features_by_wkt(self):
        ctx = new_test_service_context()
        feature_collection = find_features(ctx, geom_wkt="POLYGON ((2 49, 2 55, -1 55, -1 49, 2 49))")
        self._assertFeatureCollection(feature_collection, 2, {'1', '4'})

        with self.assertRaises(ServiceBadRequestError) as cm:
            find_features(ctx, geom_wkt="POLYGLON ((2 49, 2 55, -1 55, -1 49, 2 49))")
        self.assertEqual("HTTP 400: Received invalid geometry WKT.", f"{cm.exception}")

    def test_find_features_by_geojson(self):
        ctx = new_test_service_context()

        geojson_obj = {'type': 'Polygon',
                       'coordinates': (((2.0, 49.0), (2.0, 55.0), (-1.0, 55.0), (-1.0, 49.0), (2.0, 49.0)),)}
        feature_collection = find_features(ctx, geojson_obj=geojson_obj)
        self._assertFeatureCollection(feature_collection, 2, {'1', '4'})

        geojson_obj = {'type': 'Feature', 'properties': {}, 'geometry': geojson_obj}
        feature_collection = find_features(ctx, geojson_obj=geojson_obj)
        self._assertFeatureCollection(feature_collection, 2, {'1', '4'})

        geojson_obj = {'type': 'FeatureCollection', 'features': [geojson_obj]}
        feature_collection = find_features(ctx, geojson_obj=geojson_obj)
        self._assertFeatureCollection(feature_collection, 2, {'1', '4'})

        with self.assertRaises(ServiceBadRequestError) as cm:
            geojson_obj = {'type': 'FeatureCollection', 'features': []}
            find_features(ctx, geojson_obj=geojson_obj)
        self.assertEqual("HTTP 400: Received invalid GeoJSON object.", f"{cm.exception}")

    def test_find_dataset_features(self):
        ctx = new_test_service_context()
        feature_collection = find_dataset_features(ctx, "demo")
        self._assertFeatureCollection(feature_collection, 3, {'1', '2', '3'})

    def _assertFeatureCollection(self, feature_collection, expected_count, expected_ids):
        self.assertIsInstance(feature_collection, dict)
        self.assertIn("features", feature_collection)
        features = feature_collection["features"]
        self.assertIsInstance(features, list)
        self.assertEqual(expected_count, len(features))
        actual_ids = {f["properties"]["ID"] or f["properties"]["id"] for f in features}
        self.assertEqual(expected_ids, actual_ids)
