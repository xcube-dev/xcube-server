import unittest

from test.helpers import new_test_service_context, RequestParamsMock
from xcube_server.context import ServiceContext
from xcube_server.controllers.features import find_features, find_dataset_features
from xcube_server.controllers.tiles import get_dataset_tile, get_ne2_tile, get_dataset_tile_grid, get_ne2_tile_grid
from xcube_server.defaults import API_PREFIX
from xcube_server.errors import ServiceBadRequestError


class FeaturesControllerTest(unittest.TestCase):

    def test_find_features(self):
        ctx = new_test_service_context()
        feature_collection = find_features(ctx)
        self.assertIsInstance(feature_collection, dict)
        self.assertIn("features", feature_collection)

    def test_find_dataset_features(self):
        ctx = new_test_service_context()
        feature_collection = find_dataset_features(ctx, "demo")
        self.assertIsInstance(feature_collection, dict)
        self.assertIn("features", feature_collection)
