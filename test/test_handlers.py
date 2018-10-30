from tornado.testing import AsyncHTTPTestCase

from test.helpers import new_test_service_context
from xcube_server.app import new_application


# For usage of the tornado.testing.AsyncHTTPTestCase see http://www.tornadoweb.org/en/stable/testing.html
from xcube_server.defaults import API_PREFIX


class HandlersTest(AsyncHTTPTestCase):
    def get_app(self):
        application = new_application()
        application.service_context = new_test_service_context()
        return application

    def test_fetch_base(self):
        response = self.fetch(API_PREFIX + '/')
        self.assertEqual(200, response.code)

    def test_fetch_wmts_capabilities(self):
        response = self.fetch(API_PREFIX + '/wmts/1.0.0/WMTSCapabilities.xml')
        self.assertEqual(200, response.code)

    def test_fetch_wmts_tile(self):
        response = self.fetch(API_PREFIX + '/wmts/1.0.0/tile/demo/conc_chl/0/0/0.png')
        self.assertEqual(200, response.code)

    def test_fetch_wmts_tile_with_params(self):
        response = self.fetch(API_PREFIX + '/wmts/1.0.0/tile/demo/conc_chl/0/0/0.png?time=current&cbar=jet')
        self.assertEqual(200, response.code)

    def test_fetch_dataset_tile(self):
        response = self.fetch(API_PREFIX + '/tile/demo/conc_chl/0/0/0.png')
        self.assertEqual(200, response.code)

    def test_fetch_dataset_tile_with_params(self):
        response = self.fetch(API_PREFIX + '/tile/demo/conc_chl/0/0/0.png?time=current&cbar=jet')
        self.assertEqual(200, response.code)

    def test_fetch_dataset_tile_grid_ol4_json(self):
        response = self.fetch(API_PREFIX + '/tilegrid/demo/conc_chl/ol4.json')
        self.assertEqual(200, response.code)

    def test_fetch_dataset_tile_grid_cesium_json(self):
        response = self.fetch(API_PREFIX + '/tilegrid/demo/conc_chl/cesium.json')
        self.assertEqual(200, response.code)

    def test_fetch_ne2_tile(self):
        response = self.fetch(API_PREFIX + '/tile/ne2/0/0/0.jpg')
        self.assertEqual(200, response.code)

    def test_fetch_ne2_tile_grid(self):
        response = self.fetch(API_PREFIX + '/tilegrid/ne2/ol4.json')
        self.assertEqual(200, response.code)

    def test_fetch_datasets_json(self):
        response = self.fetch(API_PREFIX + '/datasets.json')
        self.assertEqual(200, response.code)

    def test_fetch_variables_json(self):
        response = self.fetch(API_PREFIX + '/variables/demo.json')
        self.assertEqual(200, response.code)
        response = self.fetch(API_PREFIX + '/variables/demo.json?client=ol4')
        self.assertEqual(200, response.code)
        response = self.fetch(API_PREFIX + '/variables/demo.json?client=cesium')
        self.assertEqual(200, response.code)

    def test_fetch_coords_json(self):
        response = self.fetch(API_PREFIX + '/coords/demo/time.json')
        self.assertEqual(200, response.code)

    def test_fetch_color_bars_json(self):
        response = self.fetch(API_PREFIX + '/colorbars.json')
        self.assertEqual(200, response.code)

    def test_fetch_features(self):
        response = self.fetch(API_PREFIX + '/features.json?bbox=10,10,20,20')
        self.assertEqual(200, response.code)

    def test_fetch_features_for_dataset(self):
        response = self.fetch(API_PREFIX + '/features/demo.json')
        self.assertEqual(200, response.code)
