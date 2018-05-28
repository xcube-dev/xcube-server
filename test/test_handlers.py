from tornado.testing import AsyncHTTPTestCase

from test.helpers import new_demo_service_context
from xcts.app import new_application


# For usage of the tornado.testing.AsyncHTTPTestCase see http://www.tornadoweb.org/en/stable/testing.html

class HandlersTest(AsyncHTTPTestCase):
    def get_app(self):
        application = new_application()
        application.service_context = new_demo_service_context()
        return application

    def test_fetch_base(self):
        response = self.fetch('/')
        self.assertEqual(200, response.code)

    #def test_fetch_get_capabilities(self):
    #    response = self.fetch('/xcts-wmts/0.1.0/WMTSCapabilities.xml')
    #    self.assertEqual(200, response.code)

    def test_fetch_dataset_tile(self):
        response = self.fetch('/xcts/demo/conc_chl/tile/0/0/0.png')
        self.assertEqual(200, response.code)

    def test_fetch_dataset_tile_grid(self):
        response = self.fetch('/xcts/demo/conc_chl/tilegrid/ol4.json')
        self.assertEqual(200, response.code)

    def test_fetch_ne2_tile(self):
        response = self.fetch('/xcts/ne2/tile/0/0/0.jpg')
        self.assertEqual(200, response.code)

    def test_fetch_ne2_tile_grid(self):
        response = self.fetch('/xcts/ne2/tilegrid/ol4.json')
        self.assertEqual(200, response.code)
