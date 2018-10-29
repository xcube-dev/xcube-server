import os
import unittest

from test.helpers import get_res_test_dir, new_test_service_context
from xcube_server.controllers.wmts import get_wmts_capabilities


class WmtsControllerTest(unittest.TestCase):

    def test_get_wmts_capabilities(self):
        self.maxDiff = None
        with open(os.path.join(get_res_test_dir(), 'WMTSCapabilities.xml')) as fp:
            expected_capabilities = fp.read()
        ctx = new_test_service_context()
        capabilities = get_wmts_capabilities(ctx, 'application/xml', 'http://bibo')
        # print(80 * '=')
        # print(capabilities)
        # print(80 * '=')
        self.assertEqual(expected_capabilities.replace(' ', ''), capabilities.replace(' ', ''))
