import unittest

from xcube_server.context import ServiceContext
from xcube_server.controllers.catalogue import get_color_bars
from xcube_server.errors import ServiceBadRequestError


class CatalogueControllerTest(unittest.TestCase):

    def test_get_colorbars(self):
        ctx = ServiceContext()

        response = get_color_bars(ctx, 'application/json')
        self.assertIsInstance(response, str)
        self.assertTrue(len(response) > 40)
        self.assertEqual('[\n  [\n    "Perceptually Uniform Sequenti', response[0:40])

        response = get_color_bars(ctx, 'text/html')
        self.assertIsInstance(response, str)
        self.assertTrue(len(response) > 40)
        self.assertEqual('<!DOCTYPE html>\n<html lang="en">\n<head><', response[0:40])

        with self.assertRaises(ServiceBadRequestError) as cm:
            get_color_bars(ctx, 'text/xml')
        self.assertEqual(400, cm.exception.status_code)
        self.assertEqual("Format 'text/xml' not supported for color bars", cm.exception.reason)
