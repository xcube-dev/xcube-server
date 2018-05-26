import unittest

from tornado.web import HTTPError

from xcts.errors import ServiceError, ServiceConfigError, ServiceRequestError


class ServiceRequestErrorTest(unittest.TestCase):
    def test_same_base_type(self):
        self.assertIsInstance(ServiceError(), HTTPError)
        self.assertIsInstance(ServiceConfigError(), ServiceError)
        self.assertIsInstance(ServiceRequestError(), ServiceError)
