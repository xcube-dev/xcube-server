import unittest

from tornado.ioloop import IOLoop

from xcts.app import new_service


class AppSmokeTest(unittest.TestCase):

    def test_start_stop_service(self):
        service = new_service(args=['--port', '20001'])
        IOLoop.current().call_later(0.1, service.stop)
        service.start()
