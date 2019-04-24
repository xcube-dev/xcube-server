import unittest
from xcube_server.cli import main

class CliSmokeTest(unittest.TestCase):

    def test_help(self):
        with self.assertRaises(SystemExit):
            main(args=["--help"])
