# The MIT License (MIT)
# Copyright (c) 2018 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import argparse
import os
import sys

from tornado.web import Application, StaticFileHandler

from xcts import __version__, __description__
from xcts.common import LOGGER
from xcts.handlers import NE2Handler, TileHandler, InfoHandler
from xcts.service import url_pattern, Service

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_LOG = LOGGER


def new_application():
    application = Application([
        ('/_static/(.*)', StaticFileHandler, {'path': os.path.join(os.path.dirname(__file__), 'resources')}),
        (url_pattern('/'), InfoHandler),
        (url_pattern('/xcts/{{ds_name}}/{{var_name}}/{{z}}/{{y}}/{{x}}.png'), TileHandler),
        (url_pattern('/xcts/ne2/{{z}}/{{y}}/{{x}}.jpg'), NE2Handler),
    ])
    return application


def new_service(args=None) -> Service:
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('--version', '-V', action='version', version=__version__)
    parser.add_argument('--port', '-p', dest='port', metavar='PORT', type=int, default=8080,
                        help='port number where the service will listen on')
    parser.add_argument('--address', '-a', dest='address', metavar='ADDRESS',
                        help='server address, if omitted "localhost" is used', default='localhost')
    parser.add_argument('--update', '-u', dest='update_period', metavar='UPDATE_PERIOD', type=float, default=1.,
                        help="if given, service will update after given seconds of inactivity")
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true',
                        help="if given, logging will be delegated to the console (stderr)")

    args_obj = parser.parse_args(args)

    return Service(new_application(),
                   log_to_stderr=args_obj.verbose,
                   port=args_obj.port,
                   address=args_obj.address,
                   update_period=args_obj.update_period)


def main(args=None) -> int:
    try:
        print(f'{__description__}, version {__version__}')
        service = new_service(args)
        service.start()
        return 0
    except Exception as e:
        print('error: %s' % e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
