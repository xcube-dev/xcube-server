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

from xcube_wmts import __version__, __description__
from xcube_wmts.service import url_pattern, Service
from xcube_wmts.rest import NE2Handler, TileHandler
from xcube_wmts.common import LOGGER

from tornado.web import Application, StaticFileHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_LOG = LOGGER


def create_application():
    application = Application([
        ('/_static/(.*)', StaticFileHandler, {'path': os.path.join(os.path.dirname(__file__), 'resources')}),
        (url_pattern('/ws/res/tile/{{base_dir}}/{{res_id}}/{{z}}/{{y}}/{{x}}.png'), TileHandler),
        (url_pattern('/ws/ne2/tile/{{z}}/{{y}}/{{x}}.jpg'), NE2Handler),
    ])
    return application


def main(args=None) -> int:
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

    try:
        args_obj = parser.parse_args(args)

        service = Service()
        service.start(create_application(),
                      log_to_stderr=args_obj.verbose,
                      port=args_obj.port,
                      address=args_obj.address,
                      update_period=args_obj.update_period)

        return 0
    except Exception as e:
        print('error: %s' % e)
        return 1


if __name__ == '__main__':
    sys.exit(main())
