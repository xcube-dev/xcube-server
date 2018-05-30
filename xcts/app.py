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
from xcts.handlers import NE2TileHandler, DatasetTileHandler, InfoHandler, NE2TileGridHandler, \
    DatasetTileGridHandler, WMTSCapabilitiesXmlHandler, ColorBarsJsonHandler
from xcts.service import url_pattern, Service, DEFAULT_PORT, DEFAULT_ADDRESS, DEFAULT_UPDATE_PERIOD, DEFAULT_CONFIG_FILE

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


def new_application():
    application = Application([
        ('/res/(.*)', StaticFileHandler, {'path': os.path.join(os.path.dirname(__file__), 'res')}),
        (url_pattern('/'), InfoHandler),
        (url_pattern('/xcts-wmts/1.0.0/WMTSCapabilities.xml'), WMTSCapabilitiesXmlHandler),
        (url_pattern('/xcts-wmts/1.0.0/tile/{{ds_name}}/{{var_name}}/{{z}}/{{y}}/{{x}}.png'), DatasetTileHandler),
        (url_pattern('/xcts/tile/{{ds_name}}/{{var_name}}/{{z}}/{{x}}/{{y}}.png'), DatasetTileHandler),
        (url_pattern('/xcts/tilegrid/{{ds_name}}/{{var_name}}/{{format_name}}'), DatasetTileGridHandler),
        (url_pattern('/xcts/tile/ne2/{{z}}/{{x}}/{{y}}.jpg'), NE2TileHandler),
        (url_pattern('/xcts/tilegrid/ne2/{{format_name}}'), NE2TileGridHandler),
        (url_pattern('/xcts/colorbars.json'), ColorBarsJsonHandler, dict(format_name='text/json')),
        (url_pattern('/xcts/colorbars.html'), ColorBarsJsonHandler, dict(format_name='text/html')),
    ])
    return application


def new_service(args=None) -> Service:
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('--version', '-V', action='version', version=__version__)
    parser.add_argument('--address', '-a', dest='address', metavar='ADDRESS',
                        help='Server address. '
                             f'Defaults to {DEFAULT_ADDRESS!r}.',
                        default=DEFAULT_ADDRESS)
    parser.add_argument('--port', '-p', dest='port', metavar='PORT', type=int,
                        default=DEFAULT_PORT,
                        help='Port number where the service will listen on. '
                             f'Defaults to {DEFAULT_PORT}.')
    parser.add_argument('--update', '-u', dest='update_period', metavar='UPDATE_PERIOD', type=float,
                        default=DEFAULT_UPDATE_PERIOD,
                        help='Service will update after given seconds of inactivity. Zero or a negative value will '
                             'disable update checks. '
                             f'Defaults to {DEFAULT_UPDATE_PERIOD!r}.')
    parser.add_argument('--config', '-c', dest='config_file', metavar='CONFIG_FILE', default=None,
                        help='Configuration file. '
                             f'Defaults to {DEFAULT_CONFIG_FILE!r}.')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true',
                        help="if given, logging will be delegated to the console (stderr)")

    args_obj = parser.parse_args(args)

    return Service(new_application(),
                   log_to_stderr=args_obj.verbose,
                   port=args_obj.port,
                   address=args_obj.address,
                   config_file=args_obj.config_file,
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
