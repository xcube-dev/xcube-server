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

from xcube_server import __version__, __description__
from xcube_server.defaults import DEFAULT_PORT, DEFAULT_NAME, DEFAULT_ADDRESS, DEFAULT_UPDATE_PERIOD, \
    DEFAULT_CONFIG_FILE, API_PREFIX
from xcube_server.handlers import GetNE2TileHandler, GetDatasetVarTileHandler, InfoHandler, GetNE2TileGridHandler, \
    GetDatasetVarTileGridHandler, GetWMTSCapabilitiesXmlHandler, GetColorBarsJsonHandler, GetColorBarsHtmlHandler, \
    GetDatasetsHandler, FindPlacesHandler, FindDatasetPlacesHandler, \
    GetDatasetCoordsHandler, GetTimeSeriesInfoHandler, GetTimeSeriesForPointHandler, WMTSKvpHandler, \
    GetTimeSeriesForGeometryHandler, GetTimeSeriesForFeaturesHandler, GetTimeSeriesForGeometriesHandler, \
    GetPlaceGroupsHandler, GetDatasetVarLegendHandler, GetDatasetHandler
from xcube_server.service import url_pattern, Service

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


def new_application(name: str = DEFAULT_NAME):
    prefix = f"/{name}{API_PREFIX}"
    application = Application([
        (prefix + '/res/(.*)',
         StaticFileHandler, {'path': os.path.join(os.path.dirname(__file__), 'res')}),
        (prefix + url_pattern('/'),
         InfoHandler),

        (prefix + url_pattern('/wmts/1.0.0/WMTSCapabilities.xml'),
         GetWMTSCapabilitiesXmlHandler),
        (prefix + url_pattern('/wmts/1.0.0/tile/{{ds_id}}/{{var_name}}/{{z}}/{{y}}/{{x}}.png'),
         GetDatasetVarTileHandler),
        (prefix + url_pattern('/wmts/kvp'),
         WMTSKvpHandler),

        # Natural Earth 2 tiles for testing

        (prefix + url_pattern('/datasets'),
         GetDatasetsHandler),
        (prefix + url_pattern('/datasets/{{ds_id}}'),
         GetDatasetHandler),
        (prefix + url_pattern('/datasets/{{ds_id}}/coords/{{dim_name}}'),
         GetDatasetCoordsHandler),
        (prefix + url_pattern('/datasets/{{ds_id}}/vars/{{var_name}}/legend.png'),
         GetDatasetVarLegendHandler),
        (prefix + url_pattern('/datasets/{{ds_id}}/vars/{{var_name}}/tiles/{{z}}/{{x}}/{{y}}.png'),
         GetDatasetVarTileHandler),
        (prefix + url_pattern('/datasets/{{ds_id}}/vars/{{var_name}}/tilegrid'),
         GetDatasetVarTileGridHandler),

        # Natural Earth 2 tiles for testing

        (prefix + url_pattern('/ne2/tilegrid'),
         GetNE2TileGridHandler),
        (prefix + url_pattern('/ne2/tiles/{{z}}/{{x}}/{{y}}.jpg'),
         GetNE2TileHandler),

        # Color Bars API

        (prefix + url_pattern('/colorbars'),
         GetColorBarsJsonHandler),
        (prefix + url_pattern('/colorbars.html'),
         GetColorBarsHtmlHandler),

        # Places API (PRELIMINARY & UNSTABLE - will be revised soon)

        (prefix + url_pattern('/places'),
         GetPlaceGroupsHandler),
        (prefix + url_pattern('/places/{{collection_name}}'),
         FindPlacesHandler),
        (prefix + url_pattern('/places/{{collection_name}}/{{ds_id}}'),
         FindDatasetPlacesHandler),

        # Time-series API (for VITO's DCS4COP viewer only, PRELIMINARY & UNSTABLE - will be revised soon)

        (prefix + url_pattern('/ts'),
         GetTimeSeriesInfoHandler),
        (prefix + url_pattern('/ts/{{ds_id}}/{{var_name}}/point'),
         GetTimeSeriesForPointHandler),
        (prefix + url_pattern('/ts/{{ds_id}}/{{var_name}}/geometry'),
         GetTimeSeriesForGeometryHandler),
        (prefix + url_pattern('/ts/{{ds_id}}/{{var_name}}/geometries'),
         GetTimeSeriesForGeometriesHandler),
        (prefix + url_pattern('/ts/{{ds_id}}/{{var_name}}/places'),
         GetTimeSeriesForFeaturesHandler),
    ])
    return application


def new_service(args=None) -> Service:
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('--version', '-V', action='version', version=__version__)
    parser.add_argument('--name', '-n', dest='name', metavar='NAME',
                        help='Service name. '
                             f'Defaults to {DEFAULT_NAME!r}.',
                        default=DEFAULT_NAME)
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

    return Service(new_application(args_obj.name),
                   name=args_obj.name,
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
