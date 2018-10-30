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

import json

from tornado.ioloop import IOLoop

from xcube_server.controllers.features import find_features, find_dataset_features
from . import __version__, __description__
from .controllers.catalogue import get_datasets, get_dataset_variables, get_dataset_coordinates, get_color_bars
from .controllers.tiles import get_dataset_tile, get_dataset_tile_grid, get_ne2_tile, get_ne2_tile_grid
from .controllers.time_series import get_time_series_info, get_time_series_for_point
from .controllers.wmts import get_wmts_capabilities
from .errors import ServiceResourceNotFoundError, ServiceBadRequestError
from .service import ServiceRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


# noinspection PyAbstractClass
class GetWMTSCapabilitiesXmlHandler(ServiceRequestHandler):

    async def get(self):
        capabilities = await IOLoop.current().run_in_executor(None,
                                                              get_wmts_capabilities,
                                                              self.service_context,
                                                              'application/xml',
                                                              self.base_url)
        self.set_header('Content-Type', 'application/xml')
        self.finish(capabilities)


# noinspection PyAbstractClass
class GetDatasetsJsonHandler(ServiceRequestHandler):

    def get(self):
        response = get_datasets(self.service_context)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass
class GetVariablesJsonHandler(ServiceRequestHandler):

    def get(self, ds_name: str):
        client = self.params.get_query_argument('client', 'ol4')
        response = get_dataset_variables(self.service_context, ds_name, client, self.base_url)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass
class GetCoordinatesJsonHandler(ServiceRequestHandler):

    def get(self, ds_name: str, dim_name: str):
        response = get_dataset_coordinates(self.service_context, ds_name, dim_name)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass,PyBroadException
class GetTileDatasetHandler(ServiceRequestHandler):

    async def get(self, ds_name: str, var_name: str, z: str, x: str, y: str):
        tile = await IOLoop.current().run_in_executor(None,
                                                      get_dataset_tile,
                                                      self.service_context,
                                                      ds_name, var_name,
                                                      x, y, z,
                                                      self.params)
        self.set_header('Content-Type', 'image/png')
        self.finish(tile)


# noinspection PyAbstractClass
class GetTileGridDatasetHandler(ServiceRequestHandler):

    def get(self, ds_name: str, var_name: str, format_name: str):
        response = get_dataset_tile_grid(self.service_context,
                                         ds_name, var_name,
                                         format_name, self.base_url)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass
class GetTileNE2Handler(ServiceRequestHandler):

    async def get(self, z: str, x: str, y: str):
        response = await IOLoop.current().run_in_executor(None,
                                                          get_ne2_tile,
                                                          self.service_context,
                                                          x, y, z,
                                                          self.params)
        self.set_header('Content-Type', 'image/jpg')
        self.finish(response)


# noinspection PyAbstractClass
class GetTileGridNE2Handler(ServiceRequestHandler):

    def get(self, format_name: str):
        response = get_ne2_tile_grid(self.service_context, format_name, self.base_url)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass
class GetColorBarsJsonHandler(ServiceRequestHandler):

    # noinspection PyShadowingBuiltins
    def get(self):
        mime_type = 'application/json'
        response = get_color_bars(self.service_context, mime_type)
        self.set_header('Content-Type', mime_type)
        self.write(response)


# noinspection PyAbstractClass
class GetColorBarsHtmlHandler(ServiceRequestHandler):

    # noinspection PyShadowingBuiltins
    def get(self):
        mime_type = 'text/html'
        response = get_color_bars(self.service_context, mime_type)
        self.set_header('Content-Type', mime_type)
        self.write(response)


# noinspection PyAbstractClass
class FindFeaturesHandler(ServiceRequestHandler):

    # noinspection PyShadowingBuiltins
    def get(self):
        query_expr = self.params.get_query_argument("query", None)
        geom_wkt = self.params.get_query_argument("geom", None)
        box_coords = self.params.get_query_argument("bbox", None)
        comb_op = self.params.get_query_argument("comb", "and")
        if geom_wkt and box_coords:
            raise ServiceBadRequestError('Only one of "geom" and "bbox" may be given.')
        response = find_features(self.service_context,
                                 geom_wkt=geom_wkt, box_coords=box_coords,
                                 query_expr=query_expr, comb_op=comb_op)
        self.set_header('Content-Type', "application/json")
        self.write(json.dumps(response, indent=2))

    # noinspection PyShadowingBuiltins
    def post(self):
        query_expr = self.params.get_query_argument("query", None)
        comb_op = self.params.get_query_argument("comb", "and")
        geojson_obj = json.loads(self.request.body.decode('utf-8'))
        response = find_features(self.service_context,
                                 geojson_obj=geojson_obj,
                                 query_expr=query_expr, comb_op=comb_op)
        self.set_header('Content-Type', "application/json")
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass
class FindDatasetFeaturesHandler(ServiceRequestHandler):

    # noinspection PyShadowingBuiltins
    def get(self, ds_name: str):
        query_expr = self.params.get_query_argument("query", None)
        comb_op = self.params.get_query_argument("comb", "and")
        response = find_dataset_features(self.service_context,
                                         ds_name, query_expr=query_expr, comb_op=comb_op)
        self.set_header('Content-Type', "application/json")
        self.write(json.dumps(response, indent=2))


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(dict(name='xcube_server',
                                   description=__description__,
                                   version=__version__), indent=2))


# noinspection PyAbstractClass
class TimeSeriesInfoHandler(ServiceRequestHandler):

    async def get(self):
        response = await IOLoop.current().run_in_executor(None, get_time_series_info, self.service_context)
        self.set_header('Content-Type', 'application/json')
        self.finish(response)


# noinspection PyAbstractClass
class TimeSeriesForPointHandler(ServiceRequestHandler):

    async def get(self, ds_name: str, var_name: str):
        response = await IOLoop.current().run_in_executor(None,
                                                          get_time_series_for_point,
                                                          self.service_context,
                                                          ds_name, var_name,
                                                          self.params)
        self.set_header('Content-Type', 'application/json')
        self.finish(response)
