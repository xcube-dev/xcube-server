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

from . import __version__, __description__
from .controllers.catalogue import get_datasets, get_dataset_variables, get_dataset_coordinates, get_color_bars
from .controllers.features import find_features, find_dataset_features
from .controllers.tiles import get_dataset_tile, get_dataset_tile_grid, get_ne2_tile, get_ne2_tile_grid
from .controllers.time_series import get_time_series_info, get_time_series_for_point, get_time_series_for_geometry, \
    get_time_series_for_geometry_collection, get_time_series_for_feature_collection
from .controllers.wmts import get_wmts_capabilities_xml
from .errors import ServiceBadRequestError
from .service import ServiceRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

_WMTS_KVP_KEYS = [
    'Service',
    'Request',
    'Version',
    'Format',
    'Style',
    'Layer',
    'TileMatrixSet',
    'TileMatrix',
    'TileRow',
    'TileCol'
]

_WMTS_KVP_LOWER_KEYS = [k.lower() for k in _WMTS_KVP_KEYS]


# noinspection PyAbstractClass
class WMTSKvpHandler(ServiceRequestHandler):

    async def get(self):
        # According to WMTS 1.0 spec, all WMTS-specific keys must be case insensitive.
        self._convert_wmts_keys_to_lower_case()

        service = self.params.get_query_argument('service')
        if service != "WMTS":
            raise ServiceBadRequestError('Value for "service" parameter must be "WMTS"')
        request = self.params.get_query_argument('request')
        if request == "GetCapabilities":
            capabilities = await IOLoop.current().run_in_executor(None,
                                                                  get_wmts_capabilities_xml,
                                                                  self.service_context,
                                                                  self.base_url)
            self.set_header('Content-Type', 'application/xml')
            self.finish(capabilities)
        elif request == "GetTile":
            version = self.params.get_query_argument('version')
            if version != "1.0.0":
                raise ServiceBadRequestError('Value for "version" parameter must be "1.0.0"')
            layer = self.params.get_query_argument('layer')
            try:
                ds_name, var_name = layer.split(".")
            except ValueError as e:
                raise ServiceBadRequestError('Value for "layer" parameter must be "<dataset>.<variable>"') from e
            # The following parameters are mandatory s prescribed by WMTS spec, but we don't need them
            # tileMatrixSet = self.params.get_query_argument_int('tilematrixset')
            # style = self.params.get_query_argument('style')
            mime_type = self.params.get_query_argument('format')
            if mime_type != "image/png":
                raise ServiceBadRequestError('Value for "format" parameter must be "image/png"')
            x = self.params.get_query_argument_int('tilecol')
            y = self.params.get_query_argument_int('tilerow')
            z = self.params.get_query_argument_int('tilematrix')
            tile = await IOLoop.current().run_in_executor(None,
                                                          get_dataset_tile,
                                                          self.service_context,
                                                          ds_name, var_name,
                                                          x, y, z,
                                                          self.params)
            self.set_header('Content-Type', 'image/png')
            self.finish(tile)
        elif request == "GetFeatureInfo":
            raise ServiceBadRequestError('Request type "GetFeatureInfo" not yet implemented')
        else:
            raise ServiceBadRequestError(f'Invalid request type "{request}"')

    def _convert_wmts_keys_to_lower_case(self):
        query_arguments = dict(self.request.query_arguments)
        query_keys = {k.lower(): k for k in query_arguments.keys()}
        for lower_key in _WMTS_KVP_LOWER_KEYS:
            if lower_key in query_keys:
                query_key = query_keys[lower_key]
                value = query_arguments[query_key]
                del query_arguments[query_key]
                query_arguments[lower_key] = value
        self.request.query_arguments = query_arguments


# noinspection PyAbstractClass
class GetWMTSCapabilitiesXmlHandler(ServiceRequestHandler):

    async def get(self):
        capabilities = await IOLoop.current().run_in_executor(None,
                                                              get_wmts_capabilities_xml,
                                                              self.service_context,
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
            raise ServiceBadRequestError('Only one of "geom" and "bbox" may be given')
        response = find_features(self.service_context,
                                 geom_wkt=geom_wkt, box_coords=box_coords,
                                 query_expr=query_expr, comb_op=comb_op)
        self.set_header('Content-Type', "application/json")
        self.write(json.dumps(response, indent=2))

    # noinspection PyShadowingBuiltins
    def post(self):
        query_expr = self.params.get_query_argument("query", None)
        comb_op = self.params.get_query_argument("comb", "and")
        geojson_obj = self.get_body_as_json_object()
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
        lon = self.params.get_query_argument_float('lon')
        lat = self.params.get_query_argument_float('lat')
        start_date = self.params.get_query_argument_datetime('startDate', default=None)
        end_date = self.params.get_query_argument_datetime('endDate', default=None)

        response = await IOLoop.current().run_in_executor(None,
                                                          get_time_series_for_point,
                                                          self.service_context,
                                                          ds_name, var_name,
                                                          lon, lat,
                                                          start_date, end_date)
        self.set_header('Content-Type', 'application/json')
        self.finish(response)


# noinspection PyAbstractClass
class TimeSeriesForGeometryHandler(ServiceRequestHandler):

    async def post(self, ds_name: str, var_name: str):
        start_date = self.params.get_query_argument_datetime('startDate', default=None)
        end_date = self.params.get_query_argument_datetime('endDate', default=None)
        geometry = self.get_body_as_json_object("GeoJSON geometry")

        response = await IOLoop.current().run_in_executor(None,
                                                          get_time_series_for_geometry,
                                                          self.service_context,
                                                          ds_name, var_name,
                                                          geometry,
                                                          start_date, end_date)
        self.set_header('Content-Type', 'application/json')
        self.finish(response)


# noinspection PyAbstractClass
class TimeSeriesForGeometriesHandler(ServiceRequestHandler):

    async def post(self, ds_name: str, var_name: str):
        start_date = self.params.get_query_argument_datetime('startDate', default=None)
        end_date = self.params.get_query_argument_datetime('endDate', default=None)
        geometry_collection = self.get_body_as_json_object("GeoJSON geometry collection")

        response = await IOLoop.current().run_in_executor(None,
                                                          get_time_series_for_geometry_collection,
                                                          self.service_context,
                                                          ds_name, var_name,
                                                          geometry_collection,
                                                          start_date, end_date)
        self.set_header('Content-Type', 'application/json')
        self.finish(response)


# noinspection PyAbstractClass
class TimeSeriesForFeaturesHandler(ServiceRequestHandler):

    async def post(self, ds_name: str, var_name: str):
        start_date = self.params.get_query_argument_datetime('startDate', default=None)
        end_date = self.params.get_query_argument_datetime('endDate', default=None)
        feature_collection = self.get_body_as_json_object("GeoJSON feature collection")

        response = await IOLoop.current().run_in_executor(None,
                                                          get_time_series_for_feature_collection,
                                                          self.service_context,
                                                          ds_name, var_name,
                                                          feature_collection,
                                                          start_date, end_date)
        self.set_header('Content-Type', 'application/json')
        self.finish(response)
