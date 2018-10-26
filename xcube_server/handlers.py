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
from .controllers.tiles import get_dataset_tile, get_dataset_tile_grid, get_ne2_tile, get_ne2_tile_grid
from .controllers.wmts import get_wmts_capabilities
from .errors import ServiceResourceNotFoundError
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
class GetColorBarsHandler(ServiceRequestHandler):

    # noinspection PyShadowingBuiltins
    def get(self, format: str):
        mime_type = dict(json='application/json', html='text/html').get(format)
        if not mime_type:
            raise ServiceResourceNotFoundError("Invalid format.")
        response = get_color_bars(self.service_context, mime_type)
        self.set_header('Content-Type', mime_type)
        self.write(response)


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(dict(name='xcube_server',
                                   description=__description__,
                                   version=__version__), indent=2))
