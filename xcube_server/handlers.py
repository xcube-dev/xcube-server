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

from xcube_server.context import get_tile_source_options
from . import __version__, __description__
from .service import ServiceRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


# noinspection PyAbstractClass
class GetWMTSCapabilitiesXmlHandler(ServiceRequestHandler):

    async def get(self):
        capabilities = await IOLoop.current().run_in_executor(None,
                                                              self.service_context.get_wmts_capabilities,
                                                              'application/xml',
                                                              self.base_url)
        self.set_header('Content-Type', 'application/xml')
        self.finish(capabilities)


# noinspection PyAbstractClass
class GetDatasetsJsonHandler(ServiceRequestHandler):

    def get(self):
        dataset_descriptors = self.service_context.get_dataset_descriptors()
        datasets = list()
        for dataset_descriptor in dataset_descriptors:
            datasets.append(dict(name=dataset_descriptor['Identifier'],
                                 title=dataset_descriptor['Title']))
        response = dict(datasets=datasets)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response))


# noinspection PyAbstractClass
class GetVariablesJsonHandler(ServiceRequestHandler):

    def get(self, ds_name: str):
        ds = self.service_context.get_dataset(ds_name)
        client = self.params.get_query_argument('client', 'ol4')
        variables = list()
        for var_name in ds.data_vars:
            var = ds.data_vars[var_name]
            if 'time' not in var.dims or 'lat' not in var.dims or 'lon' not in var.dims:
                continue
            attrs = var.attrs
            tile_grid = self.service_context.get_or_compute_tile_grid(ds_name, var)
            ol_tile_xyz_source_options = get_tile_source_options(tile_grid,
                                                                 self.service_context.get_dataset_tile_url(
                                                                     ds_name, var_name, self.base_url),
                                                                 client)
            variables.append(dict(id=f'{ds_name}{var_name}',
                                  name=var_name,
                                  dims=list(var.dims),
                                  shape=list(var.shape),
                                  dtype=str(var.dtype),
                                  units=attrs.get('units', ''),
                                  title=attrs.get('title', attrs.get('long_name', var_name)),
                                  tileSourceOptions=ol_tile_xyz_source_options))
        attrs = ds.attrs
        response = dict(name=ds_name,
                        title=attrs.get('title', ''),
                        bbox=[attrs.get('geospatial_lon_min', -180),
                              attrs.get('geospatial_lat_min', -90),
                              attrs.get('geospatial_lon_max', +180),
                              attrs.get('geospatial_lat_max', +90)],
                        variables=variables)
        self.set_header('Content-Type', 'application/json')
        self.finish(json.dumps(response))


# noinspection PyAbstractClass
class GetCoordinatesJsonHandler(ServiceRequestHandler):

    def get(self, ds_name: str, dim_name: str):
        import numpy as np
        ds, var = self.service_context.get_dataset_and_coord_variable(ds_name, dim_name)
        values = list()
        if np.issubdtype(var.dtype, np.floating):
            converter = float
        elif np.issubdtype(var.dtype, np.integer):
            converter = int
        else:
            converter = str
        for value in var.values:
            values.append(converter(value))
        response = dict(name=dim_name,
                        dtype=str(var.dtype),
                        values=values)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(response))


# noinspection PyAbstractClass,PyBroadException
class GetTileDatasetHandler(ServiceRequestHandler):

    async def get(self, ds_name: str, var_name: str, z: str, x: str, y: str):
        tile = await IOLoop.current().run_in_executor(None,
                                                      self.service_context.get_dataset_tile,
                                                      ds_name, var_name,
                                                      x, y, z,
                                                      self.params)
        self.set_header('Content-Type', 'image/png')
        self.finish(tile)


# noinspection PyAbstractClass
class GetTileGridDatasetHandler(ServiceRequestHandler):

    def get(self, ds_name: str, var_name: str, format_name: str):
        ts = self.service_context.get_dataset_tile_grid(ds_name, var_name,
                                                        format_name, self.base_url)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class GetTileNE2Handler(ServiceRequestHandler):

    async def get(self, z: str, x: str, y: str):
        tile = await IOLoop.current().run_in_executor(None,
                                                      self.service_context.get_ne2_tile,
                                                      x, y, z,
                                                      self.params)
        self.set_header('Content-Type', 'image/jpg')
        self.finish(tile)


# noinspection PyAbstractClass
class GetTileGridNE2Handler(ServiceRequestHandler):

    def get(self, format_name: str):
        ts = self.service_context.get_ne2_tile_grid(format_name, self.base_url)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class GetColorBarsHandler(ServiceRequestHandler):

    # noinspection PyAttributeOutsideInit
    def initialize(self, format_name: str = 'application/json'):
        self.format_name = format_name

    def get(self):
        response = self.service_context.get_color_bars(self.format_name)
        self.set_header('Content-Type', self.format_name)
        self.write(response)


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(dict(name='xcube_server',
                                   description=__description__,
                                   version=__version__), indent=2))
