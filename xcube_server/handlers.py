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

from tornado import gen
from tornado.ioloop import IOLoop

from . import __version__, __description__
from .service import ServiceRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


class GetWMTSCapabilitiesXmlHandler(ServiceRequestHandler):
    @gen.coroutine
    def get(self):
        capabilities = yield IOLoop.current().run_in_executor(None,
                                                              self.service_context.get_wmts_capabilities,
                                                              'text/xml',
                                                              self.base_url)
        self.set_header('Content-Type', 'text/xml')
        self.write(capabilities)


class GetDatasetsJsonHandler(ServiceRequestHandler):
    def get(self):
        dataset_descriptors = self.service_context.get_dataset_descriptors()
        datasets = list()
        for dataset_descriptor in dataset_descriptors:
            datasets.append(dict(name=dataset_descriptor['Identifier'],
                                 title=dataset_descriptor['Title']))
        response = dict(datasets=datasets)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(response))


class GetDatasetJsonHandler(ServiceRequestHandler):
    @gen.coroutine
    def get(self, ds_name: str):
        ds = self.service_context.get_dataset(ds_name)
        variables = list()
        for var_name in ds.data_vars:
            var = ds.data_vars[var_name]
            if 'time' not in var.dims or 'lat' not in var.dims or 'lon' not in var.dims:
                continue
            attrs = var.attrs
            variables.append(dict(id=f'{ds_name}{var_name}',
                                  name=var_name,
                                  dims=list(var.dims),
                                  shape=list(var.shape),
                                  dtype=str(var.dtype),
                                  units=attrs.get('units', ''),
                                  title=attrs.get('title', attrs.get('long_name', var_name))))
        attrs = ds.attrs
        response = dict(name=ds_name,
                        title=attrs.get('title', ''),
                        bbox=[attrs.get('geospatial_lon_min', -180),
                              attrs.get('geospatial_lat_min', -90),
                              attrs.get('geospatial_lon_max', +180),
                              attrs.get('geospatial_lat_max', +90)],
                        variables=variables)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(response))


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
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(response))


# noinspection PyAbstractClass,PyBroadException
class GetTileDatasetHandler(ServiceRequestHandler):

    @gen.coroutine
    def get(self, ds_name: str, var_name: str, z: str, x: str, y: str):
        tile = yield IOLoop.current().run_in_executor(None,
                                                      self.service_context.get_dataset_tile,
                                                      ds_name, var_name,
                                                      x, y, z,
                                                      self.params)
        self.set_header('Content-Type', 'image/png')
        self.write(tile)


# noinspection PyAbstractClass
class GetTileGridDatasetHandler(ServiceRequestHandler):

    def get(self, ds_name: str, var_name: str, format_name: str):
        import json

        ts = self.service_context.get_dataset_tile_grid(ds_name, var_name, format_name, self.base_url)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class GetTileNE2Handler(ServiceRequestHandler):

    @gen.coroutine
    def get(self, z: str, x: str, y: str):
        tile = yield IOLoop.current().run_in_executor(None, self.service_context.get_ne2_tile, x, y, z, self.params)
        self.set_header('Content-Type', 'image/jpg')
        self.write(tile)


# noinspection PyAbstractClass
class GetTileGridNE2Handler(ServiceRequestHandler):

    def get(self, format_name: str):
        ts = self.service_context.get_ne2_tile_grid(format_name, self.base_url)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class GetColorBarsHandler(ServiceRequestHandler):

    # noinspection PyAttributeOutsideInit
    def initialize(self, format_name: str = 'text/json'):
        self.format_name = format_name

    def get(self):
        response = self.service_context.get_color_bars(self.format_name)
        self.set_header('Content-Type', self.format_name)
        self.write(response)


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(dict(name='xcube_server', description=__description__, version=__version__), indent=2))
