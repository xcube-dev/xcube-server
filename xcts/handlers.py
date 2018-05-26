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

from . import __version__, __description__
from .service import ServiceRequestHandler

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"


# noinspection PyAbstractClass,PyBroadException
class DatasetTileHandler(ServiceRequestHandler):

    # TODO: make this coroutine, see https://stackoverflow.com/questions/32374238/caching-and-reusing-a-function-result-in-tornado?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
    def get(self, ds_name: str, var_name: str, z: str, y: str, x: str):
        # GLOBAL_LOCK.acquire()

        x, y, z = int(x), int(y), int(z)
        var_index = self.get_query_argument_int_tuple('index', ())
        cmap_name = self.get_query_argument('cmap', default=None)
        cmap_min = self.get_query_argument_float('vmin', default=None)
        cmap_max = self.get_query_argument_float('vmax', default=None)

        tile = self.service_context.get_dataset_tile(ds_name, var_name,
                                                     x, y, z,
                                                     var_index,
                                                     cmap_name, cmap_min, cmap_max)

        self.set_header('Content-Type', 'image/png')
        self.write(tile)

        # GLOBAL_LOCK.release()


# noinspection PyAbstractClass
class DatasetTileSchemaHandler(ServiceRequestHandler):

    def get(self, ds_name: str, var_name: str, format_name: str):
        ts = self.service_context.get_dataset_tile_schema(ds_name, var_name, format_name)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class NE2TileHandler(ServiceRequestHandler):

    # TODO: make this coroutine, see https://stackoverflow.com/questions/32374238/caching-and-reusing-a-function-result-in-tornado?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
    def get(self, z: str, y: str, x: str):
        x, y, z = int(x), int(y), int(z)
        tile = self.service_context.get_ne2_tile(x, y, z)
        self.set_header('Content-Type', 'image/jpg')
        self.write(tile)


# noinspection PyAbstractClass
class NE2TileSchemaHandler(ServiceRequestHandler):

    def get(self, format_name: str):
        ts = self.service_context.get_ne2_tile_schema(format_name)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(dict(name='xcts', description=__description__, version=__version__), indent=2))
