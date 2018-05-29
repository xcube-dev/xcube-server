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


class WMTSCapabilitiesXmlHandler(ServiceRequestHandler):
    @gen.coroutine
    def get(self):
        capabilities = yield IOLoop.current().run_in_executor(None,
                                                              self.service_context.get_wmts_capabilities,
                                                              'text/xml',
                                                              self.base_url)
        self.set_header('Content-Type', 'text/xml')
        self.write(capabilities)


# noinspection PyAbstractClass,PyBroadException
class DatasetTileHandler(ServiceRequestHandler):

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
class DatasetTileGridHandler(ServiceRequestHandler):

    def get(self, ds_name: str, var_name: str, format_name: str):
        ts = self.service_context.get_dataset_tile_grid(ds_name, var_name, format_name, self.base_url)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class NE2TileHandler(ServiceRequestHandler):

    @gen.coroutine
    def get(self, z: str, x: str, y: str):
        tile = yield IOLoop.current().run_in_executor(None, self.service_context.get_ne2_tile, x, y, z, self.params)
        self.set_header('Content-Type', 'image/jpg')
        self.write(tile)


# noinspection PyAbstractClass
class NE2TileGridHandler(ServiceRequestHandler):

    def get(self, format_name: str):
        ts = self.service_context.get_ne2_tile_grid(format_name, self.base_url)
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(ts, indent=2))


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(dict(name='xcts', description=__description__, version=__version__), indent=2))
