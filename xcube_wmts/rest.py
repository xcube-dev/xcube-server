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
import warnings

__author__ = "Norman Fomferra (Brockmann Consult GmbH), " \
             "Marco Zühlke (Brockmann Consult GmbH)"

import concurrent.futures
import os.path
import sys
import time

import numpy as np
import xarray as xr

from xcube_wmts.cache import Cache, MemoryCacheStore, FileCacheStore
from xcube_wmts.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage, GeoExtent, TilingScheme, Optional
from xcube_wmts.im.ds import NaturalEarth2Image
from xcube_wmts.service import WebAPIRequestHandler
from xcube_wmts import __version__

FILE_TILE_CACHE_CAPACITY = 1000
FILE_TILE_CACHE_ENABLED = False
FILE_TILE_CACHE_PATH = './image-cache'

MEM_TILE_CACHE_CAPACITY = 1000
MEM_TILE_CACHE = Cache(MemoryCacheStore(),
                       capacity=MEM_TILE_CACHE_CAPACITY,
                       threshold=0.75)

TRACE_PERF = False

THREAD_POOL = concurrent.futures.ThreadPoolExecutor()


# noinspection PyAbstractClass
class NE2Handler(WebAPIRequestHandler):
    PYRAMID = NaturalEarth2Image.get_pyramid()

    def get(self, z, y, x):
        # print('NE2Handler.get(%s, %s, %s)' % (z, y, x))
        self.set_header('Content-Type', 'image/jpg')
        self.write(NE2Handler.PYRAMID.get_tile(int(x), int(y), int(z)))


# noinspection PyAbstractClass,PyBroadException
class TileHandler(WebAPIRequestHandler):
    PYRAMIDS = None

    def get(self, ds_name, var_name, z, y, x):
        try:
            dataset = self.get_dataset(ds_name)
            cmap_name, cmap_min, cmap_max = self.get_color_mapping(var_name)

            # GLOBAL_LOCK.acquire()

            var_index = self.get_query_argument_int_tuple('index', ())
            cmap_name = self.get_query_argument('cmap', default=cmap_name)
            cmap_min = self.get_query_argument_float('vmin', default=cmap_min)
            cmap_max = self.get_query_argument_float('vmax', default=cmap_max)

            if TileHandler.PYRAMIDS is None:
                TileHandler.PYRAMIDS = dict()

            array_id = '%s-%s-%s' % (ds_name,
                                     var_name,
                                     ','.join(map(str, var_index)))
            image_id = '%s-%s-%s-%s' % (array_id,
                                        cmap_name,
                                        cmap_min,
                                        cmap_max)

            pyramid_id = 'impy-%s' % image_id

            if pyramid_id in TileHandler.PYRAMIDS:
                pyramid = TileHandler.PYRAMIDS[pyramid_id]
            else:
                variable = dataset[var_name]
                no_data_value = variable.attrs.get('_FillValue')
                valid_range = variable.attrs.get('valid_range')
                if valid_range is None:
                    valid_min = variable.attrs.get('valid_min')
                    valid_max = variable.attrs.get('valid_max')
                    if valid_min is not None and valid_max is not None:
                        valid_range = [valid_min, valid_max]

                # Make sure we work with 2D image arrays only
                if variable.ndim == 2:
                    array = variable
                elif variable.ndim > 2:
                    if not var_index or len(var_index) != variable.ndim - 2:
                        var_index = (0,) * (variable.ndim - 2)

                    # noinspection PyTypeChecker
                    var_index += (slice(None), slice(None),)

                    # print('var_index =', var_index)
                    array = variable[var_index]
                else:
                    self.write_status_error(message='Variable must be an N-D Dataset with N >= 2, '
                                                    'but "%s" is only %d-D' % (var_name, variable.ndim))
                    return

                cmap_min = np.nanmin(array.values) if np.isnan(cmap_min) else cmap_min
                cmap_max = np.nanmax(array.values) if np.isnan(cmap_max) else cmap_max
                # print('cmap_min =', cmap_min)
                # print('cmap_max =', cmap_max)

                mem_tile_cache = MEM_TILE_CACHE
                if FILE_TILE_CACHE_ENABLED:
                    rgb_tile_cache_dir = os.path.join(FILE_TILE_CACHE_PATH, 'v%s' % __version__, 'tiles')
                    rgb_tile_cache = Cache(FileCacheStore(rgb_tile_cache_dir, ".png"),
                                           capacity=FILE_TILE_CACHE_CAPACITY,
                                           threshold=0.75)
                else:
                    rgb_tile_cache = None

                def array_image_id_factory(level):
                    return 'arr-%s/%s' % (array_id, level)

                tiling_scheme = get_tiling_scheme(variable)
                if tiling_scheme is None:
                    self.write_status_error(
                        message='Internal error: failed to compute tiling scheme for array_id="%s"' % array_id)
                    return

                # print('tiling_scheme =', repr(tiling_scheme))
                pyramid = ImagePyramid.create_from_array(array, tiling_scheme,
                                                         level_image_id_factory=array_image_id_factory)
                pyramid = pyramid.apply(lambda image, level:
                                        TransformArrayImage(image,
                                                            image_id='tra-%s/%d' % (array_id, level),
                                                            flip_y=tiling_scheme.geo_extent.inv_y,
                                                            force_masked=True,
                                                            no_data_value=no_data_value,
                                                            valid_range=valid_range,
                                                            tile_cache=mem_tile_cache))
                pyramid = pyramid.apply(lambda image, level:
                                        ColorMappedRgbaImage(image,
                                                             image_id='rgb-%s/%d' % (image_id, level),
                                                             value_range=(cmap_min, cmap_max),
                                                             cmap_name=cmap_name,
                                                             encode=True,
                                                             format='PNG',
                                                             tile_cache=rgb_tile_cache))
                TileHandler.PYRAMIDS[image_id] = pyramid
                if TRACE_PERF:
                    print('Created pyramid "%s":' % image_id)
                    print('  tile_size:', pyramid.tile_size)
                    print('  num_level_zero_tiles:', pyramid.num_level_zero_tiles)
                    print('  num_levels:', pyramid.num_levels)

            if TRACE_PERF:
                print('PERF: >>> Tile:', image_id, z, y, x)

            t1 = time.clock()
            tile = pyramid.get_tile(int(x), int(y), int(z))
            t2 = time.clock()

            self.set_header('Content-Type', 'image/png')
            self.write(tile)

            if TRACE_PERF:
                print('PERF: <<< Tile:', image_id, z, y, x, 'took', t2 - t1, 'seconds')

            # GLOBAL_LOCK.release()

        except Exception:
            self.write_status_error(exc_info=sys.exc_info())

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_color_mapping(self, var_name):
        # TODO
        return 'jet', 0., 1.

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_dataset(self, ds_name):
        # TODO
        return xr.Dataset()


def get_tiling_scheme(var: xr.DataArray) -> Optional[TilingScheme]:
    """
    Compute a tiling scheme for the given variable *var*.

    :param var: A variable of an xarray dataset.
    :return:  a new TilingScheme object or None if *var* cannot be represented as a spatial image
    """
    lat_dim_name = 'lat'
    lon_dim_name = 'lon'
    if not lat_dim_name or not lon_dim_name:
        return None
    if lat_dim_name not in var.coords or lon_dim_name not in var.coords:
        return None
    width, height = var.shape[-1], var.shape[-2]
    lats = var.coords[lat_dim_name]
    lons = var.coords[lon_dim_name]
    try:
        geo_extent = GeoExtent.from_coord_arrays(lons, lats)
    except ValueError as e:
        warnings.warn(f'failed to derive geo-extent for tiling scheme: {e}')
        # Create a default geo-extent which is probably wrong, but at least we see something
        geo_extent = GeoExtent()
    try:
        return TilingScheme.create(width, height, 360, 360, geo_extent)
    except ValueError:
        return TilingScheme(1, 1, 1, width, height, geo_extent)
