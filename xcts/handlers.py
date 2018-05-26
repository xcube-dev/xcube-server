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
import concurrent.futures
import json
import os.path
import time
import warnings

import numpy as np
import s3fs
import xarray as xr
import zarr

from xcts import __version__, __description__
from xcts.cache import Cache, MemoryCacheStore, FileCacheStore
from xcts.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage, GeoExtent, TilingScheme, Optional
from xcts.im.ds import NaturalEarth2Image
from xcts.service import ServiceRequestHandler, ServiceError, ServiceConfigError

__author__ = "Norman Fomferra (Brockmann Consult GmbH)"

# TODO: configure in Service class
FILE_TILE_CACHE_CAPACITY = 1000
FILE_TILE_CACHE_ENABLED = False
FILE_TILE_CACHE_PATH = './image-cache'

# TODO: configure in Service class
MEM_TILE_CACHE_CAPACITY = 1000
# TODO: move into Service class
MEM_TILE_CACHE = Cache(MemoryCacheStore(),
                       capacity=MEM_TILE_CACHE_CAPACITY,
                       threshold=0.75)

# TODO: configure in Service class
TRACE_PERF = True

# TODO: move into Service class
THREAD_POOL = concurrent.futures.ThreadPoolExecutor()

# TODO: move into Service class
DATASET_CACHE = dict()

DEFAULT_CBAR = 'jet'
DEFAULT_VMIN = 0.
DEFAULT_VMAX = 1.


# noinspection PyAbstractClass,PyBroadException
class DatasetTileHandler(ServiceRequestHandler):
    # TODO: move into Service class
    PYRAMIDS = None

    def get(self, ds_name, var_name, z, y, x):
        dataset = self.get_dataset(ds_name)
        cmap_name, cmap_min, cmap_max = self.get_color_mapping(ds_name, var_name)

        # GLOBAL_LOCK.acquire()

        var_index = self.get_query_argument_int_tuple('index', ())
        cmap_name = self.get_query_argument('cmap', default=cmap_name)
        cmap_min = self.get_query_argument_float('vmin', default=cmap_min)
        cmap_max = self.get_query_argument_float('vmax', default=cmap_max)

        if DatasetTileHandler.PYRAMIDS is None:
            DatasetTileHandler.PYRAMIDS = dict()

        array_id = '%s-%s-%s' % (ds_name,
                                 var_name,
                                 ','.join(map(str, var_index)))
        image_id = '%s-%s-%s-%s' % (array_id,
                                    cmap_name,
                                    cmap_min,
                                    cmap_max)

        pyramid_id = 'impy-%s' % image_id

        if pyramid_id in DatasetTileHandler.PYRAMIDS:
            pyramid = DatasetTileHandler.PYRAMIDS[pyramid_id]
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
                raise ServiceError(reason='Variable must be an N-D Dataset with N >= 2, '
                                          f'but {var_name!r} is only {variable.ndim}-D')

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
                raise ServiceError(reason='Failed computing tiling scheme for array_id="%s"' % array_id)

            import pprint
            pprint.pprint(tiling_scheme)

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
            DatasetTileHandler.PYRAMIDS[image_id] = pyramid
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

    # TODO: move into Service class
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_dataset_descriptor(self, ds_name):
        datasets = self.service.config.get('datasets')
        if not datasets:
            raise ServiceConfigError(reason=f"missing datasets in configuration")
        if ds_name not in datasets:
            raise ServiceConfigError(reason=f"unknown dataset {ds_name!r}")
        return datasets[ds_name]

    # TODO: move into Service class
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_color_mapping(self, ds_name, var_name):
        dataset_descriptor = self.get_dataset_descriptor(ds_name)
        color_profile_name = dataset_descriptor.get('color_profile', 'default')
        color_profiles = dataset_descriptor.get('color_profiles')
        if color_profiles:
            color_profile = color_profiles.get(color_profile_name)
            if color_profile:
                cmap = color_profile.get('cmap', DEFAULT_CBAR)
                vmin, vmax = color_profile.get('vrange', (DEFAULT_VMIN, DEFAULT_VMAX))
                return cmap, vmin, vmax
        return DEFAULT_CBAR, DEFAULT_VMIN, DEFAULT_VMAX

    # TODO: move into Service class
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_dataset(self, ds_name):
        global DATASET_CACHE
        if ds_name in DATASET_CACHE:
            ds, _ = DATASET_CACHE[ds_name]
        else:
            dataset_descriptor = self.get_dataset_descriptor(ds_name)

            path = dataset_descriptor.get('path')
            if not path:
                raise ServiceConfigError(reason=f"missing 'path' entry in dataset descriptor {ds_name}")

            t1 = time.clock()

            fs_type = dataset_descriptor.get('fs', 'local')
            if fs_type == 'obs':
                data_format = dataset_descriptor.get('format', 'zarr')
                if data_format != 'zarr':
                    raise ServiceConfigError(reason=f"invalid format={data_format!r} in dataset descriptor {ds_name!r}")
                client_kwargs = {}
                if 'endpoint' in dataset_descriptor:
                    client_kwargs['endpoint_url'] = dataset_descriptor['endpoint']
                if 'region' in dataset_descriptor:
                    client_kwargs['region_name'] = dataset_descriptor['region']
                s3 = s3fs.S3FileSystem(anon=True, client_kwargs=client_kwargs)
                store = s3fs.S3Map(root=path, s3=s3, check=False)
                cached_store = zarr.LRUStoreCache(store, max_size=2 ** 28)
                ds = xr.open_zarr(cached_store)
            elif fs_type == 'local':
                data_format = dataset_descriptor.get('format', 'nc')
                if data_format == 'nc':
                    ds = xr.open_dataset(path)
                elif data_format == 'zarr':
                    ds = xr.open_zarr(path)
                else:
                    raise ServiceConfigError(reason=f"invalid format={data_format!r} in dataset descriptor {ds_name!r}")
            else:
                raise ServiceConfigError(reason=f"invalid fs={fs_type!r} in dataset descriptor {ds_name!r}")

            DATASET_CACHE[ds_name] = ds, dataset_descriptor

            t2 = time.clock()

            if TRACE_PERF:
                print(f'PERF: opening {ds_name!r} took {t2-t1} seconds')

        return ds


# noinspection PyAbstractClass
class DatasetTileSchemaHandler(ServiceRequestHandler):

    def get(self, ds_name, var_name):
        dataset = self.get_dataset(ds_name)
        variable = dataset[var_name]
        tiling_scheme = get_tiling_scheme(variable)
        if tiling_scheme is None:
            raise ServiceError(reason=f'Failed computing tiling scheme for variable {var_name!r}')
        self.set_header('Content-Type', 'text/json')
        # TODO
        self.write(json.dumps(dict(todo='Fill me!'), indent=2))


# noinspection PyAbstractClass
class NE2TileHandler(ServiceRequestHandler):
    # TODO: move into Service class
    PYRAMID = NaturalEarth2Image.get_pyramid()

    def get(self, z, y, x):
        # print('NE2Handler.get(%s, %s, %s)' % (z, y, x))
        self.set_header('Content-Type', 'image/jpg')
        self.write(NE2TileHandler.PYRAMID.get_tile(int(x), int(y), int(z)))


# noinspection PyAbstractClass
class NE2TileSchemaHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'text/json')
        # TODO
        self.write(json.dumps(dict(todo='Fill me!'), indent=2))


# noinspection PyAbstractClass
class InfoHandler(ServiceRequestHandler):

    def get(self):
        self.set_header('Content-Type', 'text/json')
        self.write(json.dumps(dict(name='xcts', description=__description__, version=__version__), indent=2))


def get_tiling_scheme(var: xr.DataArray) -> Optional[TilingScheme]:
    """
    Compute a tiling scheme for the given variable *var*.

    :param var: A variable of an xarray dataset.
    :return:  a new TilingScheme object or None if *var* cannot be represented as a spatial image
    """
    lat_dim_name = 'lat'
    lon_dim_name = 'lon'
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
