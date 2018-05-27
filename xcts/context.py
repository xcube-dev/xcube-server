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
import logging
import os
import time
from typing import Any, Dict, Sequence, Optional

import numpy as np
import s3fs
import xarray as xr
import zarr

from . import __version__
from .cache import MemoryCacheStore, Cache, FileCacheStore
from .defaults import DEFAULT_MAX_THREAD_COUNT, DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, \
    DEFAULT_CMAP_VMAX, TRACE_PERF, MEM_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_PATH, \
    FILE_TILE_CACHE_ENABLED
from .errors import ServiceConfigError, ServiceError, ServiceRequestError
from .im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage, TileGrid
from .im.ne2 import NaturalEarth2Image
from .tile import compute_tile_grid

_LOG = logging.getLogger('xcts')

Config = Dict[str, Any]


class ServiceContext:

    def __init__(self, base_dir=None, config: Config = None):
        self.base_dir = os.path.abspath(base_dir or '')
        self._config = config or dict()
        self.dataset_cache = dict()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=DEFAULT_MAX_THREAD_COUNT,
                                                                 thread_name_prefix='xcts')
        self.pyramid_cache = dict()
        self.mem_tile_cache = Cache(MemoryCacheStore(),
                                    capacity=MEM_TILE_CACHE_CAPACITY,
                                    threshold=0.75)
        if FILE_TILE_CACHE_ENABLED:
            tile_cache_dir = os.path.join(FILE_TILE_CACHE_PATH, 'v%s' % __version__, 'tiles')
            self.rgb_tile_cache = Cache(FileCacheStore(tile_cache_dir, ".png"),
                                        capacity=FILE_TILE_CACHE_CAPACITY,
                                        threshold=0.75)
        else:
            self.rgb_tile_cache = None

    @property
    def config(self) -> Config:
        return self._config

    @config.setter
    def config(self, config: Config):
        if self._config:
            old_dataset_descriptors = self._config.get('datasets')
            new_dataset_descriptors = config.get('datasets')
            if not new_dataset_descriptors:
                for ds, _ in self.dataset_cache.values():
                    ds.close()
                self.dataset_cache.clear()
            if new_dataset_descriptors and old_dataset_descriptors:
                for ds_name in self.dataset_cache.keys():
                    if ds_name not in new_dataset_descriptors:
                        ds, _ = old_dataset_descriptors[ds_name]
                        ds.close()
                        del self.dataset_cache[ds_name]
        self._config = config

    # TODO: make this a generator using self.thread_pool
    def get_dataset_tile(self,
                         ds_name: str,
                         var_name: str,
                         x: int, y: int, z: int,
                         var_index: Optional[Sequence[int]] = None,
                         cmap_cbar: Optional[str] = None,
                         cmap_vmin: Optional[float] = None,
                         cmap_vmax: Optional[float] = None):

        if cmap_cbar is None or cmap_vmin is None or cmap_vmax is None:
            default_cmap_cbar, default_cmap_vmin, default_cmap_vmax = self.get_color_mapping(ds_name, var_name)
            cmap_cbar = cmap_cbar or default_cmap_cbar
            cmap_vmin = cmap_vmin or default_cmap_vmin
            cmap_vmax = cmap_vmax or default_cmap_vmax

        array_id = '%s-%s' % (ds_name, var_name)
        if var_index and len(var_index) > 0:
            array_id += '-%s' % ','.join(map(str, var_index))

        image_id = '%s-%s-%s-%s' % (array_id, cmap_cbar, cmap_vmin, cmap_vmax)

        pyramid_id = 'pyramid-%s' % image_id

        if pyramid_id in self.pyramid_cache:
            pyramid = self.pyramid_cache[pyramid_id]
        else:
            dataset, variable = self.get_dataset_and_variable(ds_name, var_name)

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
                raise ServiceError(reason=f'Variable {var_name!r} of dataset {var_name!r} '
                                          'must be an N-D Dataset with N >= 2, '
                                          f'but {var_name!r} is only {variable.ndim}-D')

            cmap_vmin = np.nanmin(array.values) if np.isnan(cmap_vmin) else cmap_vmin
            cmap_vmax = np.nanmax(array.values) if np.isnan(cmap_vmax) else cmap_vmax

            # print('cmap_vmin =', cmap_vmin)
            # print('cmap_vmax =', cmap_vmax)

            def array_image_id_factory(level):
                return 'arr-%s/%s' % (array_id, level)

            tile_grid = self.get_tile_grid(ds_name, var_name, variable)

            pyramid = ImagePyramid.create_from_array(array, tile_grid,
                                                     level_image_id_factory=array_image_id_factory)
            pyramid = pyramid.apply(lambda image, level:
                                    TransformArrayImage(image,
                                                        image_id='tra-%s/%d' % (array_id, level),
                                                        flip_y=tile_grid.geo_extent.inv_y,
                                                        force_masked=True,
                                                        no_data_value=no_data_value,
                                                        valid_range=valid_range,
                                                        tile_cache=self.mem_tile_cache))
            pyramid = pyramid.apply(lambda image, level:
                                    ColorMappedRgbaImage(image,
                                                         image_id='rgb-%s/%d' % (image_id, level),
                                                         value_range=(cmap_vmin, cmap_vmax),
                                                         cmap_name=cmap_cbar,
                                                         encode=True,
                                                         format='PNG',
                                                         tile_cache=self.rgb_tile_cache))
            self.pyramid_cache[image_id] = pyramid
            if TRACE_PERF:
                print('Created pyramid "%s":' % image_id)
                print('  tile_size:', pyramid.tile_size)
                print('  num_level_zero_tiles:', pyramid.num_level_zero_tiles)
                print('  num_levels:', pyramid.num_levels)

        if TRACE_PERF:
            print('PERF: >>> Tile:', image_id, z, y, x)

        t1 = time.clock()
        tile = pyramid.get_tile(x, y, z)
        t2 = time.clock()

        if TRACE_PERF:
            print('PERF: <<< Tile:', image_id, z, y, x, 'took', t2 - t1, 'seconds')

        return tile

    def get_dataset_and_variable(self, ds_name: str, var_name: str):
        dataset = self.get_dataset(ds_name)
        if var_name in dataset:
            return dataset, dataset[var_name]
        raise ServiceRequestError(status_code=404, reason=f'Variable {var_name!r} not found in dataset {ds_name!r}')

    def get_dataset_tile_grid(self, ds_name: str, var_name: str, format_name: str, base_url: str) -> Dict[str, Any]:
        dataset, variable = self.get_dataset_and_variable(ds_name, var_name)
        tile_grid = self.get_tile_grid(ds_name, var_name, variable)
        if format_name == 'ol4.json':
            return _tile_grid_to_ol4_xyz_source_options(
                base_url + f'/xcts/{ds_name}/{var_name}/tile' + '/{z}/{x}/{y}.png', tile_grid)
        else:
            raise ServiceRequestError(status_code=404, reason=f'Unknown tile schema format {format_name!r}')

    # noinspection PyMethodMayBeStatic
    def get_tile_grid(self, ds_name: str, var_name: str, var: xr.DataArray):
        tile_grid = compute_tile_grid(var)
        if tile_grid is None:
            raise ServiceError(reason=f'Failed computing tile grid for variable {var_name!r} of dataset {ds_name!r}')
        return tile_grid

    # TODO: make this a generator using self.thread_pool
    def get_ne2_tile(self, x: int, y: int, z: int):
        return NaturalEarth2Image.get_pyramid().get_tile(x, y, z)

    # noinspection PyMethodMayBeStatic
    def get_ne2_tile_grid(self, format_name: str, base_url: str):
        if format_name == 'ol4.json':
            return _tile_grid_to_ol4_xyz_source_options(base_url + '/xcts/ne2/tile/{z}/{x}/{y}.jpg',
                                                        NaturalEarth2Image.get_pyramid().tile_grid)
        else:
            raise ServiceRequestError(status_code=404, reason=f'Unknown tile schema format {format_name!r}')

    def get_dataset_descriptor(self, ds_name: str):
        datasets = self.config.get('datasets')
        if not datasets:
            raise ServiceConfigError(reason=f"No datasets configured")
        if ds_name not in datasets:
            raise ServiceRequestError(status_code=404, reason=f"Dataset {ds_name!r} not found")
        return datasets[ds_name]

    def get_color_mapping(self, ds_name: str, var_name: str):
        dataset_descriptor = self.get_dataset_descriptor(ds_name)
        color_profile_name = dataset_descriptor.get('color_profile', 'default')
        color_profiles = self.config.get('color_profiles')
        if color_profiles:
            color_profile = color_profiles.get(color_profile_name)
            if color_profile:
                color_mapping = color_profile.get(var_name)
                if color_mapping:
                    cmap_cbar = color_mapping.get('cbar', DEFAULT_CMAP_CBAR)
                    cmap_vmin, cmap_vmax = color_mapping.get('vrange', (DEFAULT_CMAP_VMIN, DEFAULT_CMAP_VMAX))
                    return cmap_cbar, cmap_vmin, cmap_vmax
        _LOG.warning(f'color mapping for variable {var_name!r} of dataset {ds_name!r} undefined: using defaults')
        return DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, DEFAULT_CMAP_VMAX

    def get_dataset(self, ds_name: str):
        if ds_name in self.dataset_cache:
            ds, _ = self.dataset_cache[ds_name]
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
                if not os.path.isabs(path):
                    path = os.path.join(self.base_dir, path)
                data_format = dataset_descriptor.get('format', 'nc')
                if data_format == 'nc':
                    ds = xr.open_dataset(path)
                elif data_format == 'zarr':
                    ds = xr.open_zarr(path)
                else:
                    raise ServiceConfigError(reason=f"invalid format={data_format!r} in dataset descriptor {ds_name!r}")
            else:
                raise ServiceConfigError(reason=f"invalid fs={fs_type!r} in dataset descriptor {ds_name!r}")

            self.dataset_cache[ds_name] = ds, dataset_descriptor

            t2 = time.clock()

            if TRACE_PERF:
                print(f'PERF: opening {ds_name!r} took {t2-t1} seconds')

        return ds


def _tile_grid_to_ol4_xyz_source_options(url: str, tile_grid: TileGrid):
    """
    Convert TileGrid into options to be used with ol.source.XYZ(options) of OpenLayers 4.x.

    See

    * https://openlayers.org/en/latest/apidoc/ol.source.XYZ.html
    * https://openlayers.org/en/latest/examples/xyz.html

    :param tile_grid: tile grid
    :param url: source url
    :return:
    """
    ge = tile_grid.geo_extent
    res0 = (ge.north - ge.south) / tile_grid.height(0)
    #   https://openlayers.org/en/latest/examples/xyz.html
    #   https://openlayers.org/en/latest/apidoc/ol.source.XYZ.html
    return dict(url=url,
                projection='EPSG:4326',
                minZoom=0,
                maxZoom=tile_grid.num_levels - 1,
                tileGrid=dict(extent=[ge.west, ge.south, ge.east, ge.north],
                              origin=[ge.west, ge.south if ge.inv_y else ge.north],
                              tileSize=[tile_grid.tile_size[0], tile_grid.tile_size[1]],
                              resolutions=[res0 / (2 ** i) for i in range(tile_grid.num_levels)]))
