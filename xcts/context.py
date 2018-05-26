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

from xcts.im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage, TilingScheme
from xcts.im.ds import NaturalEarth2Image
from . import __version__
from .cache import MemoryCacheStore, Cache, FileCacheStore
from .defaults import DEFAULT_MAX_THREAD_COUNT, DEFAULT_CBAR, DEFAULT_VMIN, \
    DEFAULT_VMAX, TRACE_PERF, MEM_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_PATH, \
    FILE_TILE_CACHE_ENABLED
from .errors import ServiceConfigError, ServiceError, ServiceRequestError
from .tile import get_tiling_scheme

_LOG = logging.getLogger('xcts')

Config = Dict[str, Any]


class ServiceContext:

    def __init__(self):
        self._config = dict()
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
                         var_index: Optional[Sequence[int]],
                         cmap_name: Optional[str],
                         cmap_min: Optional[float],
                         cmap_max: Optional[float]):

        if cmap_name is None or cmap_min is None or cmap_max is None:
            default_cmap_name, default_cmap_min, default_cmap_max = self.get_color_mapping(ds_name, var_name)
            cmap_name = cmap_name or default_cmap_name
            cmap_min = cmap_min or default_cmap_min
            cmap_max = cmap_max or default_cmap_max

        array_id = '%s-%s-%s' % (ds_name,
                                 var_name,
                                 ','.join(map(str, var_index)))
        image_id = '%s-%s-%s-%s' % (array_id,
                                    cmap_name,
                                    cmap_min,
                                    cmap_max)

        pyramid_id = 'impy-%s' % image_id

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
                raise ServiceError(reason='Variable must be an N-D Dataset with N >= 2, '
                                          f'but {var_name!r} is only {variable.ndim}-D')

            cmap_min = np.nanmin(array.values) if np.isnan(cmap_min) else cmap_min
            cmap_max = np.nanmax(array.values) if np.isnan(cmap_max) else cmap_max

            # print('cmap_min =', cmap_min)
            # print('cmap_max =', cmap_max)

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
                                                        tile_cache=self.mem_tile_cache))
            pyramid = pyramid.apply(lambda image, level:
                                    ColorMappedRgbaImage(image,
                                                         image_id='rgb-%s/%d' % (image_id, level),
                                                         value_range=(cmap_min, cmap_max),
                                                         cmap_name=cmap_name,
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
        raise ServiceRequestError(reason=f'variable {var_name!r} does not exist in dataset {ds_name!r}')

    def get_dataset_tile_schema(self, ds_name: str, var_name: str, format_name: str) -> Dict[str, Any]:
        dataset, variable = self.get_dataset_and_variable(ds_name, var_name)

        try:
            ts = get_tiling_scheme(variable)
        except ValueError as e:
            raise ServiceError(reason=f'Failed computing tiling scheme for variable {var_name!r}: {e}')

        if format_name == 'ol4':
            return _to_ol4_xyz_options(ts)
        else:
            raise ServiceRequestError(reason=f'Unknown tile schema format {format_name!r}')

    # TODO: make this a generator using self.thread_pool
    def get_ne2_tile(self, x: int, y: int, z: int):
        return NaturalEarth2Image.get_pyramid().get_tile(x, y, z)

    # noinspection PyMethodMayBeStatic
    def get_ne2_tile_schema(self, format_name: str):
        if format_name == 'ol4':
            return _to_ol4_xyz_options(NaturalEarth2Image.get_pyramid().tiling_scheme)
        else:
            raise ServiceRequestError(reason=f'Unknown tile schema format {format_name!r}')

    def get_dataset_descriptor(self, ds_name: str):
        datasets = self.config.get('datasets')
        if not datasets:
            raise ServiceConfigError(reason=f"missing datasets in configuration")
        if ds_name not in datasets:
            raise ServiceConfigError(reason=f"unknown dataset {ds_name!r}")
        return datasets[ds_name]

    def get_color_mapping(self, ds_name: str, var_name: str):
        dataset_descriptor = self.get_dataset_descriptor(ds_name)
        color_profile_name = dataset_descriptor.get('color_profile', 'default')
        color_profiles = dataset_descriptor.get('color_profiles')
        if color_profiles:
            color_profile = color_profiles.get(color_profile_name)
            if color_profile:
                color_mapping = color_profile.get(var_name)
                if color_mapping:
                    cmap = color_mapping.get('cmap', DEFAULT_CBAR)
                    vmin, vmax = color_mapping.get('vrange', (DEFAULT_VMIN, DEFAULT_VMAX))
                    return cmap, vmin, vmax
        _LOG.warning(f'color mapping for variable {var_name!r} of dataset {ds_name!r} undefined: using defaults')
        return DEFAULT_CBAR, DEFAULT_VMIN, DEFAULT_VMAX

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


def _to_ol4_xyz_options(ts: TilingScheme):
    ge = ts.geo_extent
    res0 = (ge.north - ge.south) / ts.height(0)
    # TODO: add url option, see
    #   https://openlayers.org/en/latest/examples/xyz.html
    #   https://openlayers.org/en/latest/apidoc/ol.source.XYZ.html
    return dict(projection='EPSG:4326',
                minZoom=0,
                maxZoom=ts.num_levels - 1,
                tileGrid=dict(extent=[ge.west, ge.south, ge.east, ge.north],
                              origin=[ge.west, ge.south if ge.inv_y else ge.north],
                              tileSize=[ts.tile_size[0], ts.tile_size[1]],
                              resolutions=[res0 / (2 ** i) for i in range(ts.num_levels)]))
