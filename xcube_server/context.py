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

import glob
import logging
import os
import time
from typing import Any, Dict, List, Optional

import fiona
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr

from . import __version__
from .cache import MemoryCacheStore, Cache, FileCacheStore
from .defaults import DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, \
    DEFAULT_CMAP_VMAX, TRACE_PERF, MEM_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_PATH, \
    FILE_TILE_CACHE_ENABLED, API_PREFIX, DEFAULT_NAME
from .errors import ServiceConfigError, ServiceError, ServiceBadRequestError, ServiceResourceNotFoundError
from .logtime import log_time
from .reqparams import RequestParams

COMPUTE_DATASET = 'compute_dataset'
ALL_FEATURES = "all"

_LOG = logging.getLogger('xcube')

Config = Dict[str, Any]


# noinspection PyMethodMayBeStatic
class ServiceContext:

    def __init__(self,
                 name: str = DEFAULT_NAME,
                 base_dir: str = None,
                 config: Config = None):
        self._name = name
        self.base_dir = os.path.abspath(base_dir or '')
        self._config = config or dict()
        self.dataset_cache = dict()  # contains tuples of form (ds, ds_descriptor, tile_grid_cache)
        # TODO by forman: move pyramid_cache, mem_tile_cache, rgb_tile_cache into dataset_cache values
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
        self._feature_collection_cache = dict()

    @property
    def config(self) -> Config:
        return self._config

    @config.setter
    def config(self, config: Config):
        if self._config:
            old_dataset_descriptors = self._config.get('Datasets')
            new_dataset_descriptors = config.get('Datasets')
            if not new_dataset_descriptors:
                for ds, _, _ in self.dataset_cache.values():
                    ds.close()
                self.dataset_cache.clear()
            if new_dataset_descriptors and old_dataset_descriptors:
                ds_names = list(self.dataset_cache.keys())
                for ds_name in ds_names:
                    dataset_descriptor = self.find_dataset_descriptor(new_dataset_descriptors, ds_name)
                    if dataset_descriptor is None:
                        ds, _, _ = self.dataset_cache[ds_name]
                        ds.close()
                        del self.dataset_cache[ds_name]
        self._config = config

    def get_service_url(self, base_url, *path: str):
        return base_url + '/' + self._name + API_PREFIX + '/' + '/'.join(path)

    def get_dataset_and_variable(self, ds_name: str, var_name: str):
        dataset = self.get_dataset(ds_name)
        if var_name in dataset:
            return dataset, dataset[var_name]
        raise ServiceResourceNotFoundError(f'Variable "{var_name}" not found in dataset "{ds_name}"')

    def get_dataset_descriptors(self):
        dataset_descriptors = self.config.get('Datasets')
        if not dataset_descriptors:
            raise ServiceConfigError(f"No datasets configured")
        return dataset_descriptors

    def get_dataset_descriptor(self, ds_name: str) -> Dict[str, str]:
        dataset_descriptors = self.get_dataset_descriptors()
        if not dataset_descriptors:
            raise ServiceConfigError(f"No datasets configured")
        dataset_descriptor = self.find_dataset_descriptor(dataset_descriptors, ds_name)
        if dataset_descriptor is None:
            raise ServiceResourceNotFoundError(f'Dataset "{ds_name}" not found')
        return dataset_descriptor

    def get_color_mapping(self, ds_name: str, var_name: str):
        dataset_descriptor = self.get_dataset_descriptor(ds_name)
        style_name = dataset_descriptor.get('Style', 'default')
        styles = self.config.get('Styles')
        if styles:
            style = None
            for s in styles:
                if style_name == s['Identifier']:
                    style = s
            # TODO: check color_mappings is not None
            if style:
                color_mappings = style.get('ColorMappings')
                if color_mappings:
                    # TODO: check color_mappings is not None
                    color_mapping = color_mappings.get(var_name)
                    if color_mapping:
                        cmap_cbar = color_mapping.get('ColorBar', DEFAULT_CMAP_CBAR)
                        cmap_vmin, cmap_vmax = color_mapping.get('ValueRange', (DEFAULT_CMAP_VMIN, DEFAULT_CMAP_VMAX))
                        return cmap_cbar, cmap_vmin, cmap_vmax
        _LOG.warning(f'color mapping for variable {var_name!r} of dataset {ds_name!r} undefined: using defaults')
        return DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, DEFAULT_CMAP_VMAX

    def get_dataset(self, ds_name: str) -> xr.Dataset:
        if ds_name in self.dataset_cache:
            ds, _, _ = self.dataset_cache[ds_name]
        else:
            dataset_descriptor = self.get_dataset_descriptor(ds_name)

            path = dataset_descriptor.get('Path')
            if not path:
                raise ServiceConfigError(f"Missing 'path' entry in dataset descriptor {ds_name}")

            t1 = time.clock()

            fs_type = dataset_descriptor.get('FileSystem', 'local')
            if fs_type == 'obs':
                data_format = dataset_descriptor.get('Format', 'zarr')
                if data_format != 'zarr':
                    raise ServiceConfigError(f"Invalid format={data_format!r} in dataset descriptor {ds_name!r}")
                client_kwargs = {}
                if 'Endpoint' in dataset_descriptor:
                    client_kwargs['endpoint_url'] = dataset_descriptor['Endpoint']
                if 'Region' in dataset_descriptor:
                    client_kwargs['region_name'] = dataset_descriptor['Region']
                s3 = s3fs.S3FileSystem(anon=True, client_kwargs=client_kwargs)
                store = s3fs.S3Map(root=path, s3=s3, check=False)
                cached_store = zarr.LRUStoreCache(store, max_size=2 ** 28)
                with log_time(f"opened remote dataset {path}"):
                    ds = xr.open_zarr(cached_store)
            elif fs_type == 'local':
                if not os.path.isabs(path):
                    path = os.path.join(self.base_dir, path)
                data_format = dataset_descriptor.get('Format', 'nc')
                if data_format == 'nc':
                    with log_time(f"opened local NetCDF dataset {path}"):
                        ds = xr.open_dataset(path)
                elif data_format == 'zarr':
                    with log_time(f"opened local zarr dataset {path}"):
                        ds = xr.open_zarr(path)
                else:
                    raise ServiceConfigError(f"Invalid format={data_format!r} in dataset descriptor {ds_name!r}")
            elif fs_type == 'computed':
                if not os.path.isabs(path):
                    path = os.path.join(self.base_dir, path)
                with open(path) as fp:
                    python_code = fp.read()

                local_env = dict()
                global_env = None
                try:
                    exec(python_code, global_env, local_env)
                except Exception as e:
                    raise ServiceError(f"Failed to compute dataset {ds_name!r} from {path!r}: {e}") from e

                callable_name = dataset_descriptor.get('Function', COMPUTE_DATASET)
                callable_args = dataset_descriptor.get('Args', [])

                callable_obj = local_env.get(callable_name)
                if callable_obj is None:
                    raise ServiceConfigError(f"Invalid dataset descriptor {ds_name!r}: "
                                             f"no callable named {callable_name!r} found in {path!r}")
                elif not callable(callable_obj):
                    raise ServiceConfigError(f"Invalid dataset descriptor {ds_name!r}: "
                                             f"object {callable_name!r} in {path!r} is not callable")

                args = list()
                for arg_value in callable_args:
                    if isinstance(arg_value, str) and len(arg_value) > 2 \
                            and arg_value.startswith('@') and arg_value.endswith('@'):
                        ref_ds_name = arg_value[1:-1]
                        if not self.get_dataset_descriptor(ref_ds_name):
                            raise ServiceConfigError(f"Invalid dataset descriptor {ds_name!r}: "
                                                     f"argument {arg_value!r} of callable {callable_name!r} "
                                                     f"must reference another dataset")
                        args.append(self.get_dataset(ref_ds_name))
                    else:
                        args.append(arg_value)

                try:
                    with log_time(f"created computed dataset {ds_name}"):
                        ds = callable_obj(*args)
                except Exception as e:
                    raise ServiceError(f"Failed to compute dataset {ds_name!r} "
                                       f"from function {callable_name!r} in {path!r}: {e}") from e
                if not isinstance(ds, xr.Dataset):
                    raise ServiceError(f"Failed to compute dataset {ds_name!r} "
                                       f"from function {callable_name!r} in {path!r}: "
                                       f"expected an xarray.Dataset but got a {type(ds)}")
            else:
                raise ServiceConfigError(f"Invalid fs={fs_type!r} in dataset descriptor {ds_name!r}")

            tile_grid_cache = dict()
            self.dataset_cache[ds_name] = ds, dataset_descriptor, tile_grid_cache

            t2 = time.clock()

            if TRACE_PERF:
                print(f'PERF: opening {ds_name!r} took {t2-t1} seconds')

        return ds

    def get_feature_collections(self) -> List[Dict]:
        features_configs = self._config.get("Features", [])
        feature_collections = []
        for features_config in features_configs:
            feature_collections.append(dict(id=features_config.get("Identifier"),
                                            title=features_config.get("Title")))
        return feature_collections

    def get_feature_collection(self, collection_name: str = ALL_FEATURES) -> Dict:
        if ALL_FEATURES not in self._feature_collection_cache:
            features_configs = self._config.get("Features", [])
            all_features = []
            feature_index = 0
            for features_config in features_configs:
                curr_collection_name = features_config.get("Identifier")
                if not curr_collection_name:
                    raise ServiceError("Missing 'Identifier' entry in 'Features'")
                if curr_collection_name == ALL_FEATURES:
                    raise ServiceError("Invalid 'Identifier' entry in 'Features'")
                curr_collection_wc = features_config.get("Path")
                if not curr_collection_wc:
                    raise ServiceError("Missing 'Path' entry in 'Features'")
                if not os.path.isabs(curr_collection_wc):
                    curr_collection_wc = os.path.join(self.base_dir, curr_collection_wc)

                features = []
                collection_files = glob.glob(curr_collection_wc)
                for collection_file in collection_files:
                    with fiona.open(collection_file) as feature_collection:
                        for feature in feature_collection:
                            self._remove_feature_id(feature)
                            feature["id"] = str(feature_index)
                            feature_index += 1
                            features.append(feature)
                self._feature_collection_cache[curr_collection_name] = dict(type="FeatureCollection",
                                                                            features=features)
                all_features.extend(features)

            self._feature_collection_cache[ALL_FEATURES] = dict(type="FeatureCollection",
                                                                features=all_features)

        if collection_name not in self._feature_collection_cache:
            raise ServiceResourceNotFoundError(f'Feature collection "{collection_name}" not found')
        return self._feature_collection_cache[collection_name]

    @classmethod
    def _remove_feature_id(cls, feature: Dict):
        cls._remove_id(feature)
        if "properties" in feature:
            cls._remove_id(feature["properties"])

    @classmethod
    def _remove_id(cls, properties: Dict):
        if "id" in properties:
            del properties["id"]
        if "ID" in properties:
            del properties["ID"]

    def get_dataset_and_coord_variable(self, ds_name: str, dim_name: str):
        ds = self.get_dataset(ds_name)
        if dim_name not in ds.coords:
            raise ServiceResourceNotFoundError(f'Dimension {dim_name!r} has no coordinates in dataset {ds_name!r}')
        return ds, ds.coords[dim_name]

    @classmethod
    def get_var_indexers(cls,
                         ds_name: str,
                         var_name: str,
                         var: xr.DataArray,
                         dim_names: List[str],
                         params: RequestParams) -> Dict[str, Any]:
        var_indexers = dict()
        for dim_name in dim_names:
            if dim_name not in var.coords:
                raise ServiceBadRequestError(
                    f'dimension {dim_name!r} of variable {var_name!r} of dataset {ds_name!r} has no coordinates')
            coord_var = var.coords[dim_name]
            dim_value_str = params.get_query_argument(dim_name, None)
            try:
                if dim_value_str is None:
                    var_indexers[dim_name] = coord_var.values[0]
                elif dim_value_str == 'current':
                    var_indexers[dim_name] = coord_var.values[-1]
                elif np.issubdtype(coord_var.dtype, np.floating):
                    var_indexers[dim_name] = float(dim_value_str)
                elif np.issubdtype(coord_var.dtype, np.integer):
                    var_indexers[dim_name] = int(dim_value_str)
                elif np.issubdtype(coord_var.dtype, np.datetime64):
                    var_indexers[dim_name] = pd.to_datetime(dim_value_str)
                else:
                    raise ValueError(f'unable to dimension value {dim_value_str!r} to {coord_var.dtype!r}')
            except ValueError as e:
                raise ServiceBadRequestError(
                    f'{dim_value_str!r} is not a valid value for dimension {dim_name!r} '
                    f'of variable {var_name!r} of dataset {ds_name!r}') from e
        return var_indexers

    @classmethod
    def find_dataset_descriptor(cls,
                                dataset_descriptors: List[Dict[str, Any]],
                                ds_name: str) -> Optional[Dict[str, Any]]:
        # TODO: optimize by dict/key lookup
        return next((dsd for dsd in dataset_descriptors if dsd['Identifier'] == ds_name), None)
