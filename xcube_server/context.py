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
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import fiona
import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr

from xcube_server.im import TileGrid
from . import __version__
from .cache import MemoryCacheStore, Cache, FileCacheStore
from .defaults import DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, \
    DEFAULT_CMAP_VMAX, FILE_TILE_CACHE_PATH, \
    API_PREFIX, DEFAULT_NAME, DEFAULT_TRACE_PERF
from .errors import ServiceConfigError, ServiceError, ServiceBadRequestError, ServiceResourceNotFoundError
from .mldataset import FileStorageMultiLevelDataset, BaseMultiLevelDataset, MultiLevelDataset, \
    ComputedMultiLevelDataset, ObjectStorageMultiLevelDataset
from .perf import measure_time
from .reqparams import RequestParams

COMPUTE_DATASET = 'compute_dataset'
ALL_PLACES = "all"

_LOG = logging.getLogger('xcube')

Config = Dict[str, Any]


# noinspection PyMethodMayBeStatic
class ServiceContext:

    def __init__(self,
                 name: str = DEFAULT_NAME,
                 base_dir: str = None,
                 config: Config = None,
                 trace_perf: bool = DEFAULT_TRACE_PERF,
                 tile_comp_mode: int = None,
                 mem_tile_cache_capacity: int = None,
                 file_tile_cache_capacity: int = None):
        self._name = name
        self.base_dir = os.path.abspath(base_dir or '')
        self._config = config if config is not None else dict()
        self._place_group_cache = dict()
        self._feature_index = 0
        self._tile_comp_mode = tile_comp_mode
        self._trace_perf = trace_perf
        self._lock = threading.RLock()

        self.dataset_cache = dict()  # contains tuples of form (MultiLevelDataset, ds_descriptor)
        # TODO by forman: move pyramid_cache, mem_tile_cache, rgb_tile_cache into dataset_cache values
        self.image_cache = dict()

        if mem_tile_cache_capacity and mem_tile_cache_capacity > 0:
            self.mem_tile_cache = Cache(MemoryCacheStore(),
                                        capacity=mem_tile_cache_capacity,
                                        threshold=0.75)
        else:
            self.mem_tile_cache = None

        if file_tile_cache_capacity and file_tile_cache_capacity > 0:
            tile_cache_dir = os.path.join(FILE_TILE_CACHE_PATH, 'v%s' % __version__, 'tiles')
            self.rgb_tile_cache = Cache(FileCacheStore(tile_cache_dir, ".png"),
                                        capacity=file_tile_cache_capacity,
                                        threshold=0.75)
        else:
            self.rgb_tile_cache = None

    @property
    def config(self) -> Config:
        return self._config

    @config.setter
    def config(self, config: Config):
        if self._config:
            old_dataset_descriptors = self._config.get('Datasets')
            new_dataset_descriptors = config.get('Datasets')

            clean_image_caches = False

            if not new_dataset_descriptors:
                for ml_dataset, _ in self.dataset_cache.values():
                    ml_dataset.close()
                self.dataset_cache.clear()

            if new_dataset_descriptors and old_dataset_descriptors:
                ds_names = list(self.dataset_cache.keys())
                for ds_name in ds_names:
                    dataset_descriptor = self.find_dataset_descriptor(new_dataset_descriptors, ds_name)
                    if dataset_descriptor is None:
                        ml_dataset, _ = self.dataset_cache[ds_name]
                        ml_dataset.close()
                        del self.dataset_cache[ds_name]

            if clean_image_caches:
                self.image_cache.clear()
                if self.rgb_tile_cache is not None:
                    self.rgb_tile_cache.clear()

        self._config = config

    @property
    def tile_comp_mode(self) -> int:
        return self._tile_comp_mode

    @property
    def trace_perf(self) -> bool:
        return self._trace_perf

    def get_service_url(self, base_url, *path: str):
        return base_url + '/' + self._name + API_PREFIX + '/' + '/'.join(path)

    def get_ml_dataset(self, ds_id: str) -> MultiLevelDataset:
        ml_dataset, _ = self._get_dataset_entry(ds_id)
        return ml_dataset

    def get_dataset(self, ds_id: str) -> xr.Dataset:
        ml_dataset, _ = self._get_dataset_entry(ds_id)
        return ml_dataset.base_dataset

    def get_dataset_and_variable(self, ds_id: str, var_name: str) -> Tuple[xr.Dataset, xr.DataArray]:
        dataset = self.get_dataset(ds_id)
        if var_name in dataset:
            return dataset, dataset[var_name]
        raise ServiceResourceNotFoundError(f'Variable "{var_name}" not found in dataset "{ds_id}"')

    def get_variable_for_z(self, ds_id: str, var_name: str, z_index: int) -> xr.DataArray:
        ml_dataset = self.get_ml_dataset(ds_id)
        dataset = ml_dataset.get_dataset(ml_dataset.num_levels - 1 - z_index)
        if var_name not in dataset:
            raise ServiceResourceNotFoundError(f'Variable "{var_name}" not found in dataset "{ds_id}"')
        return dataset[var_name]

    def get_dataset_descriptors(self):
        dataset_descriptors = self.config.get('Datasets')
        if not dataset_descriptors:
            raise ServiceConfigError(f"No datasets configured")
        return dataset_descriptors

    def get_dataset_descriptor(self, ds_id: str) -> Dict[str, Any]:
        dataset_descriptors = self.get_dataset_descriptors()
        if not dataset_descriptors:
            raise ServiceConfigError(f"No datasets configured")
        dataset_descriptor = self.find_dataset_descriptor(dataset_descriptors, ds_id)
        if dataset_descriptor is None:
            raise ServiceResourceNotFoundError(f'Dataset "{ds_id}" not found')
        return dataset_descriptor

    def get_tile_grid(self, ds_id: str) -> TileGrid:
        ml_dataset, _ = self._get_dataset_entry(ds_id)
        return ml_dataset.tile_grid

    def get_color_mapping(self, ds_id: str, var_name: str):
        dataset_descriptor = self.get_dataset_descriptor(ds_id)
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
        _LOG.warning(f'color mapping for variable {var_name!r} of dataset {ds_id!r} undefined: using defaults')
        return DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, DEFAULT_CMAP_VMAX

    def _get_dataset_entry(self, ds_id: str) -> Tuple[MultiLevelDataset, Dict[str, Any]]:
        if ds_id not in self.dataset_cache:
            with self._lock:
                self.dataset_cache[ds_id] = self._create_dataset_entry(ds_id)
        return self.dataset_cache[ds_id]

    def _create_dataset_entry(self, ds_id: str) -> Tuple[MultiLevelDataset, Dict[str, Any]]:

        dataset_descriptor = self.get_dataset_descriptor(ds_id)

        path = dataset_descriptor.get('Path')
        if not path:
            raise ServiceConfigError(f"Missing 'path' entry in dataset descriptor {ds_id}")

        t1 = time.perf_counter()

        fs_type = dataset_descriptor.get('FileSystem', 'local')
        if fs_type == 'obs':
            data_format = dataset_descriptor.get('Format', 'zarr')
            s3_client_kwargs = {}
            if 'Endpoint' in dataset_descriptor:
                s3_client_kwargs['endpoint_url'] = dataset_descriptor['Endpoint']
            if 'Region' in dataset_descriptor:
                s3_client_kwargs['region_name'] = dataset_descriptor['Region']
            obs_file_system = s3fs.S3FileSystem(anon=True, client_kwargs=s3_client_kwargs)
            if data_format == 'zarr':
                store = s3fs.S3Map(root=path, s3=obs_file_system, check=False)
                cached_store = zarr.LRUStoreCache(store, max_size=2 ** 28)
                with measure_time(tag=f"opened remote zarr dataset {path}"):
                    ds = xr.open_zarr(cached_store)
                ml_dataset = BaseMultiLevelDataset(ds)
            elif data_format == 'levels':
                with measure_time(tag=f"opened remote levels dataset {path}"):
                    ml_dataset = ObjectStorageMultiLevelDataset(ds_id, obs_file_system, path,
                                                                exception_type=ServiceConfigError)
            else:
                raise ServiceConfigError(f"Invalid format={data_format!r} in dataset descriptor {ds_id!r}")
        elif fs_type == 'local':
            if not os.path.isabs(path):
                path = os.path.join(self.base_dir, path)

            data_format = dataset_descriptor.get('Format', 'nc')
            if data_format == 'nc':
                with measure_time(tag=f"opened local NetCDF dataset {path}"):
                    ds = xr.open_dataset(path)
                    ml_dataset = BaseMultiLevelDataset(ds)
            elif data_format == 'zarr':
                with measure_time(tag=f"opened local zarr dataset {path}"):
                    ds = xr.open_zarr(path)
                    ml_dataset = BaseMultiLevelDataset(ds)
            elif data_format == 'levels':
                with measure_time(tag=f"opened local levels dataset {path}"):
                    ml_dataset = FileStorageMultiLevelDataset(path)
            else:
                raise ServiceConfigError(f"Invalid format={data_format!r} in dataset descriptor {ds_id!r}")
        elif fs_type == 'memory':
            if not os.path.isabs(path):
                path = os.path.join(self.base_dir, path)

            callable_name = dataset_descriptor.get('Function', COMPUTE_DATASET)
            input_dataset_ids = dataset_descriptor.get('InputDatasets', [])
            input_parameters = dataset_descriptor.get('InputParameters', {})

            for input_dataset_id in input_dataset_ids:
                if not self.get_dataset_descriptor(input_dataset_id):
                    raise ServiceConfigError(f"Invalid dataset descriptor {ds_id!r}: "
                                             f"Input dataset {input_dataset_id!r} of callable {callable_name!r} "
                                             f"must reference another dataset")

            with measure_time(tag=f"opened memory dataset {path}"):
                ml_dataset = ComputedMultiLevelDataset(ds_id,
                                                       path,
                                                       callable_name,
                                                       input_dataset_ids,
                                                       self.get_ml_dataset,
                                                       input_parameters,
                                                       exception_type=ServiceConfigError)

        else:
            raise ServiceConfigError(f"Invalid fs={fs_type!r} in dataset descriptor {ds_id!r}")

        t2 = time.perf_counter()

        if self.config.get("trace_perf", False):
            _LOG.info(f'Opening {ds_id!r} took {t2 - t1} seconds')

        return ml_dataset, dataset_descriptor

    def get_legend_label(self, ds_name: str, var_name: str):
        dataset = self.get_dataset(ds_name)
        if var_name in dataset:
            ds = self.get_dataset(ds_name)
            units = ds[var_name].units
            return units
        raise ServiceResourceNotFoundError(f'Variable "{var_name}" not found in dataset "{ds_name}"')

    def get_place_groups(self) -> List[Dict]:
        place_group_configs = self._config.get("PlaceGroups", [])
        place_groups = []
        for features_config in place_group_configs:
            place_groups.append(dict(id=features_config.get("Identifier"),
                                     title=features_config.get("Title")))
        return place_groups

    def get_dataset_place_groups(self, ds_id: str) -> List[Dict]:
        dataset_descriptor = self.get_dataset_descriptor(ds_id)
        place_group_configs = dataset_descriptor.get("PlaceGroups")
        if not place_group_configs:
            return []

        place_group_id_prefix = f"DS-{ds_id}-"

        place_groups = []
        for k, v in self._place_group_cache.items():
            if k.startswith(place_group_id_prefix):
                place_groups.append(v)
        if place_groups:
            return place_groups

        place_groups = self._load_place_groups(place_group_configs)
        for place_group in place_groups:
            self._place_group_cache[place_group_id_prefix + place_group["id"]] = place_group

        return place_groups

    def get_place_group(self, place_group_id: str = ALL_PLACES) -> Dict:
        if ALL_PLACES not in self._place_group_cache:
            place_group_configs = self._config.get("PlaceGroups", [])
            place_groups = self._load_place_groups(place_group_configs)

            all_features = []
            for place_group in place_groups:
                all_features.extend(place_group["features"])
                self._place_group_cache[place_group["id"]] = place_group

            self._place_group_cache[ALL_PLACES] = dict(type="FeatureCollection", features=all_features)

        if place_group_id not in self._place_group_cache:
            raise ServiceResourceNotFoundError(f'Place group "{place_group_id}" not found')

        return self._place_group_cache[place_group_id]

    def _load_place_groups(self, place_group_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        place_groups = []
        for place_group_config in place_group_configs:
            place_group = self._load_place_group(place_group_config)
            place_groups.append(place_group)
        return place_groups

    def _load_place_group(self, place_group_config: Dict[str, Any]) -> Dict[str, Any]:
        ref_id = place_group_config.get("PlaceGroupRef")
        if ref_id:
            # Trigger loading of all global "PlaceGroup" entries
            self.get_place_group()
            if len(place_group_config) > 1:
                raise ServiceError("'PlaceGroupRef' if present, must be the only entry in a 'PlaceGroups' item")
            if ref_id not in self._place_group_cache:
                raise ServiceError("Invalid 'PlaceGroupRef' entry in a 'PlaceGroups' item")
            return self._place_group_cache[ref_id]

        place_group_id = place_group_config.get("Identifier")
        if not place_group_id:
            raise ServiceError("Missing 'Identifier' entry in a 'PlaceGroups' item")
        if place_group_id == ALL_PLACES:
            raise ServiceError("Invalid 'Identifier' entry in a 'PlaceGroups' item")

        place_group_title = place_group_config.get("Title", place_group_id)

        place_path_wc = place_group_config.get("Path")
        if not place_path_wc:
            raise ServiceError("Missing 'Path' entry in a 'PlaceGroups' item")
        if not os.path.isabs(place_path_wc):
            place_path_wc = os.path.join(self.base_dir, place_path_wc)

        property_mapping = place_group_config.get("PropertyMapping")
        character_encoding = place_group_config.get("CharacterEncoding", "utf-8")

        features = []
        collection_files = glob.glob(place_path_wc)
        for collection_file in collection_files:
            with fiona.open(collection_file, encoding=character_encoding) as feature_collection:
                for feature in feature_collection:
                    self._remove_feature_id(feature)
                    feature["id"] = str(self._feature_index)
                    self._feature_index += 1
                    features.append(feature)

        place_group = dict(type="FeatureCollection",
                           features=features,
                           id=place_group_id,
                           title=place_group_title,
                           propertyMapping=property_mapping)

        sub_place_group_configs = place_group_config.get("Places")
        if sub_place_group_configs:
            sub_place_groups = self._load_place_groups(sub_place_group_configs)
            place_group["placeGroups"] = sub_place_groups

        return place_group

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
