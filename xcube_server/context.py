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
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import s3fs
import xarray as xr
import zarr

from . import __version__
from .cache import MemoryCacheStore, Cache, FileCacheStore
from .defaults import DEFAULT_MAX_THREAD_COUNT, DEFAULT_CMAP_CBAR, DEFAULT_CMAP_VMIN, \
    DEFAULT_CMAP_VMAX, TRACE_PERF, MEM_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_CAPACITY, FILE_TILE_CACHE_PATH, \
    FILE_TILE_CACHE_ENABLED
from .errors import ServiceConfigError, ServiceError, ServiceBadRequestError, ServiceResourceNotFoundError
from .im import ImagePyramid, TransformArrayImage, ColorMappedRgbaImage, TileGrid
from .ne2 import NaturalEarth2Image
from .reqparams import RequestParams
from .tile import compute_tile_grid

_LOG = logging.getLogger('xcube')

Config = Dict[str, Any]


class ServiceContext:

    def __init__(self, base_dir=None, config: Config = None):
        self.base_dir = os.path.abspath(base_dir or '')
        self._config = config or dict()
        self.dataset_cache = dict() # contains tuples of form (ds, ds_descriptor, tile_grid_cache)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=DEFAULT_MAX_THREAD_COUNT,
                                                                 thread_name_prefix='xcube_server')
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
                for ds, _, _ in self.dataset_cache.values():
                    ds.close()
                self.dataset_cache.clear()
            if new_dataset_descriptors and old_dataset_descriptors:
                for ds_name in self.dataset_cache.keys():
                    if ds_name not in new_dataset_descriptors:
                        ds, _ = old_dataset_descriptors[ds_name]
                        ds.close()
                        del self.dataset_cache[ds_name]
        self._config = config

    def get_wmts_capabilities(self, format_name: str, base_url: str):

        service_identification_xml = f"""<ows:ServiceIdentification>
        <ows:Title>xcube WMTS</ows:Title>
        <ows:Abstract>Web Map Tile Service (WMTS) for xcube-conformant data cubes</ows:Abstract>
        <ows:Keywords>
            <ows:Keyword>tile</ows:Keyword>
            <ows:Keyword>tile matrix set</ows:Keyword>
            <ows:Keyword>map</ows:Keyword>
        </ows:Keywords>
        <ows:ServiceType>OGC WMTS</ows:ServiceType>
        <ows:ServiceTypeVersion>1.0.0</ows:ServiceTypeVersion>
        <ows:Fees>none</ows:Fees>
        <ows:AccessConstraints>none</ows:AccessConstraints>
    </ows:ServiceIdentification>
"""

        service_provider = self.config['ServiceProvider']
        service_contact = service_provider['ServiceContact']
        contact_info = service_contact['ContactInfo']
        phone = contact_info['Phone']
        address = contact_info['Address']

        service_provider_xml = \
            f"""<ows:ServiceProvider>
        <ows:ProviderName>{service_provider['ProviderName']}</ows:ProviderName>
        <ows:ProviderSite xlink:href="{service_provider['ProviderSite']}"/>
        <ows:ServiceContact>
            <ows:IndividualName>{service_contact['IndividualName']}</ows:IndividualName>
            <ows:PositionName>{service_contact['PositionName']}</ows:PositionName>
            <ows:ContactInfo>
                <ows:Phone>
                    <ows:Voice>{phone['Voice']}</ows:Voice>
                    <ows:Facsimile>{phone['Facsimile']}</ows:Facsimile>
                </ows:Phone>
                <ows:Address>
                    <ows:DeliveryPoint>{address['DeliveryPoint']}</ows:DeliveryPoint>
                    <ows:City>{address['City']}</ows:City>
                    <ows:AdministrativeArea>{address['AdministrativeArea']}</ows:AdministrativeArea>
                    <ows:PostalCode>{address['PostalCode']}</ows:PostalCode>
                    <ows:Country>{address['Country']}</ows:Country>
                    <ows:ElectronicMailAddress>{address['ElectronicMailAddress']}</ows:ElectronicMailAddress>
                </ows:Address>
            </ows:ContactInfo>
        </ows:ServiceContact>
    </ows:ServiceProvider>
"""

        wmts_kvp_url = base_url + '/xcube/wmts/1.0.0/kvp?'

        operations_metadata_xml = f"""<ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{wmts_kvp_url}">
                        <ows:Constraint name="GetEncoding">
                            <ows:AllowedValues>
                                <ows:Value>KVP</ows:Value>
                            </ows:AllowedValues>
                        </ows:Constraint>
                    </ows:Get>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="GetTile">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{wmts_kvp_url}">
                        <ows:Constraint name="GetEncoding">
                            <ows:AllowedValues>
                                <ows:Value>KVP</ows:Value>
                            </ows:AllowedValues>
                        </ows:Constraint>
                    </ows:Get>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
    </ows:OperationsMetadata>
"""

        dataset_descriptors = self.get_dataset_descriptors()
        tile_grids = dict()
        indent = '    '

        layer_base_url = base_url + '/xcube/wmts/1.0.0/tile/%s/%s/{TileMatrix}/{TileCol}/{TileRow}.png'

        dimensions_xml_cache = dict()

        contents_xml_lines = [(0, '<Contents>')]
        for dataset_descriptor in dataset_descriptors:
            ds_name = dataset_descriptor['Identifier']
            ds = self.get_dataset(ds_name)
            for var_name in ds.data_vars:
                var = ds[var_name]
                if len(var.shape) <= 2 or var.dims[-1] != 'lon' or var.dims[-2] != 'lat':
                    continue

                tile_grid_id = 'TileGrid_%s_%s' % (var.shape[-1], var.shape[-2])
                write_tile_matrix_set = False
                if tile_grid_id in tile_grids:
                    tile_grid = tile_grids[tile_grid_id]
                else:
                    tile_grid = self.get_or_compute_tile_grid(ds_name, var)
                    if tile_grid is not None:
                        tile_grids[tile_grid_id] = tile_grid
                        write_tile_matrix_set = True

                if tile_grid is not None:
                    if write_tile_matrix_set:
                        contents_xml_lines.append((2, '<TileMatrixSet>'))
                        contents_xml_lines.append((3, f'<ows:Identifier>{tile_grid_id}</ows:Identifier>'))
                        contents_xml_lines.append((3, f'<ows:SupportedCRS>EPSG:4326</ows:SupportedCRS>'))

                        tile_size_x = tile_grid.tile_size[0]
                        tile_size_y = tile_grid.tile_size[1]
                        lon1 = tile_grid.geo_extent.west
                        lat1 = tile_grid.geo_extent.south
                        lon2 = tile_grid.geo_extent.east
                        lat2 = tile_grid.geo_extent.north
                        res0 = (lat2 - lat1) / (tile_size_y * tile_grid.num_level_zero_tiles_y)
                        for level in range(tile_grid.num_levels):
                            factor = 2 ** level
                            num_tiles_x = tile_grid.num_level_zero_tiles_x * factor
                            num_tiles_y = tile_grid.num_level_zero_tiles_y * factor
                            res = res0 / factor
                            contents_xml_lines.append((3, '<TileMatrix>'))
                            contents_xml_lines.append((4, f'<ows:Identifier>{level}</ows:Identifier>'))
                            contents_xml_lines.append((4, f'<ScaleDenominator>{res}</ScaleDenominator>'))
                            contents_xml_lines.append((4, f'<TopLeftCorner>{lon1} {lat2}</TopLeftCorner>'))
                            contents_xml_lines.append((4, f'<TileWidth>{tile_size_x}</TileWidth>'))
                            contents_xml_lines.append((4, f'<TileHeight>{tile_size_y}</TileHeight>'))
                            contents_xml_lines.append((4, f'<MatrixWidth>{num_tiles_x}</MatrixWidth>'))
                            contents_xml_lines.append((4, f'<MatrixHeight>{num_tiles_y}</MatrixHeight>'))
                            contents_xml_lines.append((3, '</TileMatrix>'))

                        contents_xml_lines.append((2, '</TileMatrixSet>'))

                    var_title = var.attrs.get('title', var.attrs.get('long_name', var_name))
                    var_abstract = var.attrs.get('comment', '')

                    layer_tile_url = layer_base_url % (ds_name, var_name)
                    contents_xml_lines.append((2, '<Layer>'))
                    contents_xml_lines.append((3, f'<ows:Identifier>{ds_name}.{var_name}</ows:Identifier>'))
                    contents_xml_lines.append((3, f'<ows:Title>{var_title}</ows:Title>'))
                    contents_xml_lines.append((3, f'<ows:Abstract>{var_abstract}</ows:Abstract>'))
                    contents_xml_lines.append((3, '<ows:WGS84BoundingBox>'))
                    contents_xml_lines.append((4, f'<ows:LowerCorner>{lon1} {lat1}</ows:LowerCorner>'))
                    contents_xml_lines.append((4, f'<ows:UpperCorner>{lon2} {lat2}</ows:UpperCorner>'))
                    contents_xml_lines.append((3, '</ows:WGS84BoundingBox>'))
                    contents_xml_lines.append(
                        (3, '<Style isDefault="true"><ows:Identifier>Default</ows:Identifier></Style>'))
                    contents_xml_lines.append((3, '<Format>image/png</Format>'))
                    contents_xml_lines.append(
                        (3, f'<TileMatrixSetLink><TileMatrixSet>{tile_grid_id}</TileMatrixSet></TileMatrixSetLink>'))
                    contents_xml_lines.append(
                        (3, f'<ResourceURL format="image/png" resourceType="tile" template="{layer_tile_url}"/>'))

                    non_spatial_dims = var.dims[0:-2]
                    for dim_name in non_spatial_dims:
                        if dim_name not in ds.coords:
                            continue
                        dimension_xml_key = f'{ds_name}.{dim_name}'
                        if dimension_xml_key in dimensions_xml_cache:
                            dimensions_xml_lines = dimensions_xml_cache[dimension_xml_key]
                        else:
                            coord_var = ds.coords[dim_name]
                            if len(coord_var.shape) != 1:
                                # strange case
                                continue
                            coord_bnds_var_name = coord_var.attrs.get('bounds', dim_name + '_bnds')
                            coord_bnds_var = ds.coords[coord_bnds_var_name] if coord_bnds_var_name in ds else None
                            if coord_bnds_var is not None:
                                if len(coord_bnds_var.shape) != 2 \
                                        or coord_bnds_var.shape[0] != coord_bnds_var.shape[0] \
                                        or coord_bnds_var.shape[1] != 2:
                                    # strange case
                                    coord_bnds_var = None
                            var_title = coord_var.attrs.get('long_name', dim_name)
                            units = 'ISO8601' if dim_name == 'time' else coord_var.attrs.get('units', '')
                            default = 'current' if dim_name == 'time' else '0'
                            current = 'true' if dim_name == 'time' else 'false'
                            dimensions_xml_lines = [(3, '<Dimension>'),
                                                    (4, f'<ows:Identifier>{dim_name}</ows:Identifier>'),
                                                    (4, f'<ows:Title>{var_title}</ows:Title>'),
                                                    (4, f'<ows:UOM>{units}</ows:UOM>'),
                                                    (4, f'<Default>{default}</Default>'),
                                                    (4, f'<Current>{current}</Current>')]
                            if coord_bnds_var is not None:
                                for i in range(len(coord_var)):
                                    value1 = coord_bnds_var.values[i, 0]
                                    value2 = coord_bnds_var.values[i, 1]
                                    dimensions_xml_lines.append((4, f'<Value>{value1}/{value2}</Value>'))
                            else:
                                for i in range(len(coord_var)):
                                    value = coord_var.values[i]
                                    dimensions_xml_lines.append((4, f'<Value>{value}</Value>'))
                            dimensions_xml_lines.append((3, '</Dimension>'))
                            dimensions_xml_cache[dimension_xml_key] = dimensions_xml_lines

                        contents_xml_lines.extend(dimensions_xml_lines)
                    contents_xml_lines.append((2, '</Layer>'))

        contents_xml_lines.append((1, '</Contents>'))

        contents_xml = '\n'.join(['%s%s' % (n * indent, xml) for n, xml in contents_xml_lines])

        themes_xml_lines = [(0, '<Themes>')]
        for dataset_descriptor in dataset_descriptors:
            ds_name = dataset_descriptor.get('Identifier')
            ds = self.get_dataset(ds_name)
            ds_title = dataset_descriptor.get('Title', ds.attrs.get('title', f'{ds_name} xcube dataset'))
            ds_abstract = ds.attrs.get('comment', '')
            themes_xml_lines.append((2, '<Theme>'))
            themes_xml_lines.append((3, f'<ows:Title>{ds_title}</ows:Title>'))
            themes_xml_lines.append((3, f'<ows:Abstract>{ds_abstract}</ows:Abstract>'))
            themes_xml_lines.append((3, f'<ows:Identifier>{ds_name}</ows:Identifier>'))
            for var_name in ds.data_vars:
                var = ds[var_name]
                var_title = var.attrs.get('title', var.attrs.get('long_name', var_name))
                themes_xml_lines.append((3, '<Theme>'))
                themes_xml_lines.append((4, f'<ows:Title>{var_title}</ows:Title>'))
                themes_xml_lines.append((4, f'<ows:Identifier>{ds_name}.{var_name}</ows:Identifier>'))
                themes_xml_lines.append((4, f'<LayerRef>{ds_name}.{var_name}</LayerRef>'))
                themes_xml_lines.append((3, '</Theme>'))
            themes_xml_lines.append((2, '</Theme>'))
        themes_xml_lines.append((1, '</Themes>'))
        themes_xml = '\n'.join(['%s%s' % (n * indent, xml) for n, xml in themes_xml_lines])

        # print(80 * '=')
        # print(contents_xml)
        # print(80 * '=')

        get_capablities_rest_url = base_url + '/xcube/wmts/1.0.0/WMTSCapabilities.xml'
        service_metadata_url_xml = f'<ServiceMetadataURL xlink:href="{get_capablities_rest_url}"/>'

        return f"""<?xml version="1.0" encoding="UTF-8"?>
<Capabilities xmlns="http://www.opengis.net/wmts/1.0"
              xmlns:ows="http://www.opengis.net/ows/1.1"
              xmlns:xlink="http://www.w3.org/1999/xlink"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
              xsi:schemaLocation="http://www.opengis.net/wmts/1.0 http://schemas.opengis.net/wmts/1.0.0/wmtsGetCapabilities_response.xsd"
              version="1.0.0">
    {service_identification_xml}
    {service_provider_xml}
    {operations_metadata_xml}
    {contents_xml}
    {themes_xml}
    {service_metadata_url_xml}
</Capabilities>
"""

    def get_dataset_tile(self,
                         ds_name: str,
                         var_name: str,
                         x: str, y: str, z: str,
                         params: RequestParams):

        x = params.to_int('x', x)
        y = params.to_int('y', y)
        z = params.to_int('z', z)

        dataset, var = self.get_dataset_and_variable(ds_name, var_name)

        dim_names = list(var.dims)
        if 'lon' not in dim_names or 'lat' not in dim_names:
            raise ServiceBadRequestError(f'variable {var_name!r} of dataset {ds_name!r} is not geo-spatial')

        dim_names.remove('lon')
        dim_names.remove('lat')

        var_indexers = _get_var_indexers(ds_name, var_name, var, dim_names, params)

        cmap_cbar = params.get_query_argument('cbar', default=None)
        cmap_vmin = params.get_query_argument_float('vmin', default=None)
        cmap_vmax = params.get_query_argument_float('vmax', default=None)
        if cmap_cbar is None or cmap_vmin is None or cmap_vmax is None:
            default_cmap_cbar, default_cmap_vmin, default_cmap_vmax = self.get_color_mapping(ds_name, var_name)
            cmap_cbar = cmap_cbar or default_cmap_cbar
            cmap_vmin = cmap_vmin or default_cmap_vmin
            cmap_vmax = cmap_vmax or default_cmap_vmax

        # TODO: use MD5 hashes as IDs instead

        var_index_id = '-'.join(f'-{dim_name}={dim_value}' for dim_name, dim_value in var_indexers.items())
        array_id = '%s-%s-%s' % (ds_name, var_name, var_index_id)
        image_id = '%s-%s-%s-%s' % (array_id, cmap_cbar, cmap_vmin, cmap_vmax)

        if image_id in self.pyramid_cache:
            pyramid = self.pyramid_cache[image_id]
        else:
            no_data_value = var.attrs.get('_FillValue')
            valid_range = var.attrs.get('valid_range')
            if valid_range is None:
                valid_min = var.attrs.get('valid_min')
                valid_max = var.attrs.get('valid_max')
                if valid_min is not None and valid_max is not None:
                    valid_range = [valid_min, valid_max]

            # Make sure we work with 2D image arrays only
            if var.ndim == 2:
                assert len(var_indexers) == 0
                array = var
            elif var.ndim > 2:
                assert len(var_indexers) == var.ndim - 2
                array = var.sel(method='nearest', **var_indexers)
            else:
                raise ServiceBadRequestError(f'Variable {var_name!r} of dataset {var_name!r} '
                                             'must be an N-D Dataset with N >= 2, '
                                             f'but {var_name!r} is only {var.ndim}-D')

            cmap_vmin = np.nanmin(array.values) if np.isnan(cmap_vmin) else cmap_vmin
            cmap_vmax = np.nanmax(array.values) if np.isnan(cmap_vmax) else cmap_vmax

            def array_image_id_factory(level):
                return 'arr-%s/%s' % (array_id, level)

            tile_grid = self.get_tile_grid(ds_name, var_name, var)

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
        raise ServiceResourceNotFoundError(f'Variable {var_name!r} not found in dataset {ds_name!r}')

    def get_dataset_tile_grid(self, ds_name: str, var_name: str, format_name: str, base_url: str) -> Dict[str, Any]:
        dataset, variable = self.get_dataset_and_variable(ds_name, var_name)
        tile_grid = self.get_tile_grid(ds_name, var_name, variable)
        if format_name == 'ol4.json':
            return _tile_grid_to_ol4_xyz_source_options(
                self.get_dataset_tile_url(ds_name, var_name, base_url), tile_grid)
        elif format_name == 'cesium.json':
            return _tile_grid_to_cesium_source_options(
                self.get_dataset_tile_url(ds_name, var_name, base_url), tile_grid)
        else:
            raise ServiceBadRequestError(f'Unknown tile schema format {format_name!r}')

    # noinspection PyMethodMayBeStatic
    def get_dataset_tile_url(self, ds_name: str, var_name: str, base_url: str):
        return base_url + f'/xcube/tile/{ds_name}/{var_name}' + '/{z}/{x}/{y}.png'

    # noinspection PyMethodMayBeStatic
    def get_tile_grid(self, ds_name: str, var_name: str, var: xr.DataArray):
        tile_grid = self.get_or_compute_tile_grid(ds_name, var)
        if tile_grid is None:
            raise ServiceError(f'Failed computing tile grid for variable {var_name!r} of dataset {ds_name!r}')
        return tile_grid

    def get_ne2_tile(self, x: str, y: str, z: str, params: RequestParams):
        x = params.to_int('x', x)
        y = params.to_int('y', y)
        z = params.to_int('z', z)
        return NaturalEarth2Image.get_pyramid().get_tile(x, y, z)

    def get_ne2_tile_grid(self, format_name: str, base_url: str):
        if format_name == 'ol4.json':
            return _tile_grid_to_ol4_xyz_source_options(base_url + '/xcube/tile/ne2/{z}/{x}/{y}.jpg',
                                                        NaturalEarth2Image.get_pyramid().tile_grid)
        else:
            raise ServiceBadRequestError(f'Unknown tile schema format {format_name!r}')

    def get_color_bars(self, format_name):
        from .im.cmaps import get_cmaps
        import json
        cmaps = get_cmaps()
        if format_name == 'text/json':
            return json.dumps(cmaps, indent=2)
        elif format_name == 'text/html':
            html_head = '<!DOCTYPE html>\n' + \
                        '<html lang="en">\n' + \
                        '<head>' + \
                        '<meta charset="UTF-8">' + \
                        '<title>xcube server color maps</title>' + \
                        '</head>\n' + \
                        '<body style="padding: 0.2em">\n'
            html_body = ''
            html_foot = '</body>\n' \
                        '</html>\n'
            for cmap_cat, cmap_desc, cmap_bars in cmaps:
                html_body += '    <h2>%s</h2>\n' % cmap_cat
                html_body += '    <p><i>%s</i></p>\n' % cmap_desc
                html_body += '    <table style=border: 0">\n'
                for cmap_bar in cmap_bars:
                    cmap_name, cmap_data = cmap_bar
                    cmap_image = f'<img src="data:image/png;base64,{cmap_data}" width="100%%" height="32"/>'
                    html_body += f'        <tr><td style="width: 5em">{cmap_name}:</td><td style="width: 40em">{cmap_image}</td></tr>\n'
                html_body += '    </table>\n'
            return html_head + html_body + html_foot
        raise ServiceBadRequestError(f'Format {format_name!r} not supported for color bars')

    def get_dataset_descriptors(self):
        dataset_descriptors = self.config.get('Datasets')
        if not dataset_descriptors:
            raise ServiceConfigError(f"No datasets configured")
        return dataset_descriptors

    def get_dataset_descriptor(self, ds_name: str) -> Dict[str, str]:
        dataset_descriptors = self.get_dataset_descriptors()
        if not dataset_descriptors:
            raise ServiceConfigError(f"No datasets configured")
        # TODO: optimize by dict/key lookup
        for dataset_descriptor in dataset_descriptors:
            if dataset_descriptor['Identifier'] == ds_name:
                return dataset_descriptor
        raise ServiceResourceNotFoundError(f"Dataset {ds_name!r} not found")

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
                ds = xr.open_zarr(cached_store)
            elif fs_type == 'local':
                if not os.path.isabs(path):
                    path = os.path.join(self.base_dir, path)
                data_format = dataset_descriptor.get('Format', 'nc')
                if data_format == 'nc':
                    ds = xr.open_dataset(path)
                elif data_format == 'zarr':
                    ds = xr.open_zarr(path)
                else:
                    raise ServiceConfigError(f"Invalid format={data_format!r} in dataset descriptor {ds_name!r}")
            else:
                raise ServiceConfigError(f"Invalid fs={fs_type!r} in dataset descriptor {ds_name!r}")

            tile_grid_cache = dict()
            self.dataset_cache[ds_name] = ds, dataset_descriptor, tile_grid_cache

            t2 = time.clock()

            if TRACE_PERF:
                print(f'PERF: opening {ds_name!r} took {t2-t1} seconds')

        return ds

    def get_dataset_and_coord_variable(self, ds_name: str, dim_name: str):
        ds = self.get_dataset(ds_name)
        if dim_name not in ds.coords:
            raise ServiceResourceNotFoundError(f'Dimension {dim_name!r} has no coordinates in dataset {ds_name!r}')
        return ds, ds.coords[dim_name]

    def get_or_compute_tile_grid(self, ds_name: str, var: xr.DataArray):
        self.get_dataset(ds_name) # make sure ds_name provides a cached entry
        _, _, tile_grid_cache = self.dataset_cache[ds_name]
        shape = var.shape
        tile_grid_key = f'tg_{shape[-1]}_{shape[-2]}'
        if tile_grid_key in tile_grid_cache:
            tile_grid = tile_grid_cache[tile_grid_key]
        else:
            tile_grid = compute_tile_grid(var)
            tile_grid_cache[tile_grid_key] = tile_grid
        return tile_grid

def _get_var_indexers(ds_name: str,
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


def _tile_grid_to_cesium_source_options(url: str, tile_grid: TileGrid):
    """
    Convert TileGrid into options to be used with Cesium.UrlTemplateImageryProvider(options) of Cesium 1.45+.

    See

    * https://cesiumjs.org/Cesium/Build/Documentation/UrlTemplateImageryProvider.html?classFilter=UrlTemplateImageryProvider

    :param tile_grid: tile grid
    :param url: source url
    :return:
    """
    ge = tile_grid.geo_extent
    rectangle = dict(west=ge.west, south=ge.south, east=ge.east, north=ge.north)
    return dict(url=url,
                rectangle=rectangle,
                minimumLevel=0,
                maximumLevel=tile_grid.num_levels - 1,
                tileWidth=tile_grid.tile_size[0],
                tileHeight=tile_grid.tile_size[1],
                tilingScheme=dict(rectangle=rectangle,
                                  numberOfLevelZeroTilesX=tile_grid.num_level_zero_tiles_x,
                                  numberOfLevelZeroTilesY=tile_grid.num_level_zero_tiles_y))
