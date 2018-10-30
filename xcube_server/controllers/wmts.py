from .tiles import get_or_compute_tile_grid
from ..context import ServiceContext


def get_wmts_capabilities(ctx: ServiceContext, format_name: str, base_url: str):
    default_format_name = 'application/xml'
    format_name = format_name or default_format_name
    if format_name != default_format_name:
        raise ValueError(f'format_name must be "{default_format_name}"')

    service_identification_xml = f"""
    <ows:ServiceIdentification>
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

    service_provider = ctx.config['ServiceProvider']
    service_contact = service_provider['ServiceContact']
    contact_info = service_contact['ContactInfo']
    phone = contact_info['Phone']
    address = contact_info['Address']

    service_provider_xml = f"""
    <ows:ServiceProvider>
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

    wmts_kvp_url = ctx.get_service_url(base_url, 'wmts/1.0.0/kvp?')

    operations_metadata_xml = f"""
    <ows:OperationsMetadata>
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

    dataset_descriptors = ctx.get_dataset_descriptors()
    tile_grids = dict()
    indent = '    '

    layer_base_url = ctx.get_service_url(base_url, 'wmts/1.0.0/tile/%s/%s/{TileMatrix}/{TileCol}/{TileRow}.png')

    dimensions_xml_cache = dict()

    contents_xml_lines = [(0, '<Contents>')]
    for dataset_descriptor in dataset_descriptors:
        ds_name = dataset_descriptor['Identifier']
        ds = ctx.get_dataset(ds_name)
        for var_name in ds.data_vars:
            var = ds[var_name]
            if len(var.shape) <= 2 or var.dims[-1] != 'lon' or var.dims[-2] != 'lat':
                continue

            tile_grid_id = 'TileGrid_%s_%s' % (var.shape[-1], var.shape[-2])
            write_tile_matrix_set = False
            if tile_grid_id in tile_grids:
                tile_grid = tile_grids[tile_grid_id]
            else:
                tile_grid = get_or_compute_tile_grid(ctx, ds_name, var)
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
                            coord_bnds_var_values = coord_bnds_var.values
                            for i in range(len(coord_var)):
                                value1 = coord_bnds_var_values[i, 0]
                                value2 = coord_bnds_var_values[i, 1]
                                dimensions_xml_lines.append((4, f'<Value>{value1}/{value2}</Value>'))
                        else:
                            coord_var_values = coord_var.values
                            for i in range(len(coord_var)):
                                value = coord_var_values[i]
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
        ds = ctx.get_dataset(ds_name)
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

    get_capablities_rest_url = ctx.get_service_url(base_url, 'wmts/1.0.0/WMTSCapabilities.xml')
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
