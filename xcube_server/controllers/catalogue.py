import json
from typing import Dict

import numpy as np

from xcube_server.controllers.tiles import get_tile_source_options, get_dataset_tile_url, get_or_compute_tile_grid
from ..context import ServiceContext
from ..errors import ServiceBadRequestError
from ..im.cmaps import get_cmaps


def get_datasets(ctx: ServiceContext) -> Dict:
    dataset_descriptors = ctx.get_dataset_descriptors()
    datasets = list()
    for dataset_descriptor in dataset_descriptors:
        datasets.append(dict(name=dataset_descriptor['Identifier'],
                             title=dataset_descriptor['Title']))
    return dict(datasets=datasets)


def get_dataset_variables(ctx: ServiceContext, ds_name: str, client: str, base_url: str) -> Dict:
    ds = ctx.get_dataset(ds_name)
    variables = list()
    for var_name in ds.data_vars:
        var = ds.data_vars[var_name]
        if 'time' not in var.dims or 'lat' not in var.dims or 'lon' not in var.dims:
            continue
        attrs = var.attrs
        tile_grid = get_or_compute_tile_grid(ctx, ds_name, var)
        ol_tile_xyz_source_options = get_tile_source_options(tile_grid,
                                                             get_dataset_tile_url(ctx, ds_name, var_name, base_url),
                                                             client)
        variables.append(dict(id=f'{ds_name}.{var_name}',
                              name=var_name,
                              dims=list(var.dims),
                              shape=list(var.shape),
                              dtype=str(var.dtype),
                              units=attrs.get('units', ''),
                              title=attrs.get('title', attrs.get('long_name', var_name)),
                              tileSourceOptions=ol_tile_xyz_source_options))
    attrs = ds.attrs
    return dict(name=ds_name,
                title=attrs.get('title', ''),
                bbox=[attrs.get('geospatial_lon_min', -180),
                      attrs.get('geospatial_lat_min', -90),
                      attrs.get('geospatial_lon_max', +180),
                      attrs.get('geospatial_lat_max', +90)],
                variables=variables)


def get_dataset_coordinates(ctx: ServiceContext, ds_name: str, dim_name: str) -> Dict:
    ds, var = ctx.get_dataset_and_coord_variable(ds_name, dim_name)
    values = list()
    if np.issubdtype(var.dtype, np.floating):
        converter = float
    elif np.issubdtype(var.dtype, np.integer):
        converter = int
    else:
        converter = str
    for value in var.values:
        values.append(converter(value))
    return dict(name=dim_name,
                dtype=str(var.dtype),
                values=values)


# noinspection PyUnusedLocal
def get_color_bars(ctx: ServiceContext, mime_type: str) -> str:
    cmaps = get_cmaps()
    if mime_type == 'application/json':
        return json.dumps(cmaps, indent=2)
    elif mime_type == 'text/html':
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
                html_body += f'        <tr><td style="width: 5em">{cmap_name}:' \
                             f'</td><td style="width: 40em">{cmap_image}</td></tr>\n'
            html_body += '    </table>\n'
        return html_head + html_body + html_foot
    raise ServiceBadRequestError(f'Format {mime_type!r} not supported for color bars')
