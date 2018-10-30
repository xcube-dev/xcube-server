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

from ..context import ServiceContext
from ..errors import ServiceBadRequestError
from ..reqparams import RequestParams
import numpy as np
import shapely.geometry
from pandas import Timestamp
from typing import Dict


def get_time_series_info(ctx: ServiceContext) -> Dict:
    time_series_info = {'layers': []}
    descriptors = ctx.get_dataset_descriptors()
    for descriptor in descriptors:
        if 'Identifier' in descriptor:
            dataset = ctx.get_dataset(descriptor['Identifier'])
            if 'time' not in dataset.variables:
                continue
            attributes = dataset.attrs
            bounds = {'xmin': attributes.get('geospatial_lon_min', -180),
                      'ymin': attributes.get('geospatial_lat_min', -90),
                      'xmax': (attributes.get('geospatial_lon_max', +180)),
                      'ymax': attributes.get('geospatial_lat_max', +90)}
            time_data = dataset.variables['time'].data
            time_stamps = []
            for time in time_data:
                time_stamps.append(Timestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ'))
            for variable in dataset.data_vars.variables:
                variable_dict = {'name': '{0}.{1}'.format(descriptor['Identifier'], variable),
                                 'dates': time_stamps,
                                 'bounds': bounds}
                time_series_info['layers'].append(variable_dict)
    return time_series_info


def get_time_series_for_point(ctx: ServiceContext, ds_name: str, var_name: str, params: RequestParams) -> Dict:
    lat = params.get_query_argument_float('lat', default=None)
    lon = params.get_query_argument_float('lon', default=None)
    if lat is None or lon is None:
        raise ServiceBadRequestError('lat and lon must be given as query parameters')
    start_date = np.datetime64(params.get_query_argument('startDate', default='1970-01-01'))
    end_date = np.datetime64(params.get_query_argument('endDate', default='2099-12-31'))
    dataset, variable = ctx.get_dataset_and_variable(ds_name, var_name)
    dim_names = list(variable.dims)
    if 'time' not in dim_names:
        raise ServiceBadRequestError(f'variable {var_name!r} of dataset {ds_name!r} has no time information')
    if 'lon' not in dim_names or 'lat' not in dim_names:
        raise ServiceBadRequestError(f'variable {var_name!r} of dataset {ds_name!r} is not geo-spatial')
    point = shapely.geometry.Point(lon, lat)
    ds_bounds = _get_dataset_bounds(ctx, ds_name)
    time_series_for_point = {'results': []}
    if ds_bounds.contains(point):
        point_subset = variable.sel(lat=lat, lon=lon, method='Nearest')
        # noinspection PyTypeChecker
        time_subset = point_subset.sel(time=slice(start_date, end_date))
        for entry in time_subset:
            statistics = {'totalCount': 1}
            if np.isnan(entry.data):
                statistics['validCount'] = 0
                statistics['average'] = np.NAN
            else:
                statistics['validCount'] = 1
                statistics['average'] = entry.item()
            result = {'result': statistics, 'date': Timestamp(entry.time.data).strftime('%Y-%m-%d')}
            time_series_for_point['results'].append(result)
    return time_series_for_point


#TODO move this function to utils class or similar
def _get_dataset_bounds(ctx: ServiceContext, ds_name: str) -> shapely.geometry.base.BaseGeometry:
    dataset = ctx.get_dataset(ds_name)
    lon_var = dataset.coords.get("lon")
    lon_bnds_name = lon_var.attrs["bounds"] if "bounds" in lon_var.attrs else "lon_bnds"
    if lon_bnds_name in dataset.coords:
        lon_bnds_var = dataset.coords[lon_bnds_name]
        lon_min = lon_bnds_var[0][0]
        lon_max = lon_bnds_var[-1][1]
    else:
        lon_min = lon_var[0]
        lon_max = lon_var[-1]
        delta = (lon_max - lon_min) / (len(lon_var) - 1)
        lon_min -= 0.5 * delta
        lon_max += 0.5 * delta
    lat_var = dataset.coords.get("lat")
    lat_bnds_name = lat_var.attrs["bounds"] if "bounds" in lat_var.attrs else "lat_bnds"
    if lat_bnds_name in dataset.coords:
        lat_bnds_var = dataset.coords[lat_bnds_name]
        lat1 = lat_bnds_var[0][0]
        lat2 = lat_bnds_var[-1][1]
        lat_min = min(lat1, lat2)
        lat_max = max(lat1, lat2)
    else:
        lat1 = lat_var[0]
        lat2 = lat_var[-1]
        delta = abs(lat2 - lat1) / (len(lat_var) - 1)
        lat_min = min(lat1, lat2) - 0.5 * delta
        lat_max = max(lat1, lat2) + 0.5 * delta
    return _get_box_geometry(lon_min, lat_min, lon_max, lat_max)


#TODO move this function to utils class or similar
def _get_box_geometry(lon_min: float, lat_min: float,
                      lon_max: float, lat_max: float) -> shapely.geometry.base.BaseGeometry:
    if lon_max >= lon_min:
        return shapely.geometry.box(lon_min, lat_min, lon_max, lat_max)
    else:
        return shapely.geometry.MultiPolygon(polygons=[shapely.geometry.box(lon_min, lat_min, 180.0, lat_max),
                                                       shapely.geometry.box(-180.0, lat_min, lon_max, lat_max)])
