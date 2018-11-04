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

from typing import Dict, Union

import numpy as np
import shapely.geometry
import xarray as xr
from pandas import Timestamp

from ..context import ServiceContext
from ..utils import get_dataset_bounds, get_dataset_geometry, get_box_split_bounds_geometry, get_geometry_mask


def get_time_series_info(ctx: ServiceContext) -> Dict:
    time_series_info = {'layers': []}
    descriptors = ctx.get_dataset_descriptors()
    for descriptor in descriptors:
        if 'Identifier' in descriptor:
            dataset = ctx.get_dataset(descriptor['Identifier'])
            if 'time' not in dataset.variables:
                continue
            xmin, ymin, xmax, ymax = get_dataset_bounds(dataset)
            time_data = dataset.variables['time'].data
            time_stamps = []
            for time in time_data:
                time_stamps.append(Timestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ'))
            for variable in dataset.data_vars.variables:
                variable_dict = {'name': '{0}.{1}'.format(descriptor['Identifier'], variable),
                                 'dates': time_stamps,
                                 'bounds': dict(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)}
                time_series_info['layers'].append(variable_dict)
    return time_series_info


def get_time_series_for_point(ctx: ServiceContext,
                              ds_name: str, var_name: str,
                              lon: float, lat: float,
                              start_date: np.datetime64 = None, end_date: np.datetime64 = None) -> Dict:
    dataset, variable = ctx.get_dataset_and_variable(ds_name, var_name)
    return _get_time_series_for_point(dataset, variable,
                                      shapely.geometry.Point(lon, lat),
                                      start_date=start_date, end_date=end_date)


def get_time_series_for_geometry(ctx: ServiceContext,
                                 ds_name: str, var_name: str,
                                 geometry: Union[shapely.geometry.base.BaseGeometry, Dict],
                                 start_date: np.datetime64 = None, end_date: np.datetime64 = None) -> Dict:
    dataset, variable = ctx.get_dataset_and_variable(ds_name, var_name)
    if isinstance(geometry, dict):
        geometry = shapely.geometry.shape(geometry)
    return _get_time_series_for_geometry(dataset, variable,
                                         geometry,
                                         start_date=start_date, end_date=end_date)


def _get_time_series_for_point(dataset: xr.Dataset,
                               variable: xr.DataArray,
                               point: shapely.geometry.Point,
                               start_date: np.datetime64 = None,
                               end_date: np.datetime64 = None) -> Dict:
    bounds = get_dataset_geometry(dataset)
    time_series = {'results': []}
    if not bounds.contains(point):
        return time_series

    point_subset = variable.sel(lon=point.x, lat=point.y, method='Nearest')
    # noinspection PyTypeChecker
    time_subset = point_subset.sel(time=slice(start_date, end_date))
    for entry in time_subset:
        statistics = {'totalCount': 1}
        if np.isnan(entry.data):
            statistics['validCount'] = 0
            statistics['average'] = None
        else:
            statistics['validCount'] = 1
            statistics['average'] = entry.item()
        result = {'result': statistics, 'date': str(entry.time.data)}
        time_series['results'].append(result)
    return time_series


def _get_time_series_for_geometry(dataset: xr.Dataset,
                                  variable: xr.DataArray,
                                  geometry: shapely.geometry.base.BaseGeometry,
                                  start_date: np.datetime64 = None,
                                  end_date: np.datetime64 = None) -> Dict:
    if isinstance(geometry, shapely.geometry.Point):
        return _get_time_series_for_point(dataset, variable,
                                          geometry,
                                          start_date=start_date, end_date=end_date)

    ds_lon_min, ds_lat_min, ds_lon_max, ds_lat_max = get_dataset_bounds(dataset)
    dataset_geometry = get_box_split_bounds_geometry(ds_lon_min, ds_lat_min, ds_lon_max, ds_lat_max)
    # TODO: split geometry
    split_geometry = geometry
    time_series = {'results': []}
    actual_geometry = dataset_geometry.intersection(split_geometry)
    if actual_geometry.is_empty:
        return time_series

    width = len(dataset.lon)
    height = len(dataset.lat)
    res = (ds_lat_max - ds_lat_min) / height

    # TODO by forman: we may improve performance for hi-res cubes by extracting a spatial subset first
    # g_lon_min, g_lat_min, g_lon_max, g_lat_max = actual_geometry.bounds
    # x1 = _clamp(int(math.floor((g_lon_min - ds_lon_min) / res)), 0, width - 1)
    # x2 = _clamp(int(math.ceil((g_lon_max - ds_lon_min) / res)), 0, width - 1)
    # y1 = _clamp(int(math.floor((g_lat_min - ds_lat_min) / res)), 0, height - 1)
    # y2 = _clamp(int(math.ceil((g_lat_max - ds_lat_min) / res)), 0, height - 1)
    # ds_subset = dataset.isel(lon=slice(x1, x2), lat=slice(y1, y2))
    # ds_subset = ds_subset.sel(time=slice(start_date, end_date))

    mask = get_geometry_mask(width, height, actual_geometry, ds_lon_min, ds_lat_min, res)
    total_count = np.count_nonzero(mask)
    variable = variable.sel(time=slice(start_date, end_date))
    num_times = len(variable.time)

    for time_index in range(num_times):
        variable_slice = variable.isel(time=time_index)

        masked_var = variable_slice.where(mask)
        valid_count = np.count_nonzero(np.where(np.isnan(masked_var), 0, 1))
        mean_ts_var = masked_var.mean(["lat", "lon"])

        statistics = {'totalCount': total_count}
        if np.isnan(mean_ts_var.data):
            statistics['validCount'] = 0
            statistics['average'] = None
        else:
            statistics['validCount'] = valid_count
            statistics['average'] = float(mean_ts_var.data)
        result = {'result': statistics, 'date': str(mean_ts_var.time.data)}
        time_series['results'].append(result)

    return time_series

# def _clamp(x, x1, x2):
#     if x < x1:
#         return x1
#     if x > x2:
#         return x2
#     return x
