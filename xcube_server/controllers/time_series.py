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
from pandas import Timestamp
from typing import Dict


def get_time_series_info(ctx: ServiceContext) -> Dict:
    time_series_info = {'layers': []}
    descriptors = ctx.get_dataset_descriptors()
    for descriptor in descriptors:
        if 'Identifier' in descriptor:
            dataset = ctx.get_dataset(descriptor['Identifier'])
            if not 'time' in dataset.variables:
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
    if 'lon' not in dim_names or 'lat' not in dim_names:
        raise ServiceBadRequestError(f'variable {var_name!r} of dataset {ds_name!r} is not geo-spatial')
    if 'time' not in dim_names:
        raise ServiceBadRequestError(f'variable {var_name!r} of dataset {ds_name!r} has no time information')
    point_subset = variable.sel(lat=lat, lon=lon, method='Nearest')
    time_subset = point_subset.sel(time=slice(start_date, end_date))
    time_series_for_point = {'results': []}
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
