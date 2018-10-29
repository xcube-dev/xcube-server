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
from pandas import Timestamp
from typing import Any, Dict


def get_time_series_info(ctx: ServiceContext) -> Dict[str, Any]:
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
