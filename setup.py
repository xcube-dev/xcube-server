#!/usr/bin/env python3

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


from setuptools import setup, find_packages

# in alphabetical oder
requirements = [
    'cython',
    'h5netcdf',
    'matplotlib',
    'netcdf4',
    'numpy',
    'pandas',
    'pillow',
    'pyyaml',
    's3fs',
    'tornado',
    'xarray',
    'zarr',
]

packages = find_packages(exclude=["test", "test.*"])

__version__ = None
__description__ = None
with open('xcube_server/__init__.py') as f:
    exec(f.read())

setup(
    name="xcube_server",
    version=__version__,
    description=__description__,
    license='MIT',
    author='xcube Development Team',
    packages=packages,
    package_data={
        'xcube_server.res': ['**/*'],
    },
    entry_points={
        'console_scripts': [
            'xcs = xcube_server.app:main',
            'xcube-server = xcube_server.app:main',
        ],
    },
    install_requires=requirements,
)
