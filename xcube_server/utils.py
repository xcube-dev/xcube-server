import warnings
from typing import Optional, Tuple

import shapely.geometry
import xarray as xr
import numpy as np

from xcube_server.im import GeoExtent, TileGrid


def get_dataset_geometry(dataset: xr.Dataset) -> shapely.geometry.base.BaseGeometry:
    return get_box_geometry(*get_dataset_bounds(dataset))


def get_dataset_bounds(dataset: xr.Dataset) -> Tuple[float, float, float, float]:
    lon_var = dataset.coords.get("lon")
    lat_var = dataset.coords.get("lat")
    if lon_var is None:
        raise ValueError('Missing coordinate variable "lon"')
    if lat_var is None:
        raise ValueError('Missing coordinate variable "lat"')

    lon_bnds_name = lon_var.attrs["bounds"] if "bounds" in lon_var.attrs else "lon_bnds"
    if lon_bnds_name in dataset.coords:
        lon_bnds_var = dataset.coords[lon_bnds_name]
        lon_min = lon_bnds_var[0][0]
        lon_max = lon_bnds_var[-1][1]
    else:
        lon_min = lon_var[0]
        lon_max = lon_var[-1]
        delta = min(abs(np.diff(lon_var)))
        lon_min -= 0.5 * delta
        lon_max += 0.5 * delta

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
        delta = min(abs(np.diff(lat_var)))
        lat_min = min(lat1, lat2) - 0.5 * delta
        lat_max = max(lat1, lat2) + 0.5 * delta

    return float(lon_min), float(lat_min), float(lon_max), float(lat_max)


def get_box_geometry(lon_min: float, lat_min: float,
                     lon_max: float, lat_max: float) -> shapely.geometry.base.BaseGeometry:
    if lon_max >= lon_min:
        return shapely.geometry.box(lon_min, lat_min, lon_max, lat_max)
    else:
        return shapely.geometry.MultiPolygon(polygons=[shapely.geometry.box(lon_min, lat_min, 180.0, lat_max),
                                                       shapely.geometry.box(-180.0, lat_min, lon_max, lat_max)])


def compute_tile_grid(var: xr.DataArray) -> Optional[TileGrid]:
    """
    Compute an efficient tile grid for the given variable *var*.

    :param var: A variable of an xarray dataset.
    :return:  a new TileGrid object or None if *var* cannot be represented as a spatial image
    """
    lat_dim_name = 'lat'
    lon_dim_name = 'lon'
    if lat_dim_name not in var.coords or lon_dim_name not in var.coords:
        return None
    width, height = var.shape[-1], var.shape[-2]
    lats = var.coords[lat_dim_name]
    lons = var.coords[lon_dim_name]
    try:
        geo_extent = GeoExtent.from_coord_arrays(lons, lats)
    except ValueError as e:
        warnings.warn(f'failed to derive geo-extent for tile grid: {e}')
        # Create a default geo-extent which is probably wrong, but at least we see something
        geo_extent = GeoExtent()
    try:
        return TileGrid.create(width, height, 360, 360, geo_extent)
    except ValueError:
        return TileGrid(1, 1, 1, width, height, geo_extent)
