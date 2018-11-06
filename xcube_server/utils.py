import warnings
from typing import Optional, Tuple, Union, Dict, Any

import affine
import numpy as np
import rasterio.features
import shapely.geometry
import shapely.geometry
import xarray as xr

from xcube_server.im import GeoExtent, TileGrid

Bounds = Tuple[float, float, float, float]
SplitBounds = Tuple[Bounds, Optional[Bounds]]


def get_dataset_geometry(dataset: Union[xr.Dataset, xr.DataArray]) -> shapely.geometry.base.BaseGeometry:
    return get_box_split_bounds_geometry(*get_dataset_bounds(dataset))


def get_dataset_bounds(dataset: Union[xr.Dataset, xr.DataArray]) -> Bounds:
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


def get_box_split_bounds(lon_min: float, lat_min: float,
                         lon_max: float, lat_max: float) -> SplitBounds:
    if lon_max >= lon_min:
        return (lon_min, lat_min, lon_max, lat_max), None
    else:
        return (lon_min, lat_min, 180.0, lat_max), (-180.0, lat_min, lon_max, lat_max)


def get_box_split_bounds_geometry(lon_min: float, lat_min: float,
                                  lon_max: float, lat_max: float) -> shapely.geometry.base.BaseGeometry:
    box_1, box_2 = get_box_split_bounds(lon_min, lat_min, lon_max, lat_max)
    if box_2 is not None:
        return shapely.geometry.MultiPolygon(polygons=[shapely.geometry.box(*box_1), shapely.geometry.box(*box_2)])
    else:
        return shapely.geometry.box(*box_1)


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


def get_geometry_mask(width: int, height: int,
                      geometry: Union[shapely.geometry.base.BaseGeometry, Dict],
                      lon_min: float, lat_min: float, res: float) -> np.ndarray:
    # noinspection PyTypeChecker
    transform = affine.Affine(res, 0.0, lon_min,
                              0.0, -res, lat_min + res * height)
    return rasterio.features.geometry_mask([geometry],
                                           out_shape=(height, width),
                                           transform=transform,
                                           all_touched=True,
                                           invert=True)


GEOJSON_PRIMITIVE_GEOMETRY_TYPES = {"Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon"}
GEOJSON_MULTI_GEOMETRY_TYPE = "MultiGeometry"


def is_geojson_geometry(obj: Any) -> bool:
    if not isinstance(obj, dict) or "type" not in obj:
        return False

    if "type" not in obj:
        return False

    geometry_type = obj["type"]
    if geometry_type in GEOJSON_PRIMITIVE_GEOMETRY_TYPES:
        if "coordinates" not in obj:
            return False
        coordinates = obj["coordinates"]
        return coordinates is None or isinstance(coordinates, list)

    if geometry_type == GEOJSON_MULTI_GEOMETRY_TYPE:
        if "geometries" not in obj:
            return False
        geometries = obj["geometries"]
        return geometries is None or isinstance(geometries, list)

    return False
