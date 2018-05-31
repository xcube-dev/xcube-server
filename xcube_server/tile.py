import warnings
from typing import Optional

import xarray as xr

from xcube_server.im import GeoExtent, TileGrid


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
