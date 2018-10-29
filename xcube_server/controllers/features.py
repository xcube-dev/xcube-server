import logging
from typing import Any, Dict, List

import shapely.geometry
import shapely.wkt

from xcube_server.logtime import log_time
from ..context import ServiceContext

_LOG = logging.getLogger('xcube')

GeoJsonFeatureCollection = Dict
GeoJsonFeature = Dict


def find_dataset_features(ctx: ServiceContext,
                          ds_name: str,
                          query_expr: Any = None,
                          comb_op: str = "and") -> GeoJsonFeatureCollection:
    return _find_features(ctx,
                          query_geometry=_get_dataset_bounds(ctx, ds_name),
                          query_expr=query_expr, comb_op=comb_op)


def find_features(ctx: ServiceContext,
                  box_coords: str = None,
                  geom_wkt: str = None,
                  query_expr: Any = None,
                  geojson_obj: Dict = None,
                  comb_op: str = "and") -> GeoJsonFeatureCollection:
    query_geometry = None
    if box_coords:
        query_geometry = _get_box_geometry(*[float(s) for s in box_coords.split(",")])
    elif geom_wkt:
        query_geometry = shapely.wkt.loads(geom_wkt)
    elif geojson_obj:
        if geojson_obj["type"] == "FeatureCollection":
            query_geometry = shapely.geometry.shape(geojson_obj["features"][0]["geometry"])
        elif geojson_obj["type"] == "Feature":
            query_geometry = shapely.geometry.shape(geojson_obj["geometry"])
        else:
            query_geometry = shapely.geometry.shape(geojson_obj)
    return _find_features(ctx, query_geometry, query_expr, comb_op)


def _find_features(ctx: ServiceContext,
                   query_geometry: shapely.geometry.base.BaseGeometry = None,
                   query_expr: Any = None,
                   comb_op: str = "and") -> GeoJsonFeatureCollection:
    with log_time() as cm:
        features = __find_features(ctx, query_geometry, query_expr, comb_op)
    _LOG.info(f"{len(features)} features found within {cm.duration} seconds")
    return dict(type="FeatureCollection", features=features)


def __find_features(ctx: ServiceContext,
                    query_geometry: shapely.geometry.base.BaseGeometry = None,
                    query_expr: Any = None,
                    comb_op: str = "and") -> List[GeoJsonFeature]:
    features = ctx.get_features()
    matching_features = []
    if query_geometry is None:
        if query_expr is None:
            return features
        else:
            raise NotImplementedError()
    else:
        if query_expr is None:
            for feature in features:
                geometry = shapely.geometry.shape(feature["geometry"])
                if geometry.intersects(query_geometry):
                    matching_features.append(feature)
        else:
            raise NotImplementedError()
    return matching_features


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


def _get_box_geometry(lon_min: float, lat_min: float,
                      lon_max: float, lat_max: float) -> shapely.geometry.base.BaseGeometry:
    if lon_max >= lon_min:
        return shapely.geometry.box(lon_min, lat_min, lon_max, lat_max)
    else:
        return shapely.geometry.MultiPolygon(polygons=[shapely.geometry.box(lon_min, lat_min, 180.0, lat_max),
                                                       shapely.geometry.box(-180.0, lat_min, lon_max, lat_max)])
