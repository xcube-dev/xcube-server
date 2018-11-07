import logging
from typing import Any, Dict, List

import shapely.geometry
import shapely.wkt
from shapely.errors import WKTReadingError

from ..context import ServiceContext
from ..errors import ServiceBadRequestError
from ..logtime import log_time
from ..utils import get_dataset_geometry, get_box_split_bounds_geometry

_LOG = logging.getLogger('xcube')

GeoJsonFeatureCollection = Dict
GeoJsonFeature = Dict


def find_dataset_features(ctx: ServiceContext,
                          collection_name: str,
                          ds_name: str,
                          query_expr: Any = None,
                          comb_op: str = "and") -> GeoJsonFeatureCollection:
    dataset = ctx.get_dataset(ds_name)
    query_geometry = get_dataset_geometry(dataset)
    return _find_features(ctx,
                          collection_name,
                          query_geometry=query_geometry,
                          query_expr=query_expr, comb_op=comb_op)


def find_features(ctx: ServiceContext,
                  collection_name: str,
                  box_coords: str = None,
                  geom_wkt: str = None,
                  query_expr: Any = None,
                  geojson_obj: Dict = None,
                  comb_op: str = "and") -> GeoJsonFeatureCollection:
    query_geometry = None
    if box_coords:
        try:
            query_geometry = get_box_split_bounds_geometry(*[float(s) for s in box_coords.split(",")])
        except (TypeError, ValueError) as e:
            raise ServiceBadRequestError("Received invalid bounding box geometry") from e
    elif geom_wkt:
        try:
            query_geometry = shapely.wkt.loads(geom_wkt)
        except (TypeError, WKTReadingError) as e:
            raise ServiceBadRequestError("Received invalid geometry WKT") from e
    elif geojson_obj:
        try:
            if geojson_obj["type"] == "FeatureCollection":
                query_geometry = shapely.geometry.shape(geojson_obj["features"][0]["geometry"])
            elif geojson_obj["type"] == "Feature":
                query_geometry = shapely.geometry.shape(geojson_obj["geometry"])
            else:
                query_geometry = shapely.geometry.shape(geojson_obj)
        except (IndexError, ValueError, KeyError) as e:
            raise ServiceBadRequestError("Received invalid GeoJSON object") from e
    return _find_features(ctx, collection_name, query_geometry, query_expr, comb_op)


def _find_features(ctx: ServiceContext,
                   collection_name: str,
                   query_geometry: shapely.geometry.base.BaseGeometry = None,
                   query_expr: Any = None,
                   comb_op: str = "and") -> GeoJsonFeatureCollection:
    with log_time() as cm:
        features = __find_features(ctx, collection_name, query_geometry, query_expr, comb_op)
    _LOG.info(f"{len(features)} features found within {cm.duration} seconds")
    return dict(type="FeatureCollection", features=features)


def __find_features(ctx: ServiceContext,
                    collection_name: str,
                    query_geometry: shapely.geometry.base.BaseGeometry = None,
                    query_expr: Any = None,
                    comb_op: str = "and") -> List[GeoJsonFeature]:
    features = ctx.get_features(collection_name)
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
