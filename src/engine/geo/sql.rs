//! SQL surface for the spatial type: the `ST_*`, `MBR*` and `Point` function
//! library, MySQL 8.0 semantics. The expression evaluator routes a geometry
//! function here with its arguments already evaluated to [`Value`]s; this
//! module validates arity, propagates NULL, parses/serialises geometries via
//! the codec, and computes results with the [`super::algo`] / [`super::relate`]
//! kernels.

use crate::types::{ColumnType, QueryError, Value};

use super::algo;
use super::relate::{mbr_predicate, st_predicate, MbrOp, StOp};
use super::types::*;
use super::{
    geo_err, invalid_gis_data, parse_wkt, read_geometry, read_wkb, srid_mismatch, to_geojson,
    write_geometry, write_wkb, write_wkt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpatialNullPolicy {
    AnyNull,
    Evaluate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpatialResultClass {
    Geometry,
    Double,
    Integer,
    Blob,
    Text,
    Json,
    String,
    XAccessor,
    SridAccessor,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SpatialFunctionSpec {
    pub canonical_id: &'static str,
    pub sql_name: &'static str,
    pub min_args: u8,
    pub max_args: u8,
    pub null_policy: SpatialNullPolicy,
    pub result: SpatialResultClass,
    pub protocol_flags: u16,
    pub protocol_collation: u16,
}

impl SpatialFunctionSpec {
    fn result_type(self, arity: usize) -> ColumnType {
        match self.result {
            SpatialResultClass::Geometry => ColumnType::Geometry,
            SpatialResultClass::Double => ColumnType::Double,
            SpatialResultClass::Integer => ColumnType::LongLong,
            SpatialResultClass::Blob | SpatialResultClass::Text => ColumnType::Blob,
            SpatialResultClass::Json => ColumnType::Json,
            SpatialResultClass::String => ColumnType::VarString,
            SpatialResultClass::XAccessor => {
                if arity == 2 {
                    ColumnType::Geometry
                } else {
                    ColumnType::Double
                }
            }
            SpatialResultClass::SridAccessor => {
                if arity == 2 {
                    ColumnType::Geometry
                } else {
                    ColumnType::LongLong
                }
            }
        }
    }
}

/// Single source of truth shared by parser routing, both expression
/// evaluators and result-column metadata. The canonical id folds MySQL's
/// documented aliases without removing the original SQL spelling needed in
/// diagnostics.
// Keep aliases, arity, NULL handling and result metadata together. Expanding
// to a match avoids linear name/result-table scans on every spatial call.
macro_rules! spatial_functions {
    ($($canonical:literal $(| $alias:literal)* => ($min:expr, $max:expr, $null:ident, $result:ident);)*) => {
        pub(crate) fn function_spec(upper: &str) -> Option<SpatialFunctionSpec> {
            match upper {
                $($canonical $(| $alias)* => Some(SpatialFunctionSpec {
                    canonical_id: $canonical,
                    sql_name: match upper {
                        $canonical => $canonical,
                        $($alias => $alias,)*
                        _ => unreachable!(),
                    },
                    min_args: $min,
                    max_args: $max,
                    null_policy: SpatialNullPolicy::$null,
                    result: SpatialResultClass::$result,
                    protocol_flags: 0,
                    protocol_collation: 63,
                }),)*
                _ => None,
            }
        }

        #[cfg(test)]
        const GEO_FUNCTIONS: &[&str] = &[$($canonical, $($alias,)*)*];
    };
}

spatial_functions! {
    "ST_GEOMFROMTEXT" | "ST_GEOMETRYFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_GEOMCOLLFROMTEXT" | "ST_GEOMCOLLFROMTXT" | "ST_GEOMETRYCOLLECTIONFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_POINTFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_LINEFROMTEXT" | "ST_LINESTRINGFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_POLYFROMTEXT" | "ST_POLYGONFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_MPOINTFROMTEXT" | "ST_MULTIPOINTFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_MLINEFROMTEXT" | "ST_MULTILINESTRINGFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_MPOLYFROMTEXT" | "ST_MULTIPOLYGONFROMTEXT" => (1, 3, AnyNull, Geometry);
    "ST_GEOMFROMWKB" | "ST_GEOMETRYFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_POINTFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_LINEFROMWKB" | "ST_LINESTRINGFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_POLYFROMWKB" | "ST_POLYGONFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_MPOINTFROMWKB" | "ST_MULTIPOINTFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_MLINEFROMWKB" | "ST_MULTILINESTRINGFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_MPOLYFROMWKB" | "ST_MULTIPOLYGONFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_GEOMCOLLFROMWKB" | "ST_GEOMETRYCOLLECTIONFROMWKB" => (1, 3, AnyNull, Geometry);
    "ST_GEOMFROMGEOJSON" => (1, 3, AnyNull, Geometry);
    "POINT" => (2, 2, AnyNull, Geometry);
    "LINESTRING" => (1, u8::MAX, Evaluate, Geometry);
    "POLYGON" => (1, u8::MAX, Evaluate, Geometry);
    "MULTIPOINT" => (1, u8::MAX, Evaluate, Geometry);
    "MULTILINESTRING" => (1, u8::MAX, Evaluate, Geometry);
    "MULTIPOLYGON" => (1, u8::MAX, Evaluate, Geometry);
    "GEOMCOLLECTION" | "GEOMETRYCOLLECTION" => (0, u8::MAX, Evaluate, Geometry);
    "ST_ASTEXT" | "ST_ASWKT" => (1, 2, AnyNull, Text);
    "ST_ASBINARY" | "ST_ASWKB" => (1, 2, AnyNull, Blob);
    "ST_ASGEOJSON" => (1, 3, AnyNull, Json);
    "ST_GEOMETRYTYPE" => (1, 1, AnyNull, String);
    "ST_DIMENSION" => (1, 1, AnyNull, Integer);
    "ST_SRID" => (1, 2, AnyNull, SridAccessor);
    "ST_ISEMPTY" => (1, 1, AnyNull, Integer);
    "ST_ISSIMPLE" => (1, 1, AnyNull, Integer);
    "ST_ISVALID" => (1, 1, AnyNull, Integer);
    "ST_ISCLOSED" => (1, 1, AnyNull, Integer);
    "ST_X" => (1, 2, AnyNull, XAccessor);
    "ST_Y" => (1, 2, AnyNull, XAccessor);
    "ST_LATITUDE" => (1, 2, AnyNull, Double);
    "ST_LONGITUDE" => (1, 2, AnyNull, Double);
    "ST_STARTPOINT" => (1, 1, AnyNull, Geometry);
    "ST_ENDPOINT" => (1, 1, AnyNull, Geometry);
    "ST_POINTN" => (2, 2, AnyNull, Geometry);
    "ST_NUMPOINTS" => (1, 1, AnyNull, Integer);
    "ST_NUMGEOMETRIES" => (1, 1, AnyNull, Integer);
    "ST_GEOMETRYN" => (2, 2, AnyNull, Geometry);
    "ST_NUMINTERIORRINGS" | "ST_NUMINTERIORRING" => (1, 1, AnyNull, Integer);
    "ST_INTERIORRINGN" => (2, 2, AnyNull, Geometry);
    "ST_EXTERIORRING" => (1, 1, AnyNull, Geometry);
    "ST_AREA" => (1, 1, AnyNull, Double);
    "ST_LENGTH" => (1, 2, AnyNull, Double);
    "ST_DISTANCE" => (2, 3, AnyNull, Double);
    "ST_DISTANCE_SPHERE" => (2, 3, AnyNull, Double);
    "ST_FRECHETDISTANCE" => (2, 2, AnyNull, Double);
    "ST_HAUSDORFFDISTANCE" => (2, 2, AnyNull, Double);
    "ST_CENTROID" => (1, 1, AnyNull, Geometry);
    "ST_ENVELOPE" => (1, 1, AnyNull, Geometry);
    "ST_CONVEXHULL" => (1, 1, AnyNull, Geometry);
    "ST_MAKEENVELOPE" => (2, 2, AnyNull, Geometry);
    "ST_SWAPXY" => (1, 1, AnyNull, Geometry);
    "ST_VALIDATE" => (1, 1, AnyNull, Geometry);
    "ST_LINEINTERPOLATEPOINT" => (2, 2, AnyNull, Geometry);
    "ST_LINEINTERPOLATEPOINTS" => (2, 2, AnyNull, Geometry);
    "ST_POINTATDISTANCE" => (2, 2, AnyNull, Geometry);
    "ST_SIMPLIFY" => (2, 2, AnyNull, Geometry);
    "ST_TRANSFORM" => (2, 3, AnyNull, Geometry);
    "MBRCONTAINS" => (2, 2, AnyNull, Integer);
    "MBRWITHIN" => (2, 2, AnyNull, Integer);
    "MBRINTERSECTS" => (2, 2, AnyNull, Integer);
    "MBRDISJOINT" => (2, 2, AnyNull, Integer);
    "MBREQUALS" => (2, 2, AnyNull, Integer);
    "MBRTOUCHES" => (2, 2, AnyNull, Integer);
    "MBROVERLAPS" => (2, 2, AnyNull, Integer);
    "MBRCOVEREDBY" => (2, 2, AnyNull, Integer);
    "MBRCOVERS" => (2, 2, AnyNull, Integer);
    "ST_INTERSECTS" => (2, 2, AnyNull, Integer);
    "ST_DISJOINT" => (2, 2, AnyNull, Integer);
    "ST_CONTAINS" => (2, 2, AnyNull, Integer);
    "ST_WITHIN" => (2, 2, AnyNull, Integer);
    "ST_EQUALS" => (2, 2, AnyNull, Integer);
    "ST_TOUCHES" => (2, 2, AnyNull, Integer);
    "ST_CROSSES" => (2, 2, AnyNull, Integer);
    "ST_OVERLAPS" => (2, 2, AnyNull, Integer);
    "ST_GEOHASH" => (2, 3, AnyNull, String);
    "ST_LATFROMGEOHASH" => (1, 1, AnyNull, Double);
    "ST_LONGFROMGEOHASH" => (1, 1, AnyNull, Double);
    "ST_POINTFROMGEOHASH" => (1, 2, AnyNull, Geometry);
    "ST_BUFFER_STRATEGY" => (1, 3, AnyNull, Blob);
    "ST_COLLECT" => (1, 1, AnyNull, Geometry);
    "ST_UNION" => (2, 2, AnyNull, Geometry);
    "ST_INTERSECTION" => (2, 2, AnyNull, Geometry);
    "ST_DIFFERENCE" => (2, 2, AnyNull, Geometry);
    "ST_SYMDIFFERENCE" => (2, 2, AnyNull, Geometry);
    "ST_BUFFER" => (2, u8::MAX, AnyNull, Geometry);
}

/// True when `upper` (an already upper-cased function name) is a spatial
/// function handled by [`eval`].
pub(crate) fn is_geo_function(upper: &str) -> bool {
    function_spec(upper).is_some()
}

/// Result metadata for a spatial call. Setter overloads of ST_X/ST_Y/ST_SRID
/// return a geometry while their accessor overloads return numeric scalars,
/// so arity is part of the compatibility contract.
pub(crate) fn result_type(upper: &str, arity: usize) -> Option<ColumnType> {
    function_spec(upper).map(|spec| {
        debug_assert_eq!(spec.protocol_flags, 0);
        debug_assert_eq!(spec.protocol_collation, 63);
        spec.result_type(arity)
    })
}

pub(crate) fn is_geometry_collection_constructor(name: &str) -> bool {
    [
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
        "GEOMCOLLECTION",
    ]
    .iter()
    .any(|constructor| name.eq_ignore_ascii_case(constructor))
}

/// SQL NULL retains its bind-time geometry type. Untyped NULL and strings
/// remain invalid constructor arguments, even beside a typed geometry NULL.
pub(crate) fn eval_geometry_constructor(
    name: &str,
    values: &[Value],
    geometry_arguments: bool,
) -> Result<Value, QueryError> {
    let upper = name.to_ascii_uppercase();
    let spec = function_spec(&upper).expect("bound geometry constructor");
    if values.len() < usize::from(spec.min_args) || values.len() > usize::from(spec.max_args) {
        return Err(wrong_count(name));
    }
    if geometry_arguments && values.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    eval(&upper, name, values)
}

/// Evaluate a spatial function. `values` are the already-evaluated argument
/// values. NULL in any argument yields SQL NULL for every spatial function.
pub(crate) fn eval(upper: &str, name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let Some(spec) = function_spec(upper) else {
        return Err(geo_err(
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!("spatial function '{name}' is not supported"),
        ));
    };
    debug_assert_eq!(spec.sql_name, upper);
    if values.len() < usize::from(spec.min_args) || values.len() > usize::from(spec.max_args) {
        return Err(wrong_count(name));
    }
    match spec.null_policy {
        SpatialNullPolicy::AnyNull if values.iter().any(|v| matches!(v, Value::Null)) => {
            return Ok(Value::Null);
        }
        SpatialNullPolicy::AnyNull => {}
        SpatialNullPolicy::Evaluate => {}
    }
    match spec.canonical_id {
        // ---- constructors ----
        "ST_GEOMFROMTEXT" => from_text(name, values, None),
        "ST_POINTFROMTEXT" => from_text(name, values, Some(WKB_POINT)),
        "ST_LINEFROMTEXT" => from_text(name, values, Some(WKB_LINESTRING)),
        "ST_POLYFROMTEXT" => from_text(name, values, Some(WKB_POLYGON)),
        "ST_MPOINTFROMTEXT" => from_text(name, values, Some(WKB_MULTIPOINT)),
        "ST_MLINEFROMTEXT" => from_text(name, values, Some(WKB_MULTILINESTRING)),
        "ST_MPOLYFROMTEXT" => from_text(name, values, Some(WKB_MULTIPOLYGON)),
        "ST_GEOMCOLLFROMTEXT" => from_text(name, values, Some(WKB_GEOMETRYCOLLECTION)),
        "ST_GEOMFROMWKB" => from_wkb(name, values, None),
        "ST_POINTFROMWKB" => from_wkb(name, values, Some(WKB_POINT)),
        "ST_LINEFROMWKB" => from_wkb(name, values, Some(WKB_LINESTRING)),
        "ST_POLYFROMWKB" => from_wkb(name, values, Some(WKB_POLYGON)),
        "ST_MPOINTFROMWKB" => from_wkb(name, values, Some(WKB_MULTIPOINT)),
        "ST_MLINEFROMWKB" => from_wkb(name, values, Some(WKB_MULTILINESTRING)),
        "ST_MPOLYFROMWKB" => from_wkb(name, values, Some(WKB_MULTIPOLYGON)),
        "ST_GEOMCOLLFROMWKB" => from_wkb(name, values, Some(WKB_GEOMETRYCOLLECTION)),
        "ST_GEOMFROMGEOJSON" => geom_from_geojson(name, values),
        "POINT" => point_ctor(name, values),
        "LINESTRING" => typed_collection_ctor(name, values, WKB_LINESTRING),
        "POLYGON" => typed_collection_ctor(name, values, WKB_POLYGON),
        "MULTIPOINT" => typed_collection_ctor(name, values, WKB_MULTIPOINT),
        "MULTILINESTRING" => typed_collection_ctor(name, values, WKB_MULTILINESTRING),
        "MULTIPOLYGON" => typed_collection_ctor(name, values, WKB_MULTIPOLYGON),
        "GEOMCOLLECTION" => typed_collection_ctor(name, values, WKB_GEOMETRYCOLLECTION),

        // ---- format ----
        "ST_ASTEXT" => {
            let g = one_geom(name, values)?;
            Ok(Value::String(write_wkt(&g.geom)))
        }
        "ST_ASBINARY" => {
            let g = one_geom(name, values)?;
            Ok(Value::Bytes(write_wkb(&g.geom)))
        }
        "ST_ASGEOJSON" => {
            if values.len() > 1 {
                return Err(geo_err(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "ST_AsGeoJSON precision/options are not supported",
                ));
            }
            check_range(name, values, 1, 3)?;
            let g = geom_value(&values[0], name)?;
            let mut geojson_geom = g.geom.clone();
            if matches!(
                super::srs::require(g.srid, name)?.axis_order,
                super::srs::AxisOrder::LatLong
            ) {
                // RFC 7946 coordinate arrays are longitude/latitude even
                // when the SRS definition stores latitude/longitude axes.
                geojson_geom.for_each_coord_mut(&mut |coord| {
                    std::mem::swap(&mut coord.x, &mut coord.y);
                });
            }
            let text = to_geojson(&geojson_geom);
            // MySQL returns a JSON document.
            Ok(Value::Json(
                crate::engine::json::parse_text_opt(&text).unwrap_or_default(),
            ))
        }

        // ---- properties ----
        "ST_GEOMETRYTYPE" => {
            let g = one_geom(name, values)?;
            Ok(Value::String(g.geom.type_name().to_string()))
        }
        "ST_DIMENSION" => {
            let g = one_geom(name, values)?;
            Ok(g.geom
                .dimension()
                .map(|d| Value::Int(d as i64))
                .unwrap_or(Value::Null))
        }
        "ST_SRID" => st_srid(name, values),
        "ST_ISEMPTY" => {
            let g = one_geom(name, values)?;
            Ok(Value::Int(g.geom.is_empty() as i64))
        }
        "ST_ISSIMPLE" => {
            let g = one_geom(name, values)?;
            Ok(Value::Int(is_simple(&g.geom) as i64))
        }
        "ST_ISVALID" => {
            let g = one_geom(name, values)?;
            Ok(Value::Int(is_valid(&g.geom) as i64))
        }
        "ST_ISCLOSED" => {
            let g = one_geom(name, values)?;
            if matches!(g.geom, Geom::LineString(_) | Geom::MultiLineString(_)) {
                Ok(Value::Int(is_closed(&g.geom) as i64))
            } else {
                Ok(Value::Null)
            }
        }
        "ST_X" => xy(name, values, true),
        "ST_Y" => xy(name, values, false),
        "ST_LONGITUDE" => xy(name, values, true),
        "ST_LATITUDE" => xy(name, values, false),

        // ---- accessors ----
        "ST_STARTPOINT" => endpoint(name, values, true),
        "ST_ENDPOINT" => endpoint(name, values, false),
        "ST_POINTN" => point_n(name, values),
        "ST_NUMPOINTS" => num_points(name, values),
        "ST_NUMGEOMETRIES" => {
            let g = one_geom(name, values)?;
            Ok(Value::Int(g.geom.num_geometries() as i64))
        }
        "ST_GEOMETRYN" => geometry_n(name, values),
        "ST_NUMINTERIORRINGS" => num_interior_rings(name, values),
        "ST_INTERIORRINGN" => interior_ring_n(name, values),
        "ST_EXTERIORRING" => exterior_ring(name, values),

        // ---- measurement ----
        "ST_AREA" => st_area(name, values),
        "ST_LENGTH" => st_length(name, values),
        "ST_DISTANCE" => st_distance(name, values),
        "ST_DISTANCE_SPHERE" => st_distance_sphere(name, values),
        "ST_FRECHETDISTANCE" => discrete_curve_distance(name, values, true),
        "ST_HAUSDORFFDISTANCE" => discrete_curve_distance(name, values, false),

        // ---- construction ----
        "ST_CENTROID" => {
            let g = one_geom(name, values)?;
            match algo::centroid(&g.geom) {
                Some(c) => Ok(geom_to_value(Geometry::new(g.srid, Geom::Point(c)))),
                None => Ok(Value::Null),
            }
        }
        "ST_ENVELOPE" => {
            let g = one_geom(name, values)?;
            match algo::envelope(&g.geom) {
                Some(geom) => Ok(geom_to_value(Geometry::new(g.srid, geom))),
                None => Ok(Value::Null),
            }
        }
        "ST_CONVEXHULL" => {
            let g = one_geom(name, values)?;
            match algo::convex_hull(&g.geom) {
                Some(geom) => Ok(geom_to_value(Geometry::new(g.srid, geom))),
                None => Ok(Value::Null),
            }
        }
        "ST_MAKEENVELOPE" => make_envelope(name, values),
        "ST_SWAPXY" => swap_xy(name, values),
        "ST_VALIDATE" => validate_geometry(name, values),
        "ST_LINEINTERPOLATEPOINT" => line_interpolate(name, values, false, false),
        "ST_LINEINTERPOLATEPOINTS" => line_interpolate(name, values, true, false),
        "ST_POINTATDISTANCE" => line_interpolate(name, values, false, true),
        "ST_SIMPLIFY" => simplify_geometry(name, values),
        "ST_TRANSFORM" => transform_geometry(name, values),

        // ---- MBR predicates ----
        "MBRCONTAINS" => mbr_pred(name, values, MbrOp::Contains),
        "MBRWITHIN" => mbr_pred(name, values, MbrOp::Within),
        "MBRINTERSECTS" => mbr_pred(name, values, MbrOp::Intersects),
        "MBRDISJOINT" => mbr_pred(name, values, MbrOp::Disjoint),
        "MBREQUALS" => mbr_pred(name, values, MbrOp::Equals),
        "MBRTOUCHES" => mbr_pred(name, values, MbrOp::Touches),
        "MBROVERLAPS" => mbr_pred(name, values, MbrOp::Overlaps),
        "MBRCOVEREDBY" => mbr_pred(name, values, MbrOp::CoveredBy),
        "MBRCOVERS" => mbr_pred(name, values, MbrOp::Covers),

        // ---- topological predicates ----
        "ST_INTERSECTS" => st_pred(name, values, StOp::Intersects),
        "ST_DISJOINT" => st_pred(name, values, StOp::Disjoint),
        "ST_CONTAINS" => st_pred(name, values, StOp::Contains),
        "ST_WITHIN" => st_pred(name, values, StOp::Within),
        "ST_EQUALS" => st_pred(name, values, StOp::Equals),
        "ST_TOUCHES" => st_pred(name, values, StOp::Touches),
        "ST_CROSSES" => st_pred(name, values, StOp::Crosses),
        "ST_OVERLAPS" => st_pred(name, values, StOp::Overlaps),

        // ---- GeoHash encode / decode ----
        "ST_GEOHASH" => st_geohash(name, values),
        "ST_LATFROMGEOHASH" => st_from_geohash(name, values, false),
        "ST_LONGFROMGEOHASH" => st_from_geohash(name, values, true),
        "ST_POINTFROMGEOHASH" => st_point_from_geohash(name, values),

        // ---- constructive set ops ----
        "ST_UNION" | "ST_INTERSECTION" | "ST_DIFFERENCE" | "ST_SYMDIFFERENCE" | "ST_BUFFER" => {
            constructive_operation_unsupported(name)
        }
        "ST_BUFFER_STRATEGY" => st_buffer_strategy(name, values),
        "ST_COLLECT" => Err(geo_err(
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!("spatial function {name} (constructive set operation) is not yet supported"),
        )),

        _ => Err(geo_err(
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!("spatial function '{name}' is not supported"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

fn wrong_count(name: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT,
        format!("incorrect parameter count in the call to native function '{name}'"),
    )
}

// ---------------------------------------------------------------------------
// GeoHash (MySQL 8.0 ST_GeoHash / ST_LatFromGeoHash / ST_LongFromGeoHash)
// ---------------------------------------------------------------------------

const GEOHASH_BASE32: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Standard geohash encoding: interleave longitude/latitude bits (longitude
/// first) into 5-bit base32 symbols, producing exactly `len` characters.
fn geohash_encode(lon: f64, lat: f64, len: usize) -> String {
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-90.0f64, 90.0f64);
    let mut hash = String::with_capacity(len);
    let mut even = true; // longitude on even bits
    let mut bit = 0usize;
    let mut ch = 0u8;
    while hash.len() < len {
        if even {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                ch |= 1 << (4 - bit);
                lon_range.0 = mid;
            } else {
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                ch |= 1 << (4 - bit);
                lat_range.0 = mid;
            } else {
                lat_range.1 = mid;
            }
        }
        even = !even;
        if bit < 4 {
            bit += 1;
        } else {
            hash.push(GEOHASH_BASE32[ch as usize] as char);
            bit = 0;
            ch = 0;
        }
    }
    hash
}

/// Decode a geohash to the centre (longitude, latitude) of its cell.
fn geohash_decode(hash: &str) -> Option<(f64, f64)> {
    let mut lon_range = (-180.0f64, 180.0f64);
    let mut lat_range = (-90.0f64, 90.0f64);
    let mut even = true;
    for c in hash.chars() {
        let lc = c.to_ascii_lowercase() as u8;
        let cd = GEOHASH_BASE32.iter().position(|&b| b == lc)?;
        for i in 0..5 {
            let bit = (cd >> (4 - i)) & 1;
            if even {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if bit == 1 {
                    lon_range.0 = mid;
                } else {
                    lon_range.1 = mid;
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if bit == 1 {
                    lat_range.0 = mid;
                } else {
                    lat_range.1 = mid;
                }
            }
            even = !even;
        }
    }
    Some((
        (lon_range.0 + lon_range.1) / 2.0,
        (lat_range.0 + lat_range.1) / 2.0,
    ))
}

/// `ST_GeoHash(longitude, latitude, max_length)` or
/// `ST_GeoHash(point, max_length)`.
fn st_geohash(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_range(name, values, 2, 3)?;
    let (lon, lat, len_idx) = if values.len() == 3 {
        (as_f64(&values[0])?, as_f64(&values[1])?, 2)
    } else {
        let g = geom_value(&values[0], name)?;
        let Geom::Point(c) = g.geom else {
            return Err(geo_err(
                crate::error::catalog::ER_SRS_NOT_GEOGRAPHIC,
                format!("function {name} is only defined for POINT values."),
            ));
        };
        (c.x, c.y, 1)
    };
    let max_len = as_i64(&values[len_idx])?;
    if !(1..=100).contains(&max_len) {
        return Err(geo_err(
            crate::error::catalog::ER_DATA_OUT_OF_RANGE,
            format!("max geohash length value is out of range in '{name}'"),
        ));
    }
    if !(-180.0..=180.0).contains(&lon) {
        return Err(geo_err(crate::error::catalog::ER_LONGITUDE_OUT_OF_RANGE,
            format!(
                "Longitude {lon:.6} is out of range in function {name}. It must be within [-180.000000, 180.000000]."
            ),
        ));
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(geo_err(crate::error::catalog::ER_LATITUDE_OUT_OF_RANGE,
            format!(
                "Latitude {lat:.6} is out of range in function {name}. It must be within [-90.000000, 90.000000]."
            ),
        ));
    }
    Ok(Value::String(geohash_encode(lon, lat, max_len as usize)))
}

/// `ST_LatFromGeoHash(geohash)` (want_lon=false) /
/// `ST_LongFromGeoHash(geohash)` (want_lon=true).
fn st_from_geohash(name: &str, values: &[Value], want_lon: bool) -> Result<Value, QueryError> {
    check_count(name, values, 1)?;
    let hash = text_of(&values[0]);
    if hash.is_empty()
        || !hash
            .bytes()
            .all(|b| GEOHASH_BASE32.contains(&b.to_ascii_lowercase()))
    {
        return Err(geo_err(
            crate::error::catalog::ER_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS,
            format!("Incorrect arguments to {name} (not a valid geohash string)"),
        ));
    }
    let (lon, lat) = geohash_decode(&hash).ok_or_else(|| {
        geo_err(
            crate::error::catalog::ER_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS,
            format!("Incorrect arguments to {name} (not a valid geohash string)"),
        )
    })?;
    Ok(Value::Double(if want_lon { lon } else { lat }))
}

fn st_point_from_geohash(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let hash = text_of(&values[0]);
    let (lon, lat) = geohash_decode(&hash).ok_or_else(|| {
        geo_err(
            crate::error::catalog::ER_WRONG_ARGUMENTS,
            format!("Incorrect arguments to {name} (not a valid geohash string)"),
        )
    })?;
    let srid = as_srid(&values[1], name)?;
    Ok(geom_to_value(Geometry::new(
        srid,
        Geom::Point(Coord { x: lon, y: lat }),
    )))
}

fn check_count(name: &str, values: &[Value], n: usize) -> Result<(), QueryError> {
    if values.len() == n {
        Ok(())
    } else {
        Err(wrong_count(name))
    }
}

fn check_range(name: &str, values: &[Value], lo: usize, hi: usize) -> Result<(), QueryError> {
    if values.len() >= lo && values.len() <= hi {
        Ok(())
    } else {
        Err(wrong_count(name))
    }
}

/// Decode an argument that must be a geometry (column value, constructor
/// output, or internal/WKB bytes).
fn geom_value(v: &Value, func: &str) -> Result<Geometry, QueryError> {
    let geometry = match v {
        Value::Geometry(b) => read_geometry(b),
        Value::Bytes(b) => read_geometry(b),
        _ => Err(invalid_gis_data(func)),
    }?;
    validate_geometry_srs(&geometry, func)?;
    Ok(geometry)
}

fn validate_geometry_srs(geometry: &Geometry, function: &str) -> Result<(), QueryError> {
    let srs = super::srs::require(geometry.srid, function)?;
    if srs.kind != super::srs::SrsKind::Geographic {
        // Reading the projected system's native linear unit here keeps the
        // parsed SRS descriptor authoritative for measurement overloads.
        let _native_metres_per_unit = srs
            .linear_unit
            .map(|unit| unit.metres_per_unit)
            .unwrap_or(1.0);
        return Ok(());
    }
    let radians_per_unit = srs
        .angular_unit
        .map(|unit| unit.radians_per_unit)
        .unwrap_or(std::f64::consts::PI / 180.0);
    let half_turn = std::f64::consts::PI / radians_per_unit;
    let quarter_turn = std::f64::consts::FRAC_PI_2 / radians_per_unit;
    let mut violation = None;
    geometry.geom.for_each_coord(&mut |coord| {
        if violation.is_some() {
            return;
        }
        let (latitude, longitude) = match srs.axis_order {
            super::srs::AxisOrder::LatLong => (coord.x, coord.y),
            super::srs::AxisOrder::LongLat | super::srs::AxisOrder::Xy => (coord.y, coord.x),
        };
        if !(-half_turn..=half_turn).contains(&longitude) {
            violation = Some(geo_err(
                crate::error::catalog::ER_GEOMETRY_PARAM_LONGITUDE_OUT_OF_RANGE,
                format!(
                    "Longitude {longitude} is out of range in function {function}. It must be within (-{half_turn}, {half_turn}]."
                ),
            ));
        } else if !(-quarter_turn..=quarter_turn).contains(&latitude) {
            violation = Some(geo_err(
                crate::error::catalog::ER_GEOMETRY_PARAM_LATITUDE_OUT_OF_RANGE,
                format!(
                    "Latitude {latitude} is out of range in function {function}. It must be within [-{quarter_turn}, {quarter_turn}]."
                ),
            ));
        }
    });
    violation.map_or(Ok(()), Err)
}

/// One-geometry-argument convenience.
fn one_geom(name: &str, values: &[Value]) -> Result<Geometry, QueryError> {
    check_count(name, values, 1)?;
    geom_value(&values[0], name)
}

fn two_geoms(name: &str, values: &[Value]) -> Result<(Geometry, Geometry), QueryError> {
    check_count(name, values, 2)?;
    Ok((geom_value(&values[0], name)?, geom_value(&values[1], name)?))
}

fn text_of(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => other.to_display_string(),
    }
}

fn as_i64(v: &Value) -> Result<i64, QueryError> {
    crate::engine::expr::int_value(v)
}

fn as_f64(v: &Value) -> Result<f64, QueryError> {
    crate::engine::expr::numeric(v)
}

fn as_srid(v: &Value, function: &str) -> Result<u32, QueryError> {
    if matches!(
        v,
        Value::Decimal(_)
            | Value::DecimalCompact(..)
            | Value::Decimal128(..)
            | Value::DecimalWide(..)
    ) {
        if let Some(exact) = v.exact_decimal() {
            use num_traits::ToPrimitive;
            let integer = crate::util::decimal::rescale_exact(exact, 0, false)
                .into_big_parts()
                .0;
            if integer.to_u32().is_none() {
                return Err(geo_err(
                    crate::error::catalog::ER_DATA_OUT_OF_RANGE,
                    "SRID value is out of range",
                ));
            }
        }
    }
    if matches!(v, Value::UInt(value) if *value > u32::MAX as u64) {
        return Err(geo_err(
            crate::error::catalog::ER_DATA_OUT_OF_RANGE,
            "SRID value is out of range",
        ));
    }
    let s = crate::engine::expr::int_value(v)?;
    let srid = u32::try_from(s).map_err(|_| {
        geo_err(
            crate::error::catalog::ER_SRS_PARSE_ERROR,
            "the SRID value must be between 0 and 2^32-1",
        )
    })?;
    super::srs::require(srid, function)?;
    Ok(srid)
}

fn geom_to_value(g: Geometry) -> Value {
    Value::Geometry(write_geometry(&g))
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

fn apply_axis_option(
    name: &str,
    geom: &mut Geom,
    srid: u32,
    option: Option<&Value>,
) -> Result<(), QueryError> {
    let Some(option) = option else { return Ok(()) };
    let text = text_of(option);
    let text = text.trim().to_ascii_lowercase();
    let text = text.split_once('=').map_or_else(
        || text.clone(),
        |(key, value)| format!("{}={}", key.trim(), value.trim()),
    );
    let axis = match text.as_str() {
        "" | "axis-order=srid-defined" => return Ok(()),
        "axis-order=lat-long" => super::srs::AxisOrder::LatLong,
        "axis-order=long-lat" => super::srs::AxisOrder::LongLat,
        _ => return Err(buffer_strategy_arguments_error(name)),
    };
    let srs = super::srs::require(srid, name)?;
    if srs.kind == super::srs::SrsKind::Geographic && axis != srs.axis_order {
        geom.for_each_coord_mut(&mut |c| std::mem::swap(&mut c.x, &mut c.y));
    }
    Ok(())
}

fn from_text(name: &str, values: &[Value], require: Option<u32>) -> Result<Value, QueryError> {
    check_range(name, values, 1, 3)?;
    // MySQL coerces/validates the optional SRID before parsing WKT. This
    // ordering matters when both arguments are invalid: a numeric overflow is
    // reported instead of being masked by malformed geometry text.
    let srid = match values.get(1) {
        Some(v) => as_srid(v, name)?,
        None => 0,
    };
    let mut geom = parse_wkt(&text_of(&values[0]))?;
    apply_axis_option(name, &mut geom, srid, values.get(2))?;
    if let Some(code) = require {
        if geom.type_code() != code {
            return Err(geo_err(
                crate::error::catalog::ER_UNEXPECTED_GEOMETRY_TYPE,
                format!("Unexpected geometry type in function {name}."),
            ));
        }
    }
    Ok(geom_to_value(Geometry::new(srid, geom)))
}

fn from_wkb(name: &str, values: &[Value], require: Option<u32>) -> Result<Value, QueryError> {
    check_range(name, values, 1, 3)?;
    let srid = match values.get(1) {
        Some(v) => as_srid(v, name)?,
        None => 0,
    };
    let mut geom = match &values[0] {
        // ST_AsBinary output is bare WKB; a geometry value carries its WKB too.
        Value::Bytes(b) => read_wkb(b)?,
        Value::Geometry(b) => read_geometry(b)?.geom,
        other => read_wkb(text_of(other).as_bytes())?,
    };
    apply_axis_option(name, &mut geom, srid, values.get(2))?;
    if let Some(code) = require {
        if geom.type_code() != code {
            return Err(geo_err(
                crate::error::catalog::ER_UNEXPECTED_GEOMETRY_TYPE,
                format!("Unexpected geometry type in function {name}."),
            ));
        }
    }
    Ok(geom_to_value(Geometry::new(srid, geom)))
}

fn geom_from_geojson(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_range(name, values, 1, 3)?;
    // MySQL defaults the result SRID to 4326 for GeoJSON; an explicit SRID is
    // the 3rd argument.
    let srid = match values.get(2) {
        Some(v) => as_srid(v, name)?,
        None => 4326,
    };
    let option = values.get(1).map(as_i64).transpose()?.unwrap_or(1);
    if !(1..=4).contains(&option) {
        return Err(buffer_strategy_arguments_error(name));
    }
    let mut g = super::geojson::from_geojson_options(&text_of(&values[0]), srid, option != 1)?;
    if matches!(
        super::srs::require(srid, name)?.axis_order,
        super::srs::AxisOrder::LatLong
    ) {
        // GeoJSON is always longitude/latitude. Convert to the SRS-defined
        // axis order used by MySQL's internal/WKT representation.
        g.geom.for_each_coord_mut(&mut |coord| {
            std::mem::swap(&mut coord.x, &mut coord.y);
        });
    }
    Ok(geom_to_value(g))
}

fn point_ctor(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let x = as_f64(&values[0])?;
    let y = as_f64(&values[1])?;
    Ok(geom_to_value(Geometry::new(0, Geom::Point(Coord { x, y }))))
}

fn typed_collection_ctor(
    name: &str,
    values: &[Value],
    target_type: u32,
) -> Result<Value, QueryError> {
    let mut srid = None;
    let mut parts = Vec::with_capacity(values.len());
    for value in values {
        if !matches!(value, Value::Geometry(_)) {
            return Err(geo_err(
                crate::error::catalog::ER_ILLEGAL_VALUE_FOR_TYPE,
                format!("Illegal non geometric value found during parsing in function {name}"),
            ));
        }
        let geometry = geom_value(value, name)?;
        match srid {
            Some(expected) if expected != geometry.srid => {
                return Err(srid_mismatch(name, expected, geometry.srid))
            }
            None => srid = Some(geometry.srid),
            _ => {}
        }
        parts.push(geometry.geom);
    }
    let wrong_type = || {
        geo_err(
            crate::error::catalog::ER_GIS_INVALID_DATA,
            format!("Invalid GIS data provided to function {name}."),
        )
    };
    let geom = match target_type {
        WKB_LINESTRING => Geom::LineString(
            parts
                .into_iter()
                .map(|part| match part {
                    Geom::Point(coord) => Ok(coord),
                    _ => Err(wrong_type()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WKB_POLYGON => Geom::Polygon(
            parts
                .into_iter()
                .map(|part| match part {
                    Geom::LineString(line) => Ok(line),
                    _ => Err(wrong_type()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WKB_MULTIPOINT => Geom::MultiPoint(
            parts
                .into_iter()
                .map(|part| match part {
                    Geom::Point(coord) => Ok(coord),
                    _ => Err(wrong_type()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WKB_MULTILINESTRING => Geom::MultiLineString(
            parts
                .into_iter()
                .map(|part| match part {
                    Geom::LineString(line) => Ok(line),
                    _ => Err(wrong_type()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WKB_MULTIPOLYGON => Geom::MultiPolygon(
            parts
                .into_iter()
                .map(|part| match part {
                    Geom::Polygon(polygon) => Ok(polygon),
                    _ => Err(wrong_type()),
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        WKB_GEOMETRYCOLLECTION => Geom::GeometryCollection(parts),
        _ => return Err(wrong_type()),
    };
    Ok(geom_to_value(Geometry::new(srid.unwrap_or(0), geom)))
}

fn make_envelope(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let (left, right) = two_geoms(name, values)?;
    if left.srid != right.srid {
        return Err(srid_mismatch(name, left.srid, right.srid));
    }
    let (Geom::Point(a), Geom::Point(b)) = (left.geom, right.geom) else {
        return Err(invalid_gis_data(name));
    };
    let envelope =
        algo::envelope(&Geom::MultiPoint(vec![a, b])).ok_or_else(|| invalid_gis_data(name))?;
    Ok(geom_to_value(Geometry::new(left.srid, envelope)))
}

fn swap_xy(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    fn swap(geom: &mut Geom) {
        geom.for_each_coord_mut(&mut |coord| std::mem::swap(&mut coord.x, &mut coord.y));
    }

    let mut geometry = one_geom(name, values)?;
    swap(&mut geometry.geom);
    Ok(geom_to_value(geometry))
}

fn validate_geometry(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let geometry = one_geom(name, values)?;
    if is_valid(&geometry.geom) {
        Ok(geom_to_value(geometry))
    } else {
        Ok(Value::Null)
    }
}

// ---------------------------------------------------------------------------
// Properties / accessors
// ---------------------------------------------------------------------------

fn st_srid(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_range(name, values, 1, 2)?;
    let mut g = geom_value(&values[0], name)?;
    match values.get(1) {
        None => Ok(Value::Int(g.srid as i64)),
        Some(v) => {
            let target_srid = as_srid(v, name)?;
            let source_axis = super::srs::require(g.srid, name)?.axis_order;
            let target_axis = super::srs::require(target_srid, name)?.axis_order;
            let source_lat_long = matches!(source_axis, super::srs::AxisOrder::LatLong);
            let target_lat_long = matches!(target_axis, super::srs::AxisOrder::LatLong);
            if source_lat_long != target_lat_long {
                // MySQL preserves the semantic longitude/latitude pair when
                // assigning or removing a geographic SRID.  EPSG:4326 is
                // latitude/longitude ordered while SRID 0 is X/Y, so merely
                // replacing the id would reinterpret the ordinates.
                g.geom.for_each_coord_mut(&mut |coord| {
                    std::mem::swap(&mut coord.x, &mut coord.y);
                });
            }
            g.srid = target_srid;
            Ok(geom_to_value(g))
        }
    }
}

/// ST_X / ST_Y / ST_Longitude / ST_Latitude — `x` selects the axis.
fn xy(name: &str, values: &[Value], x_axis: bool) -> Result<Value, QueryError> {
    check_range(name, values, 1, 2)?;
    let g = geom_value(&values[0], name)?;
    let Geom::Point(c) = g.geom else {
        return Err(geo_err(
            crate::error::catalog::ER_SRS_NOT_GEOGRAPHIC,
            format!("function {name} is only defined for POINT values."),
        ));
    };
    let coordinate_is_x =
        if name.eq_ignore_ascii_case("ST_LATITUDE") || name.eq_ignore_ascii_case("ST_LONGITUDE") {
            let lat_long = matches!(
                super::srs::require(g.srid, name)?.axis_order,
                super::srs::AxisOrder::LatLong
            );
            if name.eq_ignore_ascii_case("ST_LATITUDE") {
                lat_long
            } else {
                !lat_long
            }
        } else {
            x_axis
        };
    match values.get(1) {
        None => Ok(Value::Double(if coordinate_is_x { c.x } else { c.y })),
        Some(v) => {
            let nv = as_f64(v)?;
            let newc = if coordinate_is_x {
                Coord { x: nv, y: c.y }
            } else {
                Coord { x: c.x, y: nv }
            };
            Ok(geom_to_value(Geometry::new(g.srid, Geom::Point(newc))))
        }
    }
}

fn endpoint(name: &str, values: &[Value], start: bool) -> Result<Value, QueryError> {
    let g = one_geom(name, values)?;
    let Geom::LineString(cs) = &g.geom else {
        return Ok(Value::Null);
    };
    if cs.is_empty() {
        return Ok(Value::Null);
    }
    let c = if start { cs[0] } else { cs[cs.len() - 1] };
    Ok(geom_to_value(Geometry::new(g.srid, Geom::Point(c))))
}

fn point_n(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let g = geom_value(&values[0], name)?;
    let n = as_i64(&values[1])?;
    let Geom::LineString(cs) = &g.geom else {
        return Ok(Value::Null);
    };
    if n < 1 || n as usize > cs.len() {
        return Ok(Value::Null);
    }
    Ok(geom_to_value(Geometry::new(
        g.srid,
        Geom::Point(cs[(n - 1) as usize]),
    )))
}

fn num_points(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let g = one_geom(name, values)?;
    match &g.geom {
        Geom::LineString(cs) => Ok(Value::Int(cs.len() as i64)),
        _ => Ok(Value::Null),
    }
}

fn geometry_n(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let g = geom_value(&values[0], name)?;
    let n = as_i64(&values[1])?;
    if n < 1 {
        return Ok(Value::Null);
    }
    let idx = (n - 1) as usize;
    let sub = match &g.geom {
        Geom::MultiPoint(ps) => ps.get(idx).map(|c| Geom::Point(*c)),
        Geom::MultiLineString(ls) => ls.get(idx).map(|l| Geom::LineString(l.clone())),
        Geom::MultiPolygon(ps) => ps.get(idx).map(|p| Geom::Polygon(p.clone())),
        Geom::GeometryCollection(gs) => gs.get(idx).cloned(),
        // A single geometry: ST_GeometryN(g, 1) yields the geometry itself.
        other => {
            if idx == 0 {
                Some(other.clone())
            } else {
                None
            }
        }
    };
    Ok(sub
        .map(|s| geom_to_value(Geometry::new(g.srid, s)))
        .unwrap_or(Value::Null))
}

fn num_interior_rings(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let g = one_geom(name, values)?;
    match &g.geom {
        Geom::Polygon(rings) => Ok(Value::Int((rings.len().saturating_sub(1)) as i64)),
        _ => Ok(Value::Null),
    }
}

fn interior_ring_n(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let g = geom_value(&values[0], name)?;
    let n = as_i64(&values[1])?;
    let Geom::Polygon(rings) = &g.geom else {
        return Ok(Value::Null);
    };
    // ring 0 is the exterior; interior rings are 1-based among the holes.
    if n < 1 || n as usize >= rings.len() {
        return Ok(Value::Null);
    }
    Ok(geom_to_value(Geometry::new(
        g.srid,
        Geom::LineString(rings[n as usize].clone()),
    )))
}

fn exterior_ring(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let g = one_geom(name, values)?;
    let Geom::Polygon(rings) = &g.geom else {
        return Ok(Value::Null);
    };
    match rings.first() {
        Some(r) => Ok(geom_to_value(Geometry::new(
            g.srid,
            Geom::LineString(r.clone()),
        ))),
        None => Ok(Value::Null),
    }
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

fn st_area(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 1)?;
    let geometry = geom_value(&values[0], name)?;
    let srs = super::srs::require(geometry.srid, name)?;
    reject_geographic_srs(name, srs)?;
    Ok(Value::Double(algo::area(&geometry.geom)))
}

fn st_length(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_range(name, values, 1, 2)?;
    let g = geom_value(&values[0], name)?;
    let srs = super::srs::require(g.srid, name)?;
    reject_geographic_srs(name, srs)?;
    let length = match &g.geom {
        Geom::LineString(_) | Geom::MultiLineString(_) | Geom::GeometryCollection(_) => {
            algo::length(&g.geom)
        }
        _ => return Ok(Value::Null),
    };
    let scale = measurement_unit_scale(name, srs, values.get(1))?;
    Ok(Value::Double(length * scale))
}

fn st_distance(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_range(name, values, 2, 3)?;
    let a = geom_value(&values[0], name)?;
    let b = geom_value(&values[1], name)?;
    if a.srid != b.srid {
        return Err(srid_mismatch(name, a.srid, b.srid));
    }
    let srs = super::srs::require(a.srid, name)?;
    reject_geographic_srs(name, srs)?;
    let distance = algo::distance(&a.geom, &b.geom);
    let Some(distance) = distance else {
        return Ok(Value::Null);
    };
    let scale = measurement_unit_scale(name, srs, values.get(2))?;
    Ok(Value::Double(distance * scale))
}

fn measurement_unit_scale(
    function: &str,
    srs: &super::srs::SrsDefinition,
    requested: Option<&Value>,
) -> Result<f64, QueryError> {
    let Some(requested) = requested else {
        return Ok(1.0);
    };
    if srs.id == 0 {
        return Err(geo_err(
            crate::error::catalog::ER_GEOMETRY_IN_UNKNOWN_LENGTH_UNIT,
            format!(
                "The geometry passed to function {} is in SRID 0, which doesn't specify a length \
                 unit. Can't convert to '{}'.",
                function.to_ascii_lowercase(),
                unit_argument_display(requested)
            ),
        ));
    }
    let name = match requested {
        Value::String(value) | Value::Decimal(value) => value.as_str(),
        Value::SmallString(value) => {
            std::str::from_utf8(value.as_slice()).map_err(|_| invalid_gis_data(function))?
        }
        Value::Bytes(value) => {
            std::str::from_utf8(value).map_err(|_| invalid_gis_data(function))?
        }
        _ => return Err(invalid_gis_data(function)),
    };
    let unit = super::srs::find_unit(name).ok_or_else(|| {
        geo_err(
            crate::error::catalog::ER_UNIT_NOT_FOUND,
            format!("There's no unit of measure named '{name}'."),
        )
    })?;
    let source_metres = match srs.kind {
        super::srs::SrsKind::Projected => srs
            .linear_unit
            .map(|unit| unit.metres_per_unit)
            .unwrap_or(1.0),
        // Geographic kernels in this module return metres.
        super::srs::SrsKind::Geographic => 1.0,
        super::srs::SrsKind::Cartesian => 1.0,
    };
    Ok(source_metres / unit.conversion_factor)
}

fn unit_argument_display(value: &Value) -> String {
    match value {
        Value::String(value) | Value::Decimal(value) => value.clone(),
        Value::SmallString(value) => String::from_utf8_lossy(value.as_slice()).into_owned(),
        Value::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        value => value.to_display_string(),
    }
}

fn reject_geographic_srs(name: &str, srs: &super::srs::SrsDefinition) -> Result<(), QueryError> {
    if srs.kind != super::srs::SrsKind::Geographic {
        return Ok(());
    }
    Err(geo_err(
        crate::error::catalog::ER_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS,
        format!("Function {name} is not implemented for geographic spatial reference systems."),
    ))
}

fn constructive_operation_unsupported(name: &str) -> Result<Value, QueryError> {
    Err(geo_err(
        crate::error::catalog::ER_SP_DOES_NOT_EXIST,
        format!("spatial function {name} (constructive set operation) is not yet supported"),
    ))
}

fn st_buffer_strategy(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    let strategy = unit_argument_display(
        values
            .first()
            .ok_or_else(|| buffer_strategy_arguments_error(name))?,
    )
    .to_ascii_lowercase();
    let (kind, parameter) = match strategy.as_str() {
        "end_flat" if values.len() == 1 => (2u32, 0.0),
        "end_round" | "join_round" | "join_miter" | "point_circle" if values.len() == 2 => {
            let parameter =
                as_f64(&values[1]).map_err(|_| buffer_strategy_arguments_error(name))?;
            if !parameter.is_finite() || parameter <= 0.0 {
                return Err(buffer_strategy_arguments_error(name));
            }
            let kind = match strategy.as_str() {
                "end_round" => 1,
                "join_round" => 3,
                "join_miter" => 4,
                "point_circle" => 5,
                _ => unreachable!(),
            };
            (kind, parameter)
        }
        _ => return Err(buffer_strategy_arguments_error(name)),
    };
    let mut encoded = Vec::with_capacity(12);
    encoded.extend_from_slice(&kind.to_le_bytes());
    encoded.extend_from_slice(&parameter.to_le_bytes());
    Ok(Value::Bytes(encoded))
}

fn buffer_strategy_arguments_error(name: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_WRONG_ARGUMENTS,
        format!("Incorrect arguments to {name}"),
    )
}

fn st_distance_sphere(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_range(name, values, 2, 3)?;
    let a = geom_value(&values[0], name)?;
    let b = geom_value(&values[1], name)?;
    if a.srid != b.srid {
        return Err(srid_mismatch(name, a.srid, b.srid));
    }
    let srs = super::srs::require(a.srid, name)?;
    if srs.kind == super::srs::SrsKind::Projected {
        return Err(geo_err(
            crate::error::catalog::ER_NOT_IMPLEMENTED_FOR_PROJECTED_SRS,
            format!("{name} does not support projected SRS"),
        ));
    }
    if !matches!(a.geom, Geom::Point(_) | Geom::MultiPoint(_))
        || !matches!(b.geom, Geom::Point(_) | Geom::MultiPoint(_))
    {
        return Err(geo_err(
            crate::error::catalog::ER_NOT_IMPLEMENTED_FOR_CARTESIAN_SRS,
            format!("{name} requires Point or MultiPoint"),
        ));
    }
    let normalize = |mut geom: Geom| -> Result<Geom, QueryError> {
        if srs.kind == super::srs::SrsKind::Geographic {
            let degrees = srs
                .angular_unit
                .map_or(1.0, |unit| unit.radians_per_unit.to_degrees());
            geom.for_each_coord_mut(&mut |c| {
                if srs.axis_order == super::srs::AxisOrder::LatLong {
                    std::mem::swap(&mut c.x, &mut c.y);
                }
                c.x *= degrees;
                c.y *= degrees;
            });
        }
        let mut valid = true;
        geom.for_each_coord(&mut |c| {
            valid &= c.x > -180.0 && c.x <= 180.0 && (-90.0..=90.0).contains(&c.y)
        });
        if !valid {
            return Err(buffer_strategy_arguments_error(name));
        }
        Ok(geom)
    };
    let a = normalize(a.geom)?;
    let b = normalize(b.geom)?;
    let radius = match values.get(2) {
        Some(v) => as_f64(v)?,
        None => algo::DEFAULT_SPHERE_RADIUS,
    };
    if !radius.is_finite() || radius <= 0.0 {
        return Err(geo_err(
            crate::error::catalog::ER_NONPOSITIVE_RADIUS,
            "the radius must be greater than zero.",
        ));
    }
    Ok(algo::distance_sphere(&a, &b, radius)
        .map(Value::Double)
        .unwrap_or(Value::Null))
}

fn curve_coords<'a>(geometry: &'a Geometry, name: &str) -> Result<&'a [Coord], QueryError> {
    match &geometry.geom {
        Geom::LineString(coords) => Ok(coords),
        _ => Err(geo_err(
            crate::error::catalog::ER_NOT_IMPLEMENTED_FOR_GEOGRAPHIC_SRS,
            format!("Function {name} is implemented only for LineString arguments."),
        )),
    }
}

fn coord_distance(left: Coord, right: Coord) -> f64 {
    (left.x - right.x).hypot(left.y - right.y)
}

fn discrete_curve_distance(
    name: &str,
    values: &[Value],
    frechet: bool,
) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let left = geom_value(&values[0], name)?;
    let right = geom_value(&values[1], name)?;
    if left.srid != right.srid {
        return Err(srid_mismatch(name, left.srid, right.srid));
    }
    let left = curve_coords(&left, name)?;
    let right = curve_coords(&right, name)?;
    if left.is_empty() || right.is_empty() {
        return Ok(Value::Null);
    }
    if !frechet {
        let directed = |a: &[Coord], b: &[Coord]| {
            a.iter()
                .map(|point| {
                    b.iter()
                        .map(|candidate| coord_distance(*point, *candidate))
                        .fold(f64::INFINITY, f64::min)
                })
                .fold(0.0, f64::max)
        };
        return Ok(Value::Double(
            directed(left, right).max(directed(right, left)),
        ));
    }

    let bytes = right
        .len()
        .saturating_mul(std::mem::size_of::<f64>())
        .saturating_mul(2);
    let _arena = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        bytes,
    )?;
    let mut previous: Vec<f64> = vec![0.0; right.len()];
    let mut current: Vec<f64> = vec![0.0; right.len()];
    for (i, left_point) in left.iter().enumerate() {
        for (j, right_point) in right.iter().enumerate() {
            let distance = coord_distance(*left_point, *right_point);
            current[j] = if i == 0 && j == 0 {
                distance
            } else if i == 0 {
                current[j - 1].max(distance)
            } else if j == 0 {
                previous[0].max(distance)
            } else {
                previous[j]
                    .min(previous[j - 1])
                    .min(current[j - 1])
                    .max(distance)
            };
        }
        std::mem::swap(&mut previous, &mut current);
    }
    Ok(Value::Double(previous[right.len() - 1]))
}

fn line_interpolate(
    name: &str,
    values: &[Value],
    repeated: bool,
    absolute_distance: bool,
) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let geometry = geom_value(&values[0], name)?;
    let coords = curve_coords(&geometry, name)?;
    if coords.len() < 2 {
        return Ok(Value::Null);
    }
    let total: f64 = coords
        .windows(2)
        .map(|segment| coord_distance(segment[0], segment[1]))
        .sum();
    let argument = as_f64(&values[1])?;
    let step = if absolute_distance {
        argument
    } else {
        argument * total
    };
    if !step.is_finite()
        || step < 0.0
        || (!absolute_distance && argument > 1.0)
        || (absolute_distance && step > total)
    {
        return Err(geo_err(
            crate::error::catalog::ER_DATA_OUT_OF_RANGE,
            format!("The interpolation argument is out of range in function {name}."),
        ));
    }

    let point_at = |target: f64| {
        let mut traversed = 0.0;
        for segment in coords.windows(2) {
            let length = coord_distance(segment[0], segment[1]);
            if target <= traversed + length || length == 0.0 {
                let ratio = if length == 0.0 {
                    0.0
                } else {
                    ((target - traversed) / length).clamp(0.0, 1.0)
                };
                return Coord {
                    x: segment[0].x + (segment[1].x - segment[0].x) * ratio,
                    y: segment[0].y + (segment[1].y - segment[0].y) * ratio,
                };
            }
            traversed += length;
        }
        *coords.last().unwrap()
    };
    if !repeated {
        return Ok(geom_to_value(Geometry::new(
            geometry.srid,
            Geom::Point(point_at(step)),
        )));
    }
    if step == 0.0 {
        return Err(geo_err(
            crate::error::catalog::ER_DATA_OUT_OF_RANGE,
            format!("The interpolation fraction must be greater than zero in {name}."),
        ));
    }
    let count = (total / step).floor() as usize;
    let _arena = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
        count.saturating_mul(std::mem::size_of::<Coord>()),
    )?;
    let mut points = Vec::new();
    points.try_reserve_exact(count).map_err(|_| {
        geo_err(
            crate::error::catalog::ER_OUTOFMEMORY,
            format!("Out of memory while evaluating {name}."),
        )
    })?;
    points.extend((1..=count).map(|index| point_at(step * index as f64)));
    Ok(geom_to_value(Geometry::new(
        geometry.srid,
        Geom::MultiPoint(points),
    )))
}

fn simplify_geometry(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let mut geometry = geom_value(&values[0], name)?;
    let tolerance = as_f64(&values[1])?;
    if !tolerance.is_finite() || tolerance < 0.0 {
        return Err(geo_err(
            crate::error::catalog::ER_DATA_OUT_OF_RANGE,
            format!("The tolerance is out of range in function {name}."),
        ));
    }

    fn perpendicular_distance(point: Coord, start: Coord, end: Coord) -> f64 {
        let dx = end.x - start.x;
        let dy = end.y - start.y;
        if dx == 0.0 && dy == 0.0 {
            return coord_distance(point, start);
        }
        ((dy * point.x - dx * point.y + end.x * start.y - end.y * start.x).abs()) / dx.hypot(dy)
    }
    fn simplify_line(points: &[Coord], tolerance: f64) -> Vec<Coord> {
        if points.len() <= 2 {
            return points.to_vec();
        }
        let mut max_distance = 0.0;
        let mut split = 0;
        for index in 1..points.len() - 1 {
            let distance =
                perpendicular_distance(points[index], points[0], points[points.len() - 1]);
            if distance > max_distance {
                max_distance = distance;
                split = index;
            }
        }
        if max_distance <= tolerance {
            return vec![points[0], points[points.len() - 1]];
        }
        let mut left = simplify_line(&points[..=split], tolerance);
        let right = simplify_line(&points[split..], tolerance);
        left.pop();
        left.extend(right);
        left
    }
    fn simplify(geom: &mut Geom, tolerance: f64) {
        match geom {
            Geom::LineString(line) => *line = simplify_line(line, tolerance),
            Geom::Polygon(rings) | Geom::MultiLineString(rings) => {
                rings
                    .iter_mut()
                    .for_each(|ring| *ring = simplify_line(ring, tolerance));
            }
            Geom::MultiPolygon(polygons) => polygons.iter_mut().for_each(|polygon| {
                polygon
                    .iter_mut()
                    .for_each(|ring| *ring = simplify_line(ring, tolerance))
            }),
            Geom::GeometryCollection(geometries) => geometries
                .iter_mut()
                .for_each(|geom| simplify(geom, tolerance)),
            Geom::Point(_) | Geom::MultiPoint(_) => {}
        }
    }
    simplify(&mut geometry.geom, tolerance);
    Ok(geom_to_value(geometry))
}

fn transform_geometry(name: &str, values: &[Value]) -> Result<Value, QueryError> {
    check_count(name, values, 2)?;
    let mut geometry = geom_value(&values[0], name)?;
    let target = as_srid(&values[1], name)?;
    if geometry.srid == target {
        return Ok(geom_to_value(geometry));
    }
    match (geometry.srid, target) {
        (4326, 3857) => geometry.geom.for_each_coord_mut(&mut |coord| {
            let latitude = coord.x.clamp(-85.051_128_779_806_6, 85.051_128_779_806_6);
            let longitude = coord.y;
            coord.x = longitude.to_radians() * 6_378_137.0;
            coord.y = ((std::f64::consts::FRAC_PI_4 + latitude.to_radians() / 2.0).tan()).ln()
                * 6_378_137.0;
        }),
        (3857, 4326) => geometry.geom.for_each_coord_mut(&mut |coord| {
            let longitude = (coord.x / 6_378_137.0).to_degrees();
            let latitude = (2.0 * (coord.y / 6_378_137.0).exp().atan()
                - std::f64::consts::FRAC_PI_2)
                .to_degrees();
            coord.x = latitude;
            coord.y = longitude;
        }),
        _ => {
            return Err(geo_err(
                crate::error::catalog::ER_TRANSFORM_SOURCE_SRS_NOT_SUPPORTED,
                format!(
                    "Transformation from SRID {} to SRID {target} is not supported.",
                    geometry.srid
                ),
            ))
        }
    }
    geometry.srid = target;
    Ok(geom_to_value(geometry))
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

fn mbr_pred(name: &str, values: &[Value], op: MbrOp) -> Result<Value, QueryError> {
    let (a, b) = two_geoms(name, values)?;
    if a.srid != b.srid {
        return Err(srid_mismatch(name, a.srid, b.srid));
    }
    Ok(mbr_predicate(op, &a.geom, &b.geom)
        .map(|t| Value::Int(t as i64))
        .unwrap_or(Value::Null))
}

fn st_pred(name: &str, values: &[Value], op: StOp) -> Result<Value, QueryError> {
    let (a, b) = two_geoms(name, values)?;
    if a.srid != b.srid {
        return Err(srid_mismatch(name, a.srid, b.srid));
    }
    if matches!(op, StOp::Touches | StOp::Crosses)
        && matches!((&a.geom, &b.geom), (Geom::Point(_), Geom::Point(_)))
    {
        return Ok(Value::Null);
    }
    Ok(st_predicate(op, &a.geom, &b.geom)
        .map(|t| Value::Int(t as i64))
        .unwrap_or(Value::Null))
}

// ---------------------------------------------------------------------------
// Validity / simplicity / closedness
// ---------------------------------------------------------------------------

fn is_closed(geom: &Geom) -> bool {
    fn line_closed(cs: &[Coord]) -> bool {
        cs.len() >= 2
            && (cs[0].x - cs[cs.len() - 1].x).abs() < 1e-9
            && (cs[0].y - cs[cs.len() - 1].y).abs() < 1e-9
    }
    match geom {
        Geom::LineString(cs) => line_closed(cs),
        Geom::MultiLineString(ls) => !ls.is_empty() && ls.iter().all(|l| line_closed(l)),
        _ => false,
    }
}

fn ring_ok(ring: &[Coord]) -> bool {
    ring.len() >= 4
        && (ring[0].x - ring[ring.len() - 1].x).abs() < 1e-9
        && (ring[0].y - ring[ring.len() - 1].y).abs() < 1e-9
}

fn is_valid(geom: &Geom) -> bool {
    use geo::Validation;
    if !super::relate::topology_geometry(geom).is_valid() {
        return false;
    }
    match geom {
        Geom::Point(c) => c.x.is_finite() && c.y.is_finite(),
        Geom::LineString(cs) => cs.is_empty() || cs.len() >= 2,
        Geom::Polygon(rings) => rings.is_empty() || rings.iter().all(|r| ring_ok(r)),
        Geom::MultiPoint(ps) => ps.iter().all(|c| c.x.is_finite() && c.y.is_finite()),
        Geom::MultiLineString(ls) => ls.iter().all(|l| l.is_empty() || l.len() >= 2),
        Geom::MultiPolygon(ps) => ps.iter().all(|p| p.iter().all(|r| ring_ok(r))),
        Geom::GeometryCollection(gs) => gs.iter().all(is_valid),
    }
}

fn is_simple(geom: &Geom) -> bool {
    match geom {
        Geom::Point(_) => true,
        Geom::MultiPoint(ps) => {
            // simple iff no two points coincide
            for i in 0..ps.len() {
                for j in (i + 1)..ps.len() {
                    if (ps[i].x - ps[j].x).abs() < 1e-9 && (ps[i].y - ps[j].y).abs() < 1e-9 {
                        return false;
                    }
                }
            }
            true
        }
        Geom::LineString(cs) => line_is_simple(cs),
        Geom::MultiLineString(ls) => ls.iter().all(|l| line_is_simple(l)),
        // Polygons are assumed simple (full OGC validity is ST_IsValid's job);
        // a richer check is a future improvement.
        _ => true,
    }
}

/// A line is simple if no pair of non-adjacent segments intersects (a closed
/// ring is allowed to meet only at its shared endpoint).
fn line_is_simple(cs: &[Coord]) -> bool {
    if cs.len() < 3 {
        return true;
    }
    let n = cs.len() - 1; // segment count
    let closed = (cs[0].x - cs[n].x).abs() < 1e-9 && (cs[0].y - cs[n].y).abs() < 1e-9;
    for i in 0..n {
        for j in (i + 1)..n {
            if j == i + 1 {
                continue; // adjacent segments share a vertex
            }
            // First and last segment of a closed ring share the start vertex.
            if closed && i == 0 && j == n - 1 {
                continue;
            }
            if algo::segments_intersect(cs[i], cs[i + 1], cs[j], cs[j + 1]) {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    #[test]
    fn mysql8046_official_spatial_surface_has_111_unique_names() {
        let mut names = GEO_FUNCTIONS.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 111);
        assert_eq!(names.len(), GEO_FUNCTIONS.len(), "duplicate GIS function");
        assert!(!names.contains(&"ST_POINT"));
    }
}

#[cfg(test)]
mod registry_contract_tests {
    use super::*;

    #[test]
    fn aliases_preserve_metadata_null_policy_and_diagnostics() {
        for name in GEO_FUNCTIONS {
            let spec = function_spec(name).unwrap();
            let canonical = function_spec(spec.canonical_id).unwrap();
            assert_eq!(spec.sql_name, *name);
            assert_eq!(
                (spec.min_args, spec.max_args),
                (canonical.min_args, canonical.max_args)
            );
            assert_eq!(spec.null_policy, canonical.null_policy);
            assert_eq!(spec.result, canonical.result);
            for arity in 0..=4 {
                let values = vec![Value::Null; arity];
                let actual = eval(name, name, &values).map_err(|error| error.code());
                let expected = eval(spec.canonical_id, name, &values).map_err(|error| error.code());
                assert_eq!(actual, expected, "{name}/{arity}");
            }
        }
        for name in ["ST_POINT", "st_x", "ABS", ""] {
            assert!(function_spec(name).is_none(), "{name}");
        }
        let error = eval("ST_GEOMETRYFROMTEXT", "St_GeometryFromText", &[]).unwrap_err();
        assert!(error.message().contains("St_GeometryFromText"));
    }

    #[test]
    fn accessor_overloads_keep_wire_types_and_collection_null_semantics() {
        for name in ["ST_X", "ST_Y", "ST_SRID"] {
            assert_eq!(result_type(name, 2), Some(ColumnType::Geometry));
            assert_eq!(
                result_type(name, 1),
                Some(if name == "ST_SRID" {
                    ColumnType::LongLong
                } else {
                    ColumnType::Double
                })
            );
        }
        assert_eq!(eval("ST_X", "ST_X", &[Value::Null]).unwrap(), Value::Null);
        let empty = eval("GEOMETRYCOLLECTION", "GEOMETRYCOLLECTION", &[]).unwrap();
        assert!(!matches!(empty, Value::Null));
        assert_eq!(result_type("ST_ASWKT", 1), Some(ColumnType::Blob));
        assert_eq!(result_type("ST_ASWKB", 1), Some(ColumnType::Blob));
    }
}
