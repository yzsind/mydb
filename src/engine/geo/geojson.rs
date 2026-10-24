//! GeoJSON (RFC 7946) input/output for `ST_AsGeoJSON` and
//! `ST_GeomFromGeoJSON`. The DOM round-trips through [`serde_json`].

use serde_json::{json, Value as J};

use crate::types::QueryError;

use super::geo_err;
use super::types::*;

fn invalid_geojson(detail: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_GIS_INVALID_DATA,
        format!("Invalid GeoJSON data provided ({detail})."),
    )
}

// --- output ---

/// Render a numeric coordinate, preferring an integer JSON token when the
/// value is integral (matching MySQL's `[1, 1]` rather than `[1.0, 1.0]`).
fn num(v: f64) -> J {
    if v.fract() == 0.0 && v.abs() < 9.0e15 {
        J::from(v as i64)
    } else {
        serde_json::Number::from_f64(v)
            .map(J::Number)
            .unwrap_or(J::Null)
    }
}

fn coord_json(c: Coord) -> J {
    J::Array(vec![num(c.x), num(c.y)])
}

fn seq_json(cs: &[Coord]) -> J {
    J::Array(cs.iter().map(|c| coord_json(*c)).collect())
}

fn rings_json(rings: &[Vec<Coord>]) -> J {
    J::Array(rings.iter().map(|r| seq_json(r)).collect())
}

fn geom_json(geom: &Geom) -> J {
    match geom {
        Geom::Point(c) => json!({"type": "Point", "coordinates": coord_json(*c)}),
        Geom::LineString(cs) => json!({"type": "LineString", "coordinates": seq_json(cs)}),
        Geom::Polygon(rings) => json!({"type": "Polygon", "coordinates": rings_json(rings)}),
        Geom::MultiPoint(ps) => json!({"type": "MultiPoint", "coordinates": seq_json(ps)}),
        Geom::MultiLineString(ls) => {
            json!({"type": "MultiLineString", "coordinates": J::Array(ls.iter().map(|l| seq_json(l)).collect())})
        }
        Geom::MultiPolygon(ps) => {
            json!({"type": "MultiPolygon", "coordinates": J::Array(ps.iter().map(|p| rings_json(p)).collect())})
        }
        Geom::GeometryCollection(gs) => {
            json!({"type": "GeometryCollection", "geometries": J::Array(gs.iter().map(geom_json).collect())})
        }
    }
}

/// `ST_AsGeoJSON` output (compact form).
pub(crate) fn to_geojson(geom: &Geom) -> String {
    geom_json(geom).to_string()
}

// --- input ---

fn parse_coord(v: &J) -> Result<Coord, QueryError> {
    let arr = v
        .as_array()
        .ok_or_else(|| invalid_geojson("coordinate is not an array"))?;
    if arr.len() < 2 {
        return Err(invalid_geojson("coordinate needs at least two numbers"));
    }
    let x = arr[0]
        .as_f64()
        .ok_or_else(|| invalid_geojson("coordinate x is not a number"))?;
    let y = arr[1]
        .as_f64()
        .ok_or_else(|| invalid_geojson("coordinate y is not a number"))?;
    Ok(Coord { x, y })
}

fn parse_seq(v: &J) -> Result<Vec<Coord>, QueryError> {
    v.as_array()
        .ok_or_else(|| invalid_geojson("coordinate list is not an array"))?
        .iter()
        .map(parse_coord)
        .collect()
}

fn parse_rings(v: &J) -> Result<Vec<Vec<Coord>>, QueryError> {
    v.as_array()
        .ok_or_else(|| invalid_geojson("ring list is not an array"))?
        .iter()
        .map(parse_seq)
        .collect()
}

fn parse_geom(v: &J, depth: u32) -> Result<Geom, QueryError> {
    if depth > 32 {
        return Err(invalid_geojson("nesting too deep"));
    }
    let ty = v
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| invalid_geojson("missing type"))?;
    let coords = || {
        v.get("coordinates")
            .ok_or_else(|| invalid_geojson("missing coordinates"))
    };
    match ty {
        "Point" => Ok(Geom::Point(parse_coord(coords()?)?)),
        "LineString" => Ok(Geom::LineString(parse_seq(coords()?)?)),
        "Polygon" => Ok(Geom::Polygon(parse_rings(coords()?)?)),
        "MultiPoint" => Ok(Geom::MultiPoint(parse_seq(coords()?)?)),
        "MultiLineString" => {
            let arr = coords()?
                .as_array()
                .ok_or_else(|| invalid_geojson("MultiLineString coordinates"))?;
            Ok(Geom::MultiLineString(
                arr.iter().map(parse_seq).collect::<Result<_, _>>()?,
            ))
        }
        "MultiPolygon" => {
            let arr = coords()?
                .as_array()
                .ok_or_else(|| invalid_geojson("MultiPolygon coordinates"))?;
            Ok(Geom::MultiPolygon(
                arr.iter().map(parse_rings).collect::<Result<_, _>>()?,
            ))
        }
        "GeometryCollection" => {
            let arr = v
                .get("geometries")
                .and_then(|g| g.as_array())
                .ok_or_else(|| invalid_geojson("GeometryCollection geometries"))?;
            Ok(Geom::GeometryCollection(
                arr.iter()
                    .map(|g| parse_geom(g, depth + 1))
                    .collect::<Result<_, _>>()?,
            ))
        }
        other => Err(invalid_geojson(&format!("unknown geometry type {other}"))),
    }
}

/// `ST_GeomFromGeoJSON` input. `srid` is applied to the result (MySQL defaults
/// to 4326 for GeoJSON).
#[cfg(test)]
pub(crate) fn from_geojson(text: &str, srid: u32) -> Result<Geometry, QueryError> {
    from_geojson_options(text, srid, true)
}

pub(crate) fn from_geojson_options(
    text: &str,
    srid: u32,
    strip_extra: bool,
) -> Result<Geometry, QueryError> {
    if !strip_extra {
        fn check(value: &J) -> Result<(), QueryError> {
            match value {
                J::Array(values) => {
                    if values.first().is_some_and(J::is_number) && values.len() > 2 {
                        return Err(invalid_geojson("coordinates have more than two dimensions"));
                    }
                    for value in values {
                        check(value)?;
                    }
                }
                J::Object(values) => {
                    for key in ["coordinates", "geometries", "geometry", "features"] {
                        if let Some(value) = values.get(key) {
                            check(value)?;
                        }
                    }
                }
                _ => {}
            }
            Ok(())
        }
        let value: J = serde_json::from_str(text).map_err(|_| invalid_geojson("invalid JSON"))?;
        check(&value)?;
    }
    from_geojson_impl(text, srid)
}

fn from_geojson_impl(text: &str, srid: u32) -> Result<Geometry, QueryError> {
    let v: J = serde_json::from_str(text).map_err(|_| invalid_geojson("not valid JSON"))?;
    let geom = parse_geom(&v, 0)?;
    super::validate_shape(&geom)?;
    Ok(Geometry::new(srid, geom))
}
