//! GeoJSON (RFC 7946) input/output for `ST_AsGeoJSON` and
//! `ST_GeomFromGeoJSON`. The DOM round-trips through [`serde_json`].

use serde_json::{json, Value as J};

use crate::types::QueryError;

use super::geo_err;
use super::types::*;

fn invalid_geojson(detail: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_INVALID_GEOJSON_UNSPECIFIED,
        format!("Invalid GeoJSON data provided ({detail})."),
    )
}

// --- output ---

/// Coordinates are JSON doubles, including integral coordinate values.
fn num(v: f64) -> J {
    serde_json::Number::from_f64(v)
        .map(J::Number)
        .unwrap_or(J::Null)
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

pub(super) fn geom_json(geom: &Geom) -> J {
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
#[cfg(test)]
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

fn parse_geom(v: &J, depth: u32, allow_feature: bool) -> Result<Option<Geom>, QueryError> {
    if depth > 32 {
        return Err(invalid_geojson("nesting too deep"));
    }
    if depth > 0 && v.get("crs").is_some_and(|value| !value.is_null()) {
        return Err(crate::error::catalog::ER_INVALID_GEOJSON_CRS_NOT_TOP_LEVEL
            .error("Member 'crs' must be specified in the top level GeoJSON object."));
    }
    let ty = v
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| invalid_geojson("missing type"))?;
    let coords = || {
        v.get("coordinates")
            .ok_or_else(|| invalid_geojson("missing coordinates"))
    };
    let geom = match ty {
        "Feature" | "FeatureCollection" if !allow_feature => {
            return Err(invalid_geojson("expected a geometry object"));
        }
        "Feature" => {
            let properties = v.get("properties").ok_or_else(|| {
                crate::error::catalog::ER_INVALID_GEOJSON_MISSING_MEMBER
                    .error("Invalid GeoJSON data: missing required member 'properties'.")
            })?;
            if !properties.is_object() && !properties.is_null() {
                return Err(invalid_geojson(
                    "Feature properties must be an object or null",
                ));
            }
            let geometry = v
                .get("geometry")
                .ok_or_else(|| invalid_geojson("missing geometry"))?;
            if geometry.is_null() {
                return Ok(None);
            }
            return parse_geom(geometry, depth + 1, false);
        }
        "FeatureCollection" | "GeometryCollection" => {
            let member = if ty == "FeatureCollection" {
                "features"
            } else {
                "geometries"
            };
            let arr = v
                .get(member)
                .and_then(J::as_array)
                .ok_or_else(|| invalid_geojson("missing collection array"))?;
            let mut geometries = Vec::with_capacity(arr.len());
            for item in arr {
                if ty == "FeatureCollection"
                    && item.get("type").and_then(J::as_str) != Some("Feature")
                {
                    return Err(invalid_geojson(
                        "FeatureCollection members must be Features",
                    ));
                }
                if let Some(geometry) = parse_geom(item, depth + 1, ty == "FeatureCollection")? {
                    geometries.push(geometry);
                }
            }
            Geom::GeometryCollection(geometries)
        }
        "Point" => Geom::Point(parse_coord(coords()?)?),
        "LineString" => Geom::LineString(parse_seq(coords()?)?),
        "Polygon" => Geom::Polygon(parse_rings(coords()?)?),
        "MultiPoint" => Geom::MultiPoint(parse_seq(coords()?)?),
        "MultiLineString" => {
            let arr = coords()?
                .as_array()
                .ok_or_else(|| invalid_geojson("MultiLineString coordinates"))?;
            Geom::MultiLineString(arr.iter().map(parse_seq).collect::<Result<_, _>>()?)
        }
        "MultiPolygon" => {
            let arr = coords()?
                .as_array()
                .ok_or_else(|| invalid_geojson("MultiPolygon coordinates"))?;
            Geom::MultiPolygon(arr.iter().map(parse_rings).collect::<Result<_, _>>()?)
        }
        other => return Err(invalid_geojson(&format!("unknown geometry type {other}"))),
    };
    Ok(Some(geom))
}

/// `ST_GeomFromGeoJSON` input. `srid` is applied to the result (MySQL defaults
/// to 4326 for GeoJSON).
#[cfg(test)]
pub(crate) fn from_geojson(text: &str, srid: u32) -> Result<Geometry, QueryError> {
    from_geojson_options(text, srid, true)
}

#[cfg(test)]
pub(crate) fn from_geojson_options(
    text: &str,
    srid: u32,
    strip_extra: bool,
) -> Result<Geometry, QueryError> {
    from_geojson_sql(text, Some(srid), strip_extra)?
        .ok_or_else(|| invalid_geojson("null feature geometry"))
}

pub(crate) fn from_geojson_sql(
    text: &str,
    explicit_srid: Option<u32>,
    strip_extra: bool,
) -> Result<Option<Geometry>, QueryError> {
    let value: J = serde_json::from_str(text).map_err(|_| invalid_geojson("invalid JSON"))?;
    if !strip_extra {
        fn check(value: &J) -> Result<(), QueryError> {
            match value {
                J::Array(values) => {
                    if values.first().is_some_and(J::is_number) && values.len() > 2 {
                        return Err(crate::error::catalog::ER_DIMENSION_UNSUPPORTED
                            .error("GeoJSON coordinates have more than two dimensions."));
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
        check(&value)?;
    }
    let srid = match explicit_srid {
        Some(srid) => srid,
        None => match value.get("crs").filter(|value| !value.is_null()) {
            None => 4326,
            Some(crs) => {
                if crs.get("type").and_then(J::as_str) != Some("name") {
                    return Err(invalid_geojson("unsupported CRS type"));
                }
                let name = crs
                    .get("properties")
                    .and_then(|p| p.get("name"))
                    .and_then(J::as_str)
                    .ok_or_else(|| invalid_geojson("missing CRS name"))?;
                if name == "urn:ogc:def:crs:OGC:1.3:CRS84" {
                    4326
                } else {
                    let code = name
                        .strip_prefix("EPSG:")
                        .or_else(|| name.strip_prefix("urn:ogc:def:crs:EPSG::"))
                        .filter(|code| !code.is_empty() && code.bytes().all(|b| b.is_ascii_digit()))
                        .ok_or_else(|| invalid_geojson("unsupported CRS name"))?;
                    code.parse()
                        .map_err(|_| invalid_geojson("invalid CRS identifier"))?
                }
            }
        },
    };
    let Some(geom) = parse_geom(&value, 0, true)? else {
        return Ok(None);
    };
    super::validate_shape(&geom)?;
    Ok(Some(Geometry::new(srid, geom)))
}
