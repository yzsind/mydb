//! High-performance spatial (GIS) support for the MySQL-compatible
//! `GEOMETRY` family of data types.
//!
//! MySQL stores a geometry on disk as a 4-byte little-endian SRID followed by
//! standard WKB, and only converts to text when a client calls `ST_AsText`.
//! mydb follows the same model: on disk and inside
//! [`crate::types::Value::Geometry`] a value is the raw `SRID + WKB` byte
//! string, so reading a geometry column off the heap costs zero parsing. The
//! bytes are parsed into a [`Geom`] tree only when a spatial function needs to
//! compute over the geometry (area, relate, …), mirroring how the JSON type
//! keeps documents binary and parses lazily.
//!
//! Submodules:
//!   * [`types`]  — the [`Geom`] DOM, [`Geometry`] (SRID + tree) and [`Mbr`].
//!   * [`wkb`]    — WKB / internal-format codec.
//!   * [`wkt`]    — WKT text parser / serializer.
//!   * [`geojson`]— GeoJSON in/out.
//!   * [`algo`]   — area / length / distance / centroid / envelope / hull.
//!   * [`relate`] — MBR predicates and topological predicates.

mod algo;
mod geojson;
mod relate;
pub(crate) mod sql;
pub(crate) mod srs;
pub(crate) mod types;
mod wkb;
mod wkt;

#[cfg(test)]
mod tests;

use std::cmp::Ordering;

use crate::types::QueryError;

pub(crate) use types::Mbr;

#[cfg(test)]
pub(crate) use algo::{
    area, centroid, convex_hull, distance, distance_sphere, envelope, length, DEFAULT_SPHERE_RADIUS,
};
#[cfg(test)]
pub(crate) use relate::{mbr_predicate, st_predicate, MbrOp, StOp};

// Re-export the codec / format surface the expression evaluator and storage
// layer build on.
#[cfg(test)]
pub(crate) use geojson::from_geojson;
pub(crate) use geojson::to_geojson;
pub(crate) use wkb::{read_geometry, read_wkb, write_geometry, write_wkb};
pub(crate) use wkt::{parse_wkt, write_wkt};

/// Build a [`QueryError`] for a GIS failure. Centralised so the SQLSTATE and
/// error-code conventions stay consistent with MySQL across the module.
#[inline]
pub(crate) fn geo_err(spec: crate::error::ErrorSpec, message: impl Into<String>) -> QueryError {
    spec.error(message)
}

/// MySQL error 3037: invalid GIS data passed to a spatial function.
pub(crate) fn invalid_gis_data(func: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_GIS_INVALID_DATA,
        format!("Invalid GIS data provided to function {func}."),
    )
}

/// Syntactic well-formedness shared by all geometry input codecs. Topological
/// validity (self intersections, hole placement) remains ST_IsValid's job.
pub(super) fn validate_shape(geom: &types::Geom) -> Result<(), QueryError> {
    use types::Geom;
    let line_ok = |cs: &[types::Coord]| cs.len() >= 2;
    let polygon_ok = |rings: &[Vec<types::Coord>]| {
        rings
            .iter()
            .all(|ring| ring.len() >= 4 && ring.first() == ring.last())
    };
    let valid = match geom {
        Geom::Point(_) | Geom::MultiPoint(_) => true,
        Geom::LineString(cs) => cs.is_empty() || line_ok(cs),
        Geom::Polygon(rings) => polygon_ok(rings),
        Geom::MultiLineString(lines) => lines.iter().all(|cs| line_ok(cs)),
        Geom::MultiPolygon(polygons) => polygons.iter().all(|rings| polygon_ok(rings)),
        Geom::GeometryCollection(geoms) => {
            for child in geoms {
                validate_shape(child)?;
            }
            return Ok(());
        }
    };
    let mut finite = true;
    geom.for_each_coord(&mut |c| finite &= c.x.is_finite() && c.y.is_finite());
    if !valid || !finite {
        return Err(invalid_gis_data("geometry input"));
    }
    Ok(())
}

/// MySQL error 3617: SRID mismatch between two geometry arguments.
pub(crate) fn srid_mismatch(func: &str, a: u32, b: u32) -> QueryError {
    geo_err(
        crate::error::catalog::ER_GIS_DIFFERENT_SRIDS,
        format!(
            "Binary geometry function {func} given two geometries of different srids: {a} and {b}, \
             which should have been identical."
        ),
    )
}

// ---------------------------------------------------------------------------
// Public facade — the surface the rest of the engine uses.
// ---------------------------------------------------------------------------

/// Validate and normalise an internal `SRID + WKB` byte string. Returns the
/// canonical little-endian re-encoding so heap storage is byte-stable
/// regardless of the byte order the value arrived in.
pub(crate) fn normalize_internal(bytes: &[u8]) -> Result<Vec<u8>, QueryError> {
    let g = read_geometry(bytes)?;
    Ok(write_geometry(&g))
}

/// Validate an internal geometry against a column's subtype / SRID
/// constraints. `allowed_type_code` is `Some(code)` for a typed column
/// (POINT/POLYGON/…) and `None` for the permissive base GEOMETRY;
/// `required_srid` is `Some(srid)` when the column declares `SRID n`.
pub(crate) fn validate_internal_constraints(
    internal: &[u8],
    allowed_type_code: Option<u32>,
    required_srid: Option<u32>,
) -> Result<(), QueryError> {
    let g = read_geometry(internal)?;
    if let Some(code) = allowed_type_code {
        if g.geom.type_code() != code {
            return Err(geo_err(
                crate::error::catalog::ER_WRONG_SRID_FOR_COLUMN,
                format!(
                    "Cannot store a {} into a column of a different geometry type.",
                    g.geom.type_name()
                ),
            ));
        }
    }
    if let Some(srid) = required_srid {
        if g.srid != srid {
            return Err(geo_err(crate::error::catalog::ER_WRONG_SRID_FOR_COLUMN,
                format!(
                    "The SRID of the geometry is {}, but the SRID of the column is {}. \
                     Consider changing the SRID of the geometry or the SRID property of the column.",
                    g.srid, srid
                ),
            ));
        }
    }
    Ok(())
}

/// Parse WKT text into an internal `SRID + WKB` byte string.
#[cfg(test)]
pub(crate) fn parse_wkt_internal(text: &str, srid: u32) -> Result<Vec<u8>, QueryError> {
    let geom = parse_wkt(text)?;
    Ok(write_geometry(&types::Geometry::new(srid, geom)))
}

/// Render an internal geometry as WKT (SRID dropped), used by
/// `Value::to_display_string` and as a fallback. Returns an empty string on
/// undecodable input (matching `json::to_text`'s lenient contract).
pub(crate) fn to_text(internal: &[u8]) -> String {
    match read_geometry(internal) {
        Ok(g) => write_wkt(&g.geom),
        Err(_) => String::new(),
    }
}

/// Minimum bounding rectangle of an internal geometry, `None` if empty or
/// undecodable. Used by the R-tree spatial index.
pub(crate) fn mbr_of(internal: &[u8]) -> Option<Mbr> {
    read_geometry(internal).ok().and_then(|g| g.geom.mbr())
}

/// Total ordering for `ORDER BY` / `DISTINCT` / `GROUP BY` on geometry
/// columns. MySQL has no geometrically meaningful order; it sorts by the
/// internal binary, which is what we do (deterministic and stable).
pub(crate) fn compare(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}
