//! WKB (Well-Known Binary) codec and the MySQL internal storage format
//! (`SRID(4, little-endian) + WKB`).
//!
//! Reads accept either byte order (NDR/little-endian or XDR/big-endian) and
//! each nested geometry inside a multi/collection carries its own byte-order
//! byte, per the OGC spec. Writes always emit little-endian (NDR), which is
//! what MySQL `ST_AsBinary` produces. Only 2-D geometries are supported; an
//! ISO Z/M or EWKB SRID-flagged type code is rejected with a clear error.

use crate::types::QueryError;

use super::geo_err;
use super::types::*;

/// Read a value in the MySQL internal format: 4-byte little-endian SRID
/// followed by standard WKB.
pub fn read_geometry(buf: &[u8]) -> Result<Geometry, QueryError> {
    if buf.len() < 4 {
        return Err(invalid_gis("internal geometry shorter than SRID prefix"));
    }
    let srid = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let mut r = Reader {
        buf,
        pos: 4,
        depth: 0,
    };
    let geom = r.read_geom()?;
    if r.pos != buf.len() {
        return Err(invalid_gis("trailing bytes"));
    }
    super::validate_shape(&geom)?;
    Ok(Geometry { srid, geom })
}

/// Serialize to the MySQL internal format (SRID prefix + NDR WKB).
pub fn write_geometry(g: &Geometry) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 64);
    out.extend_from_slice(&g.srid.to_le_bytes());
    write_geom(&mut out, &g.geom);
    out
}

/// Read bare WKB (no SRID prefix) — `ST_GeomFromWKB` input.
pub fn read_wkb(buf: &[u8]) -> Result<Geom, QueryError> {
    let mut r = Reader {
        buf,
        pos: 0,
        depth: 0,
    };
    let geom = r.read_geom()?;
    if r.pos != buf.len() {
        return Err(invalid_gis("trailing bytes"));
    }
    super::validate_shape(&geom)?;
    Ok(geom)
}

/// Serialize bare WKB (no SRID prefix), little-endian — `ST_AsBinary` output.
pub fn write_wkb(geom: &Geom) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    write_geom(&mut out, geom);
    out
}

fn invalid_gis(detail: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_GIS_INVALID_DATA,
        format!("Invalid GIS data provided ({detail})."),
    )
}

const MAX_DEPTH: u32 = 32;

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    depth: u32,
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Result<u8, QueryError> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| invalid_gis("truncated WKB"))?;
        self.pos += 1;
        Ok(b)
    }

    fn u32(&mut self, le: bool) -> Result<u32, QueryError> {
        let end = self.pos + 4;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| invalid_gis("truncated WKB integer"))?;
        let arr = [slice[0], slice[1], slice[2], slice[3]];
        self.pos = end;
        Ok(if le {
            u32::from_le_bytes(arr)
        } else {
            u32::from_be_bytes(arr)
        })
    }

    fn f64(&mut self, le: bool) -> Result<f64, QueryError> {
        let end = self.pos + 8;
        let slice = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| invalid_gis("truncated WKB double"))?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(slice);
        self.pos = end;
        let value = if le {
            f64::from_le_bytes(arr)
        } else {
            f64::from_be_bytes(arr)
        };
        if !value.is_finite() {
            return Err(invalid_gis("non-finite coordinate"));
        }
        Ok(value)
    }

    fn coord(&mut self, le: bool) -> Result<Coord, QueryError> {
        let x = self.f64(le)?;
        let y = self.f64(le)?;
        Ok(Coord { x, y })
    }

    /// Read a `count`-prefixed ring/point list.
    fn coord_seq(&mut self, le: bool) -> Result<Vec<Coord>, QueryError> {
        let n = self.u32(le)? as usize;
        // Guard against a corrupt length claiming more coords than the
        // buffer could possibly hold (16 bytes each).
        if n.saturating_mul(16) > self.buf.len() {
            return Err(invalid_gis("WKB coordinate count exceeds buffer"));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.coord(le)?);
        }
        Ok(out)
    }

    fn rings(&mut self, le: bool) -> Result<Vec<Vec<Coord>>, QueryError> {
        let n = self.u32(le)? as usize;
        if n.saturating_mul(4) > self.buf.len() {
            return Err(invalid_gis("WKB ring count exceeds buffer"));
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(self.coord_seq(le)?);
        }
        Ok(out)
    }

    fn read_geom(&mut self) -> Result<Geom, QueryError> {
        if self.depth > MAX_DEPTH {
            return Err(invalid_gis("WKB nesting too deep"));
        }
        let order = self.u8()?;
        let le = match order {
            0 => false,
            1 => true,
            _ => return Err(invalid_gis("bad WKB byte order")),
        };
        let raw = self.u32(le)?;
        // Reject ISO Z/M (>= 1000) and EWKB flag bits (Z=0x80000000,
        // M=0x40000000, SRID=0x20000000) — only 2-D is supported.
        if raw & 0xE000_0000 != 0 || raw >= 1000 {
            return Err(invalid_gis("only 2-D geometries are supported"));
        }
        match raw {
            WKB_POINT => Ok(Geom::Point(self.coord(le)?)),
            WKB_LINESTRING => Ok(Geom::LineString(self.coord_seq(le)?)),
            WKB_POLYGON => Ok(Geom::Polygon(self.rings(le)?)),
            WKB_MULTIPOINT => {
                let n = self.count(le)?;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    match self.read_sub()? {
                        Geom::Point(c) => out.push(c),
                        _ => return Err(invalid_gis("MULTIPOINT member is not a POINT")),
                    }
                }
                Ok(Geom::MultiPoint(out))
            }
            WKB_MULTILINESTRING => {
                let n = self.count(le)?;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    match self.read_sub()? {
                        Geom::LineString(cs) => out.push(cs),
                        _ => return Err(invalid_gis("MULTILINESTRING member is not a LINESTRING")),
                    }
                }
                Ok(Geom::MultiLineString(out))
            }
            WKB_MULTIPOLYGON => {
                let n = self.count(le)?;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    match self.read_sub()? {
                        Geom::Polygon(rings) => out.push(rings),
                        _ => return Err(invalid_gis("MULTIPOLYGON member is not a POLYGON")),
                    }
                }
                Ok(Geom::MultiPolygon(out))
            }
            WKB_GEOMETRYCOLLECTION => {
                let n = self.count(le)?;
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    out.push(self.read_sub()?);
                }
                Ok(Geom::GeometryCollection(out))
            }
            _ => Err(invalid_gis("unknown WKB geometry type")),
        }
    }

    /// Read an element-count u32, bounded so a corrupt length can't trigger a
    /// huge allocation (each sub-geometry is at least 9 bytes).
    fn count(&mut self, le: bool) -> Result<usize, QueryError> {
        let n = self.u32(le)? as usize;
        if n.saturating_mul(9) > self.buf.len() {
            return Err(invalid_gis("WKB element count exceeds buffer"));
        }
        Ok(n)
    }

    fn read_sub(&mut self) -> Result<Geom, QueryError> {
        self.depth += 1;
        let g = self.read_geom();
        self.depth -= 1;
        g
    }
}

// --- writers (always little-endian / NDR) ---

fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_coord(out: &mut Vec<u8>, c: Coord) {
    out.extend_from_slice(&c.x.to_le_bytes());
    out.extend_from_slice(&c.y.to_le_bytes());
}

fn write_seq(out: &mut Vec<u8>, cs: &[Coord]) {
    write_u32(out, cs.len() as u32);
    for c in cs {
        write_coord(out, *c);
    }
}

fn write_rings(out: &mut Vec<u8>, rings: &[Vec<Coord>]) {
    write_u32(out, rings.len() as u32);
    for r in rings {
        write_seq(out, r);
    }
}

pub(super) fn write_geom(out: &mut Vec<u8>, geom: &Geom) {
    out.push(1); // NDR
    write_u32(out, geom.type_code());
    match geom {
        Geom::Point(c) => write_coord(out, *c),
        Geom::LineString(cs) => write_seq(out, cs),
        Geom::Polygon(rings) => write_rings(out, rings),
        Geom::MultiPoint(ps) => {
            write_u32(out, ps.len() as u32);
            for c in ps {
                write_geom(out, &Geom::Point(*c));
            }
        }
        Geom::MultiLineString(ls) => {
            write_u32(out, ls.len() as u32);
            for l in ls {
                write_geom(out, &Geom::LineString(l.clone()));
            }
        }
        Geom::MultiPolygon(ps) => {
            write_u32(out, ps.len() as u32);
            for p in ps {
                write_geom(out, &Geom::Polygon(p.clone()));
            }
        }
        Geom::GeometryCollection(gs) => {
            write_u32(out, gs.len() as u32);
            for g in gs {
                write_geom(out, g);
            }
        }
    }
}
