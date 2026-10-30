//! WKT (Well-Known Text) parser and serializer, matching MySQL's text form:
//! `POINT(1 1)`, `POLYGON((0 0,1 0,1 1,0 0))`, `GEOMETRYCOLLECTION EMPTY`,
//! etc. Keywords are case-insensitive; numbers may be signed integers,
//! decimals, or scientific notation.

use crate::types::QueryError;

use super::geo_err;
use super::types::*;

fn invalid_wkt(detail: &str) -> QueryError {
    geo_err(
        crate::error::catalog::ER_GIS_INVALID_DATA,
        format!("Invalid GIS data provided (WKT: {detail})."),
    )
}

pub fn parse_wkt(text: &str) -> Result<Geom, QueryError> {
    let mut p = WktParser {
        s: text.as_bytes(),
        pos: 0,
    };
    p.skip_ws();
    let g = p.parse_geom(0)?;
    p.skip_ws();
    if p.pos != p.s.len() {
        return Err(invalid_wkt("trailing characters after geometry"));
    }
    super::validate_shape(&g)?;
    Ok(g)
}

const MAX_DEPTH: u32 = 32;

struct WktParser<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> WktParser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.s.len() && self.s[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.pos).copied()
    }

    fn expect(&mut self, c: u8) -> Result<(), QueryError> {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(invalid_wkt(&format!("expected '{}'", c as char)))
        }
    }

    /// Read an alphabetic keyword and upper-case it.
    fn read_keyword(&mut self) -> Result<String, QueryError> {
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.s.len() && self.s[self.pos].is_ascii_alphabetic() {
            self.pos += 1;
        }
        if self.pos == start {
            return Err(invalid_wkt("expected geometry keyword"));
        }
        Ok(String::from_utf8_lossy(&self.s[start..self.pos]).to_ascii_uppercase())
    }

    /// After a type keyword, returns true and consumes `EMPTY` if present.
    fn try_empty(&mut self) -> bool {
        let save = self.pos;
        self.skip_ws();
        let start = self.pos;
        while self.pos < self.s.len() && self.s[self.pos].is_ascii_alphabetic() {
            self.pos += 1;
        }
        if self.s[start..self.pos].eq_ignore_ascii_case(b"EMPTY") {
            true
        } else {
            self.pos = save;
            false
        }
    }

    fn parse_number(&mut self) -> Result<f64, QueryError> {
        self.skip_ws();
        let start = self.pos;
        if matches!(self.peek(), Some(b'+') | Some(b'-')) {
            self.pos += 1;
        }
        let mut saw_digit = false;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' => {
                    saw_digit = true;
                    self.pos += 1;
                }
                b'.' => self.pos += 1,
                b'e' | b'E' => {
                    self.pos += 1;
                    if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                        self.pos += 1;
                    }
                }
                _ => break,
            }
        }
        if !saw_digit {
            return Err(invalid_wkt("expected number"));
        }
        std::str::from_utf8(&self.s[start..self.pos])
            .ok()
            .and_then(|t| t.parse::<f64>().ok())
            .filter(|value| value.is_finite())
            .ok_or_else(|| invalid_wkt("malformed number"))
    }

    fn parse_coord(&mut self) -> Result<Coord, QueryError> {
        let x = self.parse_number()?;
        let y = self.parse_number()?;
        Ok(Coord { x, y })
    }

    /// `x y, x y, ...` inside an already-consumed `(`; consumes the closing
    /// `)`.
    fn parse_coord_list(&mut self) -> Result<Vec<Coord>, QueryError> {
        let mut out = vec![self.parse_coord()?];
        loop {
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    out.push(self.parse_coord()?);
                }
                Some(b')') => {
                    self.pos += 1;
                    return Ok(out);
                }
                _ => return Err(invalid_wkt("expected ',' or ')' in coordinate list")),
            }
        }
    }

    fn parse_ring_list(&mut self) -> Result<Vec<Vec<Coord>>, QueryError> {
        // already past outer '('
        let mut rings = Vec::new();
        loop {
            self.expect(b'(')?;
            rings.push(self.parse_coord_list()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b')') => {
                    self.pos += 1;
                    return Ok(rings);
                }
                _ => return Err(invalid_wkt("expected ',' or ')' in ring list")),
            }
        }
    }

    fn parse_geom(&mut self, depth: u32) -> Result<Geom, QueryError> {
        if depth > MAX_DEPTH {
            return Err(invalid_wkt("nesting too deep"));
        }
        let kw = self.read_keyword()?;
        match kw.as_str() {
            "POINT" => {
                if self.try_empty() {
                    return Err(invalid_wkt("POINT EMPTY is not supported"));
                }
                self.expect(b'(')?;
                let c = self.parse_coord()?;
                self.expect(b')')?;
                Ok(Geom::Point(c))
            }
            "LINESTRING" => {
                if self.try_empty() {
                    return Ok(Geom::LineString(Vec::new()));
                }
                self.expect(b'(')?;
                Ok(Geom::LineString(self.parse_coord_list()?))
            }
            "POLYGON" => {
                if self.try_empty() {
                    return Ok(Geom::Polygon(Vec::new()));
                }
                self.expect(b'(')?;
                Ok(Geom::Polygon(self.parse_ring_list()?))
            }
            "MULTIPOINT" => {
                if self.try_empty() {
                    return Ok(Geom::MultiPoint(Vec::new()));
                }
                self.expect(b'(')?;
                Ok(Geom::MultiPoint(self.parse_multipoint_body()?))
            }
            "MULTILINESTRING" => {
                if self.try_empty() {
                    return Ok(Geom::MultiLineString(Vec::new()));
                }
                self.expect(b'(')?;
                let mut out = Vec::new();
                loop {
                    self.expect(b'(')?;
                    out.push(self.parse_coord_list()?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.pos += 1,
                        Some(b')') => {
                            self.pos += 1;
                            break;
                        }
                        _ => return Err(invalid_wkt("expected ',' or ')'")),
                    }
                }
                Ok(Geom::MultiLineString(out))
            }
            "MULTIPOLYGON" => {
                if self.try_empty() {
                    return Ok(Geom::MultiPolygon(Vec::new()));
                }
                self.expect(b'(')?;
                let mut out = Vec::new();
                loop {
                    self.expect(b'(')?;
                    out.push(self.parse_ring_list()?);
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => self.pos += 1,
                        Some(b')') => {
                            self.pos += 1;
                            break;
                        }
                        _ => return Err(invalid_wkt("expected ',' or ')'")),
                    }
                }
                Ok(Geom::MultiPolygon(out))
            }
            "GEOMETRYCOLLECTION" => {
                if self.try_empty() {
                    return Ok(Geom::GeometryCollection(Vec::new()));
                }
                self.expect(b'(')?;
                let mut out = vec![self.parse_geom(depth + 1)?];
                loop {
                    self.skip_ws();
                    match self.peek() {
                        Some(b',') => {
                            self.pos += 1;
                            out.push(self.parse_geom(depth + 1)?);
                        }
                        Some(b')') => {
                            self.pos += 1;
                            break;
                        }
                        _ => return Err(invalid_wkt("expected ',' or ')' in collection")),
                    }
                }
                Ok(Geom::GeometryCollection(out))
            }
            other => Err(invalid_wkt(&format!("unknown geometry keyword {other}"))),
        }
    }

    /// MULTIPOINT accepts both `(1 2, 3 4)` and `((1 2),(3 4))`.
    fn parse_multipoint_body(&mut self) -> Result<Vec<Coord>, QueryError> {
        let mut out = Vec::new();
        loop {
            self.skip_ws();
            let parenthesized = self.peek() == Some(b'(');
            if parenthesized {
                self.pos += 1;
            }
            out.push(self.parse_coord()?);
            if parenthesized {
                self.expect(b')')?;
            }
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b')') => {
                    self.pos += 1;
                    return Ok(out);
                }
                _ => return Err(invalid_wkt("expected ',' or ')' in multipoint")),
            }
        }
    }
}

// --- serializer ---

/// Format a coordinate value the way MySQL renders WKT: integers without a
/// decimal point, others via the shortest round-trip representation.
fn push_num(out: &mut String, v: f64) {
    use std::fmt::Write;
    if v == 0.0 {
        // Avoid "-0".
        out.push('0');
    } else {
        let _ = write!(out, "{v}");
    }
}

fn push_coord(out: &mut String, c: Coord) {
    push_num(out, c.x);
    out.push(' ');
    push_num(out, c.y);
}

fn push_seq(out: &mut String, cs: &[Coord]) {
    out.push('(');
    for (i, c) in cs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_coord(out, *c);
    }
    out.push(')');
}

fn push_rings(out: &mut String, rings: &[Vec<Coord>]) {
    out.push('(');
    for (i, r) in rings.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_seq(out, r);
    }
    out.push(')');
}

pub fn write_wkt(geom: &Geom) -> String {
    let mut out = String::new();
    write_into(&mut out, geom);
    out
}

fn write_into(out: &mut String, geom: &Geom) {
    match geom {
        Geom::Point(c) => {
            out.push_str("POINT(");
            push_coord(out, *c);
            out.push(')');
        }
        Geom::LineString(cs) => {
            out.push_str("LINESTRING");
            if cs.is_empty() {
                out.push_str(" EMPTY");
            } else {
                push_seq(out, cs);
            }
        }
        Geom::Polygon(rings) => {
            out.push_str("POLYGON");
            if rings.is_empty() {
                out.push_str(" EMPTY");
            } else {
                push_rings(out, rings);
            }
        }
        Geom::MultiPoint(ps) => {
            out.push_str("MULTIPOINT");
            if ps.is_empty() {
                out.push_str(" EMPTY");
            } else {
                // MySQL renders MULTIPOINT members without inner parens.
                push_seq(out, ps);
            }
        }
        Geom::MultiLineString(ls) => {
            out.push_str("MULTILINESTRING");
            if ls.is_empty() {
                out.push_str(" EMPTY");
            } else {
                push_rings(out, ls);
            }
        }
        Geom::MultiPolygon(ps) => {
            out.push_str("MULTIPOLYGON");
            if ps.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push('(');
                for (i, p) in ps.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    push_rings(out, p);
                }
                out.push(')');
            }
        }
        Geom::GeometryCollection(gs) => {
            out.push_str("GEOMETRYCOLLECTION");
            if gs.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push('(');
                for (i, g) in gs.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_into(out, g);
                }
                out.push(')');
            }
        }
    }
}
