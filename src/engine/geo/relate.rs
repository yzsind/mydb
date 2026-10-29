//! Spatial relationship predicates.
//!
//! MBR predicates compare bounds directly. Topological touches, crosses and
//! overlaps use a noded DE-9IM intersection matrix, including multipart inputs.

use super::algo::{point_in_polygon, segments_intersect, Containment};
use super::types::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MbrOp {
    Contains,
    Within,
    Intersects,
    Disjoint,
    Equals,
    Touches,
    Overlaps,
    Covers,
    CoveredBy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StOp {
    Intersects,
    Disjoint,
    Contains,
    Within,
    Equals,
    Touches,
    Crosses,
    Overlaps,
}

/// Evaluate an MBR predicate. `None` when either geometry is empty (no MBR),
/// which MySQL surfaces as NULL.
pub(crate) fn mbr_predicate(op: MbrOp, a: &Geom, b: &Geom) -> Option<bool> {
    let ma = a.mbr()?;
    let mb = b.mbr()?;
    Some(match op {
        MbrOp::Contains => contains_interior(&ma, &mb),
        MbrOp::Within => contains_interior(&mb, &ma),
        MbrOp::Covers => ma.contains(&mb),
        MbrOp::CoveredBy => mb.contains(&ma),
        MbrOp::Intersects => ma.intersects(&mb),
        MbrOp::Disjoint => !ma.intersects(&mb),
        MbrOp::Equals => ma.equals(&mb),
        MbrOp::Touches => ma.intersects(&mb) && !interiors_overlap(&ma, &mb),
        MbrOp::Overlaps => {
            interiors_overlap(&ma, &mb) && !ma.contains(&mb) && !mb.contains(&ma) && !ma.equals(&mb)
        }
    })
}

#[inline]
fn contains_interior(a: &Mbr, b: &Mbr) -> bool {
    a.contains(b)
        && (if a.minx == a.maxx {
            b.minx == a.minx && b.maxx == a.maxx
        } else {
            b.minx < a.maxx && b.maxx > a.minx
        })
        && (if a.miny == a.maxy {
            b.miny == a.miny && b.maxy == a.maxy
        } else {
            b.miny < a.maxy && b.maxy > a.miny
        })
}

#[inline]
fn interiors_overlap(a: &Mbr, b: &Mbr) -> bool {
    a.minx < b.maxx && a.maxx > b.minx && a.miny < b.maxy && a.maxy > b.miny
}

/// Evaluate a topological predicate. `None` when either geometry is empty.
pub(crate) fn st_predicate(op: StOp, a: &Geom, b: &Geom) -> Option<bool> {
    if a.is_empty() || b.is_empty() {
        // ST_Equals is the documented exception to the empty-geometry NULL
        // rule: two empty geometries represent the same point set.
        return matches!(op, StOp::Equals).then(|| a.is_empty() && b.is_empty());
    }
    Some(match op {
        StOp::Intersects => intersects(a, b),
        StOp::Disjoint => !intersects(a, b),
        StOp::Contains => contains(a, b),
        StOp::Within => contains(b, a),
        StOp::Equals => equals(a, b),
        StOp::Touches => touches(a, b),
        StOp::Crosses => crosses(a, b),
        StOp::Overlaps => overlaps(a, b),
    })
}

// ---------------------------------------------------------------------------
// Point-vs-geometry coverage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cover {
    Interior,
    Boundary,
    Outside,
}

const PT_EPS: f64 = 1e-9;

fn coincides(a: Coord, b: Coord) -> bool {
    (a.x - b.x).abs() <= PT_EPS && (a.y - b.y).abs() <= PT_EPS
}

fn on_seg(p: Coord, a: Coord, b: Coord) -> bool {
    super::algo::on_segment(p, a, b)
}

/// Classify `pt` relative to `geom`.
fn cover_point(geom: &Geom, pt: Coord) -> Cover {
    match geom {
        Geom::Point(c) => {
            if coincides(*c, pt) {
                Cover::Interior
            } else {
                Cover::Outside
            }
        }
        Geom::MultiPoint(ps) => {
            if ps.iter().any(|c| coincides(*c, pt)) {
                Cover::Interior
            } else {
                Cover::Outside
            }
        }
        Geom::LineString(cs) => cover_point_line(cs, pt),
        Geom::MultiLineString(ls) => combine(ls.iter().map(|l| cover_point_line(l, pt))),
        Geom::Polygon(rings) => match point_in_polygon(pt, rings) {
            Containment::Inside => Cover::Interior,
            Containment::Boundary => Cover::Boundary,
            Containment::Outside => Cover::Outside,
        },
        Geom::MultiPolygon(ps) => combine(ps.iter().map(|p| match point_in_polygon(pt, p) {
            Containment::Inside => Cover::Interior,
            Containment::Boundary => Cover::Boundary,
            Containment::Outside => Cover::Outside,
        })),
        Geom::GeometryCollection(gs) => combine(gs.iter().map(|g| cover_point(g, pt))),
    }
}

fn cover_point_line(cs: &[Coord], pt: Coord) -> Cover {
    if cs.is_empty() {
        return Cover::Outside;
    }
    if cs.len() == 1 {
        return if coincides(cs[0], pt) {
            Cover::Interior
        } else {
            Cover::Outside
        };
    }
    let closed = coincides(cs[0], cs[cs.len() - 1]);
    for w in cs.windows(2) {
        if on_seg(pt, w[0], w[1]) {
            // Endpoints of an open line are boundary; everything else interior.
            if !closed && (coincides(pt, cs[0]) || coincides(pt, cs[cs.len() - 1])) {
                return Cover::Boundary;
            }
            return Cover::Interior;
        }
    }
    Cover::Outside
}

fn combine(it: impl Iterator<Item = Cover>) -> Cover {
    let mut best = Cover::Outside;
    for c in it {
        match c {
            Cover::Interior => return Cover::Interior,
            Cover::Boundary => best = Cover::Boundary,
            Cover::Outside => {}
        }
    }
    best
}

#[inline]
fn covered(geom: &Geom, pt: Coord) -> bool {
    cover_point(geom, pt) != Cover::Outside
}

// ---------------------------------------------------------------------------
// Boundary segments
// ---------------------------------------------------------------------------

fn boundary_segments(geom: &Geom, out: &mut Vec<(Coord, Coord)>) {
    match geom {
        Geom::Point(_) | Geom::MultiPoint(_) => {}
        Geom::LineString(cs) => push_open_segs(cs, out),
        Geom::MultiLineString(ls) => ls.iter().for_each(|l| push_open_segs(l, out)),
        Geom::Polygon(rings) => rings.iter().for_each(|r| push_ring_segs(r, out)),
        Geom::MultiPolygon(ps) => ps
            .iter()
            .for_each(|p| p.iter().for_each(|r| push_ring_segs(r, out))),
        Geom::GeometryCollection(gs) => gs.iter().for_each(|g| boundary_segments(g, out)),
    }
}

fn push_open_segs(cs: &[Coord], out: &mut Vec<(Coord, Coord)>) {
    cs.windows(2).for_each(|w| out.push((w[0], w[1])));
}

fn push_ring_segs(ring: &[Coord], out: &mut Vec<(Coord, Coord)>) {
    let n = ring.len();
    if n < 2 {
        return;
    }
    for i in 0..n {
        out.push((ring[i], ring[(i + 1) % n]));
    }
}

fn boundaries_cross(a: &Geom, b: &Geom) -> bool {
    let mut sa = Vec::new();
    let mut sb = Vec::new();
    boundary_segments(a, &mut sa);
    boundary_segments(b, &mut sb);
    sa.iter().any(|&(p1, p2)| {
        sb.iter()
            .any(|&(p3, p4)| segments_intersect(p1, p2, p3, p4))
    })
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

fn mbr_disjoint(a: &Geom, b: &Geom) -> bool {
    match (a.mbr(), b.mbr()) {
        (Some(ma), Some(mb)) => !ma.intersects(&mb),
        _ => true,
    }
}

fn intersects(a: &Geom, b: &Geom) -> bool {
    if mbr_disjoint(a, b) {
        return false;
    }
    if a.all_coords().iter().any(|&c| covered(b, c)) {
        return true;
    }
    if b.all_coords().iter().any(|&c| covered(a, c)) {
        return true;
    }
    boundaries_cross(a, b)
}

/// `a` contains `b`: every point of `b` lies in the closure of `a`, no point
/// of `b` is in the exterior of `a`, and their interiors meet.
fn contains(a: &Geom, b: &Geom) -> bool {
    use geo::Relate;
    topology_geometry(a)
        .relate(&topology_geometry(b))
        .is_contains()
}

fn equals(a: &Geom, b: &Geom) -> bool {
    // Mutual containment (cheap exact-equality fast path first).
    if a == b {
        return true;
    }
    match (a.mbr(), b.mbr()) {
        (Some(ma), Some(mb)) if ma.equals(&mb) => contains(a, b) && contains(b, a),
        _ => false,
    }
}

fn coord(c: &Coord) -> geo::Coord<f64> {
    geo::Coord { x: c.x, y: c.y }
}
fn line(cs: &[Coord]) -> geo::LineString<f64> {
    geo::LineString(cs.iter().map(coord).collect())
}
fn polygon(rings: &[Vec<Coord>]) -> geo::Polygon<f64> {
    geo::Polygon::new(
        rings.first().map_or_else(|| line(&[]), |r| line(r)),
        rings.iter().skip(1).map(|r| line(r)).collect(),
    )
}

pub(super) fn topology_geometry(geom: &Geom) -> geo::Geometry<f64> {
    match geom {
        Geom::Point(c) => geo::Point(coord(c)).into(),
        Geom::LineString(cs) => line(cs).into(),
        Geom::Polygon(rings) => polygon(rings).into(),
        Geom::MultiPoint(cs) => {
            geo::MultiPoint(cs.iter().map(|c| geo::Point(coord(c))).collect()).into()
        }
        Geom::MultiLineString(lines) => {
            geo::MultiLineString(lines.iter().map(|cs| line(cs)).collect()).into()
        }
        Geom::MultiPolygon(polygons) => {
            geo::MultiPolygon(polygons.iter().map(|rings| polygon(rings)).collect()).into()
        }
        Geom::GeometryCollection(gs) => geo::Geometry::GeometryCollection(geo::GeometryCollection(
            gs.iter().map(topology_geometry).collect(),
        )),
    }
}

/// Relate heterogeneous collections by normalized dimensional components.
/// A single mixed graph can classify an isolated point as line interior.
fn topology_groups(geom: &Geom) -> Vec<geo::Geometry<f64>> {
    use geo::{BooleanOps, Intersects};
    fn collect(
        geom: &Geom,
        points: &mut Vec<geo::Point<f64>>,
        lines: &mut Vec<geo::LineString<f64>>,
        polygons: &mut Vec<geo::Polygon<f64>>,
    ) {
        match geom {
            Geom::GeometryCollection(parts) => {
                for part in parts {
                    collect(part, points, lines, polygons);
                }
            }
            Geom::Point(point) => points.push(geo::Point(coord(point))),
            Geom::MultiPoint(parts) => {
                points.extend(parts.iter().map(|point| geo::Point(coord(point))))
            }
            Geom::LineString(points) => lines.push(line(points)),
            Geom::MultiLineString(parts) => lines.extend(parts.iter().map(|points| line(points))),
            Geom::Polygon(rings) => polygons.push(polygon(rings)),
            Geom::MultiPolygon(parts) => polygons.extend(parts.iter().map(|rings| polygon(rings))),
        }
    }
    let mut points = Vec::new();
    let mut lines = Vec::new();
    let mut polygons = Vec::new();
    collect(geom, &mut points, &mut lines, &mut polygons);
    let polygons = if polygons.len() > 1 {
        geo::unary_union(&polygons)
    } else {
        geo::MultiPolygon(polygons)
    };
    let lines = geo::MultiLineString(lines);
    let lines = if polygons.0.is_empty() {
        lines
    } else {
        polygons.clip(&lines, true)
    };
    points.retain(|point| !polygons.intersects(point) && !lines.intersects(point));
    let mut groups = Vec::with_capacity(3);
    if !points.is_empty() {
        groups.push(geo::MultiPoint(points).into());
    }
    if !lines.0.is_empty() {
        groups.push(lines.into());
    }
    if !polygons.0.is_empty() {
        groups.push(polygons.into());
    }
    groups
}

fn touches(a: &Geom, b: &Geom) -> bool {
    use geo::Relate;
    if matches!(a, Geom::GeometryCollection(_)) || matches!(b, Geom::GeometryCollection(_)) {
        let left = topology_groups(a);
        let right = topology_groups(b);
        let mut touching = false;
        for a in &left {
            for b in &right {
                let matrix = a.relate(b);
                if matrix.matches("T********").expect("valid DE-9IM mask") {
                    return false;
                }
                touching |= matrix.is_touches();
            }
        }
        return touching;
    }
    topology_geometry(a)
        .relate(&topology_geometry(b))
        .is_touches()
}

fn crosses(a: &Geom, b: &Geom) -> bool {
    use geo::Relate;
    topology_geometry(a)
        .relate(&topology_geometry(b))
        .is_crosses()
}

fn overlaps(a: &Geom, b: &Geom) -> bool {
    use geo::Relate;
    topology_geometry(a)
        .relate(&topology_geometry(b))
        .is_overlaps()
}
