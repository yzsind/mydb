//! Planar geometric algorithms: area, length, distance (planar + spherical),
//! centroid, envelope, convex hull, plus the point-in-polygon and
//! segment-intersection primitives shared with the topological-relation
//! engine in [`super::relate`].
//!
//! Robustness uses a small absolute epsilon. Test/data coordinates are
//! typically exact integers or short decimals, for which this is reliable; a
//! fully robust exact-arithmetic kernel is a future improvement.

use super::types::*;

const EPS: f64 = 1e-9;

// ---------------------------------------------------------------------------
// Low-level primitives
// ---------------------------------------------------------------------------

/// Where a point sits relative to a polygon-with-holes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Containment {
    Inside,
    Boundary,
    Outside,
}

#[inline]
fn cross(o: Coord, a: Coord, b: Coord) -> f64 {
    (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x)
}

#[inline]
fn dist2(a: Coord, b: Coord) -> f64 {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    dx * dx + dy * dy
}

/// True when `p` lies on the closed segment `a`–`b`.
pub(super) fn on_segment(p: Coord, a: Coord, b: Coord) -> bool {
    if cross(a, b, p).abs() > EPS * (1.0 + a.x.abs() + a.y.abs() + b.x.abs() + b.y.abs()) {
        return false;
    }
    p.x >= a.x.min(b.x) - EPS
        && p.x <= a.x.max(b.x) + EPS
        && p.y >= a.y.min(b.y) - EPS
        && p.y <= a.y.max(b.y) + EPS
}

/// Proper or improper intersection test for two closed segments.
pub(super) fn segments_intersect(p1: Coord, p2: Coord, p3: Coord, p4: Coord) -> bool {
    let d1 = cross(p3, p4, p1);
    let d2 = cross(p3, p4, p2);
    let d3 = cross(p1, p2, p3);
    let d4 = cross(p1, p2, p4);
    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }
    on_segment(p1, p3, p4)
        || on_segment(p2, p3, p4)
        || on_segment(p3, p1, p2)
        || on_segment(p4, p1, p2)
}

/// Crossing-number test; boundary handled separately by the caller. The ring
/// is treated as implicitly closed.
fn point_strictly_in_ring(pt: Coord, ring: &[Coord]) -> bool {
    let n = ring.len();
    if n < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let pi = ring[i];
        let pj = ring[j];
        if (pi.y > pt.y) != (pj.y > pt.y) {
            let x_int = (pj.x - pi.x) * (pt.y - pi.y) / (pj.y - pi.y) + pi.x;
            if pt.x < x_int {
                inside = !inside;
            }
        }
        j = i;
    }
    inside
}

fn point_on_ring_boundary(pt: Coord, ring: &[Coord]) -> bool {
    let n = ring.len();
    if n < 2 {
        return n == 1 && dist2(pt, ring[0]) <= EPS * EPS;
    }
    for i in 0..n {
        let a = ring[i];
        let b = ring[(i + 1) % n];
        if on_segment(pt, a, b) {
            return true;
        }
    }
    false
}

/// Locate `pt` against a polygon given as `[exterior, hole, hole, ...]`.
pub(super) fn point_in_polygon(pt: Coord, rings: &[Vec<Coord>]) -> Containment {
    if rings.is_empty() {
        return Containment::Outside;
    }
    for ring in rings {
        if point_on_ring_boundary(pt, ring) {
            return Containment::Boundary;
        }
    }
    if !point_strictly_in_ring(pt, &rings[0]) {
        return Containment::Outside;
    }
    for hole in &rings[1..] {
        if point_strictly_in_ring(pt, hole) {
            return Containment::Outside;
        }
    }
    Containment::Inside
}

// ---------------------------------------------------------------------------
// Area / length
// ---------------------------------------------------------------------------

fn ring_signed_area(ring: &[Coord]) -> f64 {
    let n = ring.len();
    if n < 3 {
        return 0.0;
    }
    let mut sum = 0.0;
    let mut j = n - 1;
    for i in 0..n {
        sum += (ring[j].x + ring[i].x) * (ring[j].y - ring[i].y);
        j = i;
    }
    sum / 2.0
}

fn polygon_area(rings: &[Vec<Coord>]) -> f64 {
    if rings.is_empty() {
        return 0.0;
    }
    let mut a = ring_signed_area(&rings[0]).abs();
    for hole in &rings[1..] {
        a -= ring_signed_area(hole).abs();
    }
    a.max(0.0)
}

/// MySQL `ST_Area`: total surface area (0 for non-areal geometry).
pub(crate) fn area(geom: &Geom) -> f64 {
    match geom {
        Geom::Polygon(rings) => polygon_area(rings),
        Geom::MultiPolygon(ps) => ps.iter().map(|p| polygon_area(p)).sum(),
        Geom::GeometryCollection(gs) => gs.iter().map(area).sum(),
        _ => 0.0,
    }
}

fn line_length(cs: &[Coord]) -> f64 {
    cs.windows(2).map(|w| dist2(w[0], w[1]).sqrt()).sum()
}

/// Total length of all linear components (used by `ST_Length`; the expr layer
/// applies MySQL's NULL-for-non-line rule).
pub(crate) fn length(geom: &Geom) -> f64 {
    match geom {
        Geom::LineString(cs) => line_length(cs),
        Geom::MultiLineString(ls) => ls.iter().map(|l| line_length(l)).sum(),
        Geom::GeometryCollection(gs) => gs.iter().map(length).sum(),
        _ => 0.0,
    }
}

// ---------------------------------------------------------------------------
// Distance (planar + spherical)
// ---------------------------------------------------------------------------

enum Prim {
    P(Coord),
    S(Coord, Coord),
}

fn collect_prims(geom: &Geom, out: &mut Vec<Prim>) {
    match geom {
        Geom::Point(c) => out.push(Prim::P(*c)),
        Geom::MultiPoint(ps) => ps.iter().for_each(|c| out.push(Prim::P(*c))),
        Geom::LineString(cs) => push_line_prims(cs, out),
        Geom::MultiLineString(ls) => ls.iter().for_each(|l| push_line_prims(l, out)),
        Geom::Polygon(rings) => rings.iter().for_each(|r| push_ring_prims(r, out)),
        Geom::MultiPolygon(ps) => ps
            .iter()
            .for_each(|p| p.iter().for_each(|r| push_ring_prims(r, out))),
        Geom::GeometryCollection(gs) => gs.iter().for_each(|g| collect_prims(g, out)),
    }
}

fn push_line_prims(cs: &[Coord], out: &mut Vec<Prim>) {
    match cs.len() {
        0 => {}
        1 => out.push(Prim::P(cs[0])),
        _ => cs.windows(2).for_each(|w| out.push(Prim::S(w[0], w[1]))),
    }
}

fn push_ring_prims(ring: &[Coord], out: &mut Vec<Prim>) {
    let n = ring.len();
    if n == 1 {
        out.push(Prim::P(ring[0]));
        return;
    }
    for i in 0..n {
        out.push(Prim::S(ring[i], ring[(i + 1) % n]));
    }
}

fn point_seg_dist(p: Coord, a: Coord, b: Coord) -> f64 {
    let l2 = dist2(a, b);
    if l2 <= EPS * EPS {
        return dist2(p, a).sqrt();
    }
    let t = (((p.x - a.x) * (b.x - a.x) + (p.y - a.y) * (b.y - a.y)) / l2).clamp(0.0, 1.0);
    let proj = Coord {
        x: a.x + t * (b.x - a.x),
        y: a.y + t * (b.y - a.y),
    };
    dist2(p, proj).sqrt()
}

fn prim_dist(x: &Prim, y: &Prim) -> f64 {
    match (x, y) {
        (Prim::P(p), Prim::P(q)) => dist2(*p, *q).sqrt(),
        (Prim::P(p), Prim::S(a, b)) | (Prim::S(a, b), Prim::P(p)) => point_seg_dist(*p, *a, *b),
        (Prim::S(a, b), Prim::S(c, d)) => {
            if segments_intersect(*a, *b, *c, *d) {
                0.0
            } else {
                point_seg_dist(*a, *c, *d)
                    .min(point_seg_dist(*b, *c, *d))
                    .min(point_seg_dist(*c, *a, *b))
                    .min(point_seg_dist(*d, *a, *b))
            }
        }
    }
}

/// True when `geom` has any areal component covering `pt`.
fn covers_point(geom: &Geom, pt: Coord) -> bool {
    match geom {
        Geom::Polygon(rings) => point_in_polygon(pt, rings) != Containment::Outside,
        Geom::MultiPolygon(ps) => ps
            .iter()
            .any(|p| point_in_polygon(pt, p) != Containment::Outside),
        Geom::GeometryCollection(gs) => gs.iter().any(|g| covers_point(g, pt)),
        _ => false,
    }
}

fn covers_component_start(area: &Geom, geometry: &Geom) -> bool {
    let line_start = |line: &[Coord]| line.first().is_some_and(|point| covers_point(area, *point));
    match geometry {
        Geom::Point(point) => covers_point(area, *point),
        Geom::LineString(line) => line_start(line),
        Geom::Polygon(rings) => rings.first().is_some_and(|ring| line_start(ring)),
        Geom::MultiPoint(points) => points.iter().any(|point| covers_point(area, *point)),
        Geom::MultiLineString(lines) => lines.iter().any(|line| line_start(line)),
        Geom::MultiPolygon(polygons) => polygons
            .iter()
            .any(|rings| rings.first().is_some_and(|ring| line_start(ring))),
        Geom::GeometryCollection(geometries) => geometries
            .iter()
            .any(|geometry| covers_component_start(area, geometry)),
    }
}

/// MySQL `ST_Distance`: minimum planar distance, `None` if either geometry is
/// empty.
pub(crate) fn distance(a: &Geom, b: &Geom) -> Option<f64> {
    let mut pa = Vec::new();
    let mut pb = Vec::new();
    collect_prims(a, &mut pa);
    collect_prims(b, &mut pb);
    if pa.is_empty() || pb.is_empty() {
        return None;
    }
    let mut best = f64::INFINITY;
    for x in &pa {
        for y in &pb {
            let d = prim_dist(x, y);
            if d < best {
                best = d;
            }
            if best == 0.0 {
                return Some(0.0);
            }
        }
    }
    // Full containment (one inside the other's interior, boundaries disjoint).
    // Every disconnected component can lie inside an areal component.
    // Boundaries are disjoint, so one point per connected component suffices.
    if covers_component_start(b, a) || covers_component_start(a, b) {
        return Some(0.0);
    }
    Some(best)
}

/// Earth radius MySQL uses by default for `ST_Distance_Sphere` (metres).
pub(crate) const DEFAULT_SPHERE_RADIUS: f64 = 6_370_986.0;

fn haversine(p: Coord, q: Coord, radius: f64) -> f64 {
    let phi1 = p.y.to_radians();
    let phi2 = q.y.to_radians();
    let dphi = (q.y - p.y).to_radians();
    let dlmb = (q.x - p.x).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlmb / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).max(0.0).sqrt());
    radius * c
}

fn sphere_points(geom: &Geom, out: &mut Vec<Coord>) -> bool {
    match geom {
        Geom::Point(c) => {
            out.push(*c);
            true
        }
        Geom::MultiPoint(ps) => {
            out.extend_from_slice(ps);
            true
        }
        _ => false,
    }
}

/// MySQL `ST_Distance_Sphere`: great-circle distance between (multi)points in
/// degrees lon/lat. `None` for unsupported geometry types or empty input.
pub(crate) fn distance_sphere(a: &Geom, b: &Geom, radius: f64) -> Option<f64> {
    let mut pa = Vec::new();
    let mut pb = Vec::new();
    if !sphere_points(a, &mut pa) || !sphere_points(b, &mut pb) {
        return None;
    }
    if pa.is_empty() || pb.is_empty() {
        return None;
    }
    let mut best = f64::INFINITY;
    for p in &pa {
        for q in &pb {
            best = best.min(haversine(*p, *q, radius));
        }
    }
    Some(best)
}

// ---------------------------------------------------------------------------
// Centroid
// ---------------------------------------------------------------------------

fn polygon_centroid(rings: &[Vec<Coord>]) -> Option<(Coord, f64)> {
    if rings.is_empty() {
        return None;
    }
    let mut cx = 0.0;
    let mut cy = 0.0;
    let mut area_sum = 0.0;
    for (idx, ring) in rings.iter().enumerate() {
        let a = ring_signed_area(ring);
        let (rx, ry) = ring_centroid_raw(ring);
        // Exterior adds, holes subtract.
        let sign = if idx == 0 { 1.0 } else { -1.0 };
        cx += sign * rx * a.abs();
        cy += sign * ry * a.abs();
        area_sum += sign * a.abs();
    }
    if area_sum.abs() < EPS {
        return None;
    }
    Some((
        Coord {
            x: cx / area_sum,
            y: cy / area_sum,
        },
        area_sum,
    ))
}

/// Centroid of a single ring. Uses the shoelace cross-term `s = Σ(x_j·y_i −
/// x_i·y_j)` for both the weighting and the area so the sign convention stays
/// internally consistent (`A = s/2`, so `C = Σ(coord_j+coord_i)·f / (3·s)`).
fn ring_centroid_raw(ring: &[Coord]) -> (f64, f64) {
    let n = ring.len();
    if n < 3 {
        // Degenerate: average of vertices.
        let (sx, sy) = ring
            .iter()
            .fold((0.0, 0.0), |(sx, sy), c| (sx + c.x, sy + c.y));
        let k = n.max(1) as f64;
        return (sx / k, sy / k);
    }
    let mut cx = 0.0;
    let mut cy = 0.0;
    let mut s = 0.0;
    let mut j = n - 1;
    for i in 0..n {
        let f = ring[j].x * ring[i].y - ring[i].x * ring[j].y;
        cx += (ring[j].x + ring[i].x) * f;
        cy += (ring[j].y + ring[i].y) * f;
        s += f;
        j = i;
    }
    if s.abs() < EPS {
        let (sx, sy) = ring
            .iter()
            .fold((0.0, 0.0), |(sx, sy), c| (sx + c.x, sy + c.y));
        return (sx / n as f64, sy / n as f64);
    }
    (cx / (3.0 * s), cy / (3.0 * s))
}

/// MySQL `ST_Centroid`: area-weighted for areal geometry, else length-weighted
/// for linear, else the vertex average for point sets. `None` if empty.
pub(crate) fn centroid(geom: &Geom) -> Option<Coord> {
    // Prefer the highest present dimension.
    let mut areal = Vec::new();
    collect_polygons(geom, &mut areal);
    if !areal.is_empty() {
        let mut cx = 0.0;
        let mut cy = 0.0;
        let mut wsum = 0.0;
        for rings in &areal {
            if let Some((c, w)) = polygon_centroid(rings) {
                cx += c.x * w;
                cy += c.y * w;
                wsum += w;
            }
        }
        if wsum.abs() > EPS {
            return Some(Coord {
                x: cx / wsum,
                y: cy / wsum,
            });
        }
    }
    // Linear: length-weighted midpoint average.
    let mut lines = Vec::new();
    collect_lines(geom, &mut lines);
    if lines.iter().any(|l| l.len() >= 2) {
        let mut cx = 0.0;
        let mut cy = 0.0;
        let mut wsum = 0.0;
        for l in &lines {
            for w in l.windows(2) {
                let len = dist2(w[0], w[1]).sqrt();
                cx += (w[0].x + w[1].x) / 2.0 * len;
                cy += (w[0].y + w[1].y) / 2.0 * len;
                wsum += len;
            }
        }
        if wsum > EPS {
            return Some(Coord {
                x: cx / wsum,
                y: cy / wsum,
            });
        }
    }
    // Points: vertex average.
    let coords = geom.all_coords();
    if coords.is_empty() {
        return None;
    }
    let (sx, sy) = coords
        .iter()
        .fold((0.0, 0.0), |(sx, sy), c| (sx + c.x, sy + c.y));
    Some(Coord {
        x: sx / coords.len() as f64,
        y: sy / coords.len() as f64,
    })
}

fn collect_polygons(geom: &Geom, out: &mut Vec<Vec<Vec<Coord>>>) {
    match geom {
        Geom::Polygon(rings) => out.push(rings.clone()),
        Geom::MultiPolygon(ps) => out.extend(ps.iter().cloned()),
        Geom::GeometryCollection(gs) => gs.iter().for_each(|g| collect_polygons(g, out)),
        _ => {}
    }
}

fn collect_lines(geom: &Geom, out: &mut Vec<Vec<Coord>>) {
    match geom {
        Geom::LineString(cs) => out.push(cs.clone()),
        Geom::MultiLineString(ls) => out.extend(ls.iter().cloned()),
        Geom::GeometryCollection(gs) => gs.iter().for_each(|g| collect_lines(g, out)),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Envelope / convex hull
// ---------------------------------------------------------------------------

/// MySQL `ST_Envelope`: the MBR as a geometry. Point → POINT, axis-aligned
/// degenerate line → LINESTRING, otherwise a closed rectangle POLYGON.
pub(crate) fn envelope(geom: &Geom) -> Option<Geom> {
    let m = geom.mbr()?;
    let degenerate_x = (m.maxx - m.minx).abs() < EPS;
    let degenerate_y = (m.maxy - m.miny).abs() < EPS;
    if degenerate_x && degenerate_y {
        Some(Geom::Point(Coord {
            x: m.minx,
            y: m.miny,
        }))
    } else if degenerate_x || degenerate_y {
        Some(Geom::LineString(vec![
            Coord {
                x: m.minx,
                y: m.miny,
            },
            Coord {
                x: m.maxx,
                y: m.maxy,
            },
        ]))
    } else {
        Some(Geom::Polygon(vec![vec![
            Coord {
                x: m.minx,
                y: m.miny,
            },
            Coord {
                x: m.maxx,
                y: m.miny,
            },
            Coord {
                x: m.maxx,
                y: m.maxy,
            },
            Coord {
                x: m.minx,
                y: m.maxy,
            },
            Coord {
                x: m.minx,
                y: m.miny,
            },
        ]]))
    }
}

/// MySQL `ST_ConvexHull`: Andrew's monotone chain over all vertices. Returns a
/// POLYGON, or a degenerate POINT / LINESTRING when all points are coincident
/// or collinear. `None` if empty.
pub(crate) fn convex_hull(geom: &Geom) -> Option<Geom> {
    let mut pts = geom.all_coords();
    if pts.is_empty() {
        return None;
    }
    pts.sort_by(|a, b| {
        a.x.partial_cmp(&b.x)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.y.partial_cmp(&b.y).unwrap_or(std::cmp::Ordering::Equal))
    });
    pts.dedup_by(|a, b| dist2(*a, *b) <= EPS * EPS);
    if pts.len() == 1 {
        return Some(Geom::Point(pts[0]));
    }
    // Build lower and upper hulls.
    let mut hull: Vec<Coord> = Vec::with_capacity(pts.len() + 1);
    for &p in &pts {
        while hull.len() >= 2 && cross(hull[hull.len() - 2], hull[hull.len() - 1], p) <= EPS {
            hull.pop();
        }
        hull.push(p);
    }
    let lower_len = hull.len() + 1;
    for &p in pts.iter().rev().skip(1) {
        while hull.len() >= lower_len && cross(hull[hull.len() - 2], hull[hull.len() - 1], p) <= EPS
        {
            hull.pop();
        }
        hull.push(p);
    }
    hull.pop(); // last == first

    if hull.len() <= 1 {
        return Some(Geom::Point(pts[0]));
    }
    if hull.len() == 2 {
        return Some(Geom::LineString(vec![hull[0], hull[1]]));
    }
    // Detect all-collinear (monotone chain collapses to a line both ways).
    let collinear = pts.iter().all(|&p| {
        cross(pts[0], pts[pts.len() - 1], p).abs() <= EPS * (1.0 + p.x.abs() + p.y.abs())
    });
    if collinear {
        return Some(Geom::LineString(vec![pts[0], pts[pts.len() - 1]]));
    }
    let mut ring = hull;
    ring.push(ring[0]); // close
    Some(Geom::Polygon(vec![ring]))
}
