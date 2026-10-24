//! Core geometry DOM and bounding-box types shared by the WKB/WKT/GeoJSON
//! codecs, the geometric algorithms, the topological-relation engine and the
//! R-tree spatial index.
//!
//! Coordinates are planar `(x, y)` doubles. MySQL stores a geometry on disk
//! as a 4-byte little-endian SRID followed by standard WKB; the in-memory
//! [`Geometry`] pairs that SRID with a parsed [`Geom`] tree. Pure-geometry
//! operations (area, hull, relate) take a `&Geom`; SRID-aware operations
//! (distance, ST_SRID) take a `&Geometry`.

/// A planar coordinate. `x` is easting / longitude, `y` is northing /
/// latitude (matching MySQL's axis-order for SRID 0 and projected SRIDs).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coord {
    pub x: f64,
    pub y: f64,
}

impl Coord {}

/// Standard WKB geometry type codes (ISO/OGC).
pub const WKB_POINT: u32 = 1;
pub const WKB_LINESTRING: u32 = 2;
pub const WKB_POLYGON: u32 = 3;
pub const WKB_MULTIPOINT: u32 = 4;
pub const WKB_MULTILINESTRING: u32 = 5;
pub const WKB_MULTIPOLYGON: u32 = 6;
pub const WKB_GEOMETRYCOLLECTION: u32 = 7;

/// Parsed geometry tree (no SRID — see [`Geometry`]).
///
/// A `Polygon` is a list of rings; ring 0 is the exterior boundary and the
/// rest are interior holes. A `MultiPolygon` is a list of such ring-lists.
/// Empty geometries are represented by empty inner vectors (e.g. an empty
/// `LineString` has no coords). MySQL forbids an empty `POINT`, so `Point`
/// always carries a coordinate.
#[derive(Debug, Clone, PartialEq)]
pub enum Geom {
    Point(Coord),
    LineString(Vec<Coord>),
    /// rings: `[exterior, hole1, hole2, ...]`
    Polygon(Vec<Vec<Coord>>),
    MultiPoint(Vec<Coord>),
    MultiLineString(Vec<Vec<Coord>>),
    MultiPolygon(Vec<Vec<Vec<Coord>>>),
    GeometryCollection(Vec<Geom>),
}

impl Geom {
    /// WKB type code (1..=7).
    pub fn type_code(&self) -> u32 {
        match self {
            Geom::Point(_) => WKB_POINT,
            Geom::LineString(_) => WKB_LINESTRING,
            Geom::Polygon(_) => WKB_POLYGON,
            Geom::MultiPoint(_) => WKB_MULTIPOINT,
            Geom::MultiLineString(_) => WKB_MULTILINESTRING,
            Geom::MultiPolygon(_) => WKB_MULTIPOLYGON,
            Geom::GeometryCollection(_) => WKB_GEOMETRYCOLLECTION,
        }
    }

    /// MySQL `ST_GeometryType` name, e.g. `"POINT"`, `"MULTIPOLYGON"`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Geom::Point(_) => "POINT",
            Geom::LineString(_) => "LINESTRING",
            Geom::Polygon(_) => "POLYGON",
            Geom::MultiPoint(_) => "MULTIPOINT",
            Geom::MultiLineString(_) => "MULTILINESTRING",
            Geom::MultiPolygon(_) => "MULTIPOLYGON",
            Geom::GeometryCollection(_) => "GEOMETRYCOLLECTION",
        }
    }

    /// MySQL `ST_Dimension`: 0 for point-like, 1 for curve-like, 2 for
    /// surface-like; a collection takes the max of its members. Returns
    /// `None` for an empty geometry (MySQL yields NULL).
    pub fn dimension(&self) -> Option<i32> {
        match self {
            Geom::Point(_) => Some(0),
            Geom::MultiPoint(ps) => (!ps.is_empty()).then_some(0),
            Geom::LineString(cs) => (!cs.is_empty()).then_some(1),
            Geom::MultiLineString(ls) => ls.iter().any(|l| !l.is_empty()).then_some(1),
            Geom::Polygon(rings) => rings.iter().any(|r| !r.is_empty()).then_some(2),
            Geom::MultiPolygon(ps) => ps
                .iter()
                .any(|p| p.iter().any(|r| !r.is_empty()))
                .then_some(2),
            Geom::GeometryCollection(gs) => gs.iter().filter_map(|g| g.dimension()).max(),
        }
    }

    /// MySQL `ST_IsEmpty`: true when the geometry contains no coordinates.
    pub fn is_empty(&self) -> bool {
        match self {
            Geom::Point(_) => false,
            Geom::LineString(cs) => cs.is_empty(),
            Geom::Polygon(rings) => rings.iter().all(|r| r.is_empty()),
            Geom::MultiPoint(ps) => ps.is_empty(),
            Geom::MultiLineString(ls) => ls.iter().all(|l| l.is_empty()),
            Geom::MultiPolygon(ps) => ps.iter().all(|p| p.iter().all(|r| r.is_empty())),
            Geom::GeometryCollection(gs) => gs.iter().all(|g| g.is_empty()),
        }
    }

    /// Number of immediate sub-geometries for `ST_NumGeometries`. A
    /// single (non-collection) geometry reports 1.
    pub fn num_geometries(&self) -> usize {
        match self {
            Geom::MultiPoint(ps) => ps.len(),
            Geom::MultiLineString(ls) => ls.len(),
            Geom::MultiPolygon(ps) => ps.len(),
            Geom::GeometryCollection(gs) => gs.len(),
            _ => 1,
        }
    }

    /// Minimum bounding rectangle, or `None` for an empty geometry.
    pub fn mbr(&self) -> Option<Mbr> {
        let mut acc: Option<Mbr> = None;
        self.for_each_coord(&mut |c| {
            acc = Some(match acc {
                None => Mbr::of_coord(c),
                Some(mut m) => {
                    m.expand(c);
                    m
                }
            });
        });
        acc
    }

    /// Visit every coordinate in the geometry, in WKB traversal order.
    pub fn for_each_coord(&self, f: &mut impl FnMut(Coord)) {
        match self {
            Geom::Point(c) => f(*c),
            Geom::LineString(cs) | Geom::MultiPoint(cs) => cs.iter().for_each(|c| f(*c)),
            Geom::Polygon(rings) | Geom::MultiLineString(rings) => {
                rings.iter().for_each(|r| r.iter().for_each(|c| f(*c)))
            }
            Geom::MultiPolygon(ps) => ps
                .iter()
                .for_each(|p| p.iter().for_each(|r| r.iter().for_each(|c| f(*c)))),
            Geom::GeometryCollection(gs) => gs.iter().for_each(|g| g.for_each_coord(f)),
        }
    }

    /// Mutably visit every coordinate in WKB traversal order.
    pub fn for_each_coord_mut(&mut self, f: &mut impl FnMut(&mut Coord)) {
        match self {
            Geom::Point(coord) => f(coord),
            Geom::LineString(coords) | Geom::MultiPoint(coords) => coords.iter_mut().for_each(f),
            Geom::Polygon(rings) | Geom::MultiLineString(rings) => rings
                .iter_mut()
                .for_each(|ring| ring.iter_mut().for_each(&mut *f)),
            Geom::MultiPolygon(polygons) => polygons.iter_mut().for_each(|polygon| {
                polygon
                    .iter_mut()
                    .for_each(|ring| ring.iter_mut().for_each(&mut *f))
            }),
            Geom::GeometryCollection(geometries) => geometries
                .iter_mut()
                .for_each(|geometry| geometry.for_each_coord_mut(f)),
        }
    }

    /// Collect every coordinate into a flat vector (used by convex hull /
    /// centroid).
    pub fn all_coords(&self) -> Vec<Coord> {
        let mut out = Vec::new();
        self.for_each_coord(&mut |c| out.push(c));
        out
    }
}

/// A geometry value as stored/served by MySQL: an SRID plus a geometry tree.
#[derive(Debug, Clone, PartialEq)]
pub struct Geometry {
    pub srid: u32,
    pub geom: Geom,
}

impl Geometry {
    pub fn new(srid: u32, geom: Geom) -> Self {
        Geometry { srid, geom }
    }
}

/// An axis-aligned minimum bounding rectangle. Also the R-tree key.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Mbr {
    pub minx: f64,
    pub miny: f64,
    pub maxx: f64,
    pub maxy: f64,
}

impl Mbr {
    #[inline]
    pub fn new(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Self {
        Mbr {
            minx,
            miny,
            maxx,
            maxy,
        }
    }

    #[inline]
    pub fn of_coord(c: Coord) -> Self {
        Mbr {
            minx: c.x,
            miny: c.y,
            maxx: c.x,
            maxy: c.y,
        }
    }

    /// Grow the rectangle to include `c`.
    #[inline]
    pub fn expand(&mut self, c: Coord) {
        self.minx = self.minx.min(c.x);
        self.miny = self.miny.min(c.y);
        self.maxx = self.maxx.max(c.x);
        self.maxy = self.maxy.max(c.y);
    }

    /// Smallest rectangle covering both `self` and `other`.
    #[inline]
    pub fn union(&self, other: &Mbr) -> Mbr {
        Mbr {
            minx: self.minx.min(other.minx),
            miny: self.miny.min(other.miny),
            maxx: self.maxx.max(other.maxx),
            maxy: self.maxy.max(other.maxy),
        }
    }

    /// True when the two rectangles share any point (closed boxes — touching
    /// edges count as intersecting, matching MySQL MBR semantics).
    #[inline]
    pub fn intersects(&self, other: &Mbr) -> bool {
        self.minx <= other.maxx
            && self.maxx >= other.minx
            && self.miny <= other.maxy
            && self.maxy >= other.miny
    }

    /// True when `self` fully covers `other` (closed boxes).
    #[inline]
    pub fn contains(&self, other: &Mbr) -> bool {
        self.minx <= other.minx
            && self.maxx >= other.maxx
            && self.miny <= other.miny
            && self.maxy >= other.maxy
    }

    #[inline]
    pub fn equals(&self, other: &Mbr) -> bool {
        self.minx == other.minx
            && self.miny == other.miny
            && self.maxx == other.maxx
            && self.maxy == other.maxy
    }

    /// Rectangle area (used by the R-tree split heuristics).
    #[inline]
    pub fn area(&self) -> f64 {
        (self.maxx - self.minx) * (self.maxy - self.miny)
    }

    /// Area increase required to enlarge `self` to also cover `other` — the
    /// R-tree choose-subtree cost.
    #[inline]
    pub fn enlargement(&self, other: &Mbr) -> f64 {
        self.union(other).area() - self.area()
    }
}
