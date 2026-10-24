//! Unit tests for the geometry codec and algorithms.

use super::types::*;
use super::*;

fn g(wkt: &str) -> Geom {
    parse_wkt(wkt).unwrap_or_else(|e| panic!("parse {wkt}: {}", e.message()))
}

#[test]
fn wkt_roundtrip_all_types() {
    let cases = [
        "POINT(1 2)",
        "LINESTRING(0 0,1 1,2 0)",
        "POLYGON((0 0,4 0,4 4,0 4,0 0))",
        "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))",
        "MULTIPOINT(0 0,1 1)",
        "MULTILINESTRING((0 0,1 1),(2 2,3 3))",
        "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))",
        "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
        "LINESTRING EMPTY",
        "GEOMETRYCOLLECTION EMPTY",
    ];
    for wkt in cases {
        let geom = g(wkt);
        let out = write_wkt(&geom);
        assert_eq!(out, wkt, "WKT round-trip mismatch");
    }
}

#[test]
fn wkb_internal_roundtrip_with_srid() {
    let internal = parse_wkt_internal("POINT(3 4)", 4326).unwrap();
    // SRID prefix is little-endian 4326.
    assert_eq!(&internal[0..4], &4326u32.to_le_bytes());
    let parsed = read_geometry(&internal).unwrap();
    assert_eq!(parsed.srid, 4326);
    assert_eq!(parsed.geom, Geom::Point(Coord { x: 3.0, y: 4.0 }));
    // Re-encode is byte-stable.
    assert_eq!(write_geometry(&parsed), internal);
    assert_eq!(to_text(&internal), "POINT(3 4)");
}

#[test]
fn wkb_reads_big_endian() {
    // Big-endian (XDR) POINT(1 2): order=00, type=1, x=1.0, y=2.0.
    let mut wkb = vec![0x00];
    wkb.extend_from_slice(&1u32.to_be_bytes());
    wkb.extend_from_slice(&1.0f64.to_be_bytes());
    wkb.extend_from_slice(&2.0f64.to_be_bytes());
    let geom = read_wkb(&wkb).unwrap();
    assert_eq!(geom, Geom::Point(Coord { x: 1.0, y: 2.0 }));
    // Writer always emits little-endian.
    let le = write_wkb(&geom);
    assert_eq!(le[0], 0x01);
}

#[test]
fn wkb_rejects_nan_coordinates_in_both_byte_orders_and_nested_geometries() {
    for little_endian in [true, false] {
        let mut point = vec![u8::from(little_endian)];
        if little_endian {
            point.extend_from_slice(&1u32.to_le_bytes());
            point.extend_from_slice(&f64::NAN.to_le_bytes());
            point.extend_from_slice(&1.0f64.to_le_bytes());
        } else {
            point.extend_from_slice(&1u32.to_be_bytes());
            point.extend_from_slice(&f64::NAN.to_be_bytes());
            point.extend_from_slice(&1.0f64.to_be_bytes());
        }
        let err = read_wkb(&point).unwrap_err();
        assert_eq!(
            err.code(),
            crate::error::catalog::ER_GIS_INVALID_DATA.code().get()
        );

        let mut internal = 0u32.to_le_bytes().to_vec();
        internal.extend_from_slice(&point);
        assert!(normalize_internal(&internal).is_err());

        let mut collection = vec![1u8];
        collection.extend_from_slice(&7u32.to_le_bytes());
        collection.extend_from_slice(&1u32.to_le_bytes());
        collection.extend_from_slice(&point);
        assert!(read_wkb(&collection).is_err());
    }
    let mut infinite = vec![1u8];
    infinite.extend_from_slice(&1u32.to_le_bytes());
    infinite.extend_from_slice(&f64::INFINITY.to_le_bytes());
    infinite.extend_from_slice(&1.0f64.to_le_bytes());
    // All constructors now reject non-finite coordinates consistently;
    // topology kernels must never receive infinities either.
    assert_eq!(
        read_wkb(&infinite).unwrap_err().code(),
        crate::error::catalog::ER_GIS_INVALID_DATA.code().get()
    );
    infinite[5..13].copy_from_slice(&f64::NEG_INFINITY.to_le_bytes());
    assert!(read_wkb(&infinite).is_err());
}

#[test]
fn rejects_3d_and_garbage() {
    // ISO Z point (type 1001) must be rejected.
    let mut wkb = vec![0x01];
    wkb.extend_from_slice(&1001u32.to_le_bytes());
    wkb.extend_from_slice(&[0u8; 24]);
    assert!(read_wkb(&wkb).is_err());
    assert!(parse_wkt("NONSENSE(1 2)").is_err());
    assert!(parse_wkt("POINT(1)").is_err());
}

#[test]
fn geojson_roundtrip() {
    let geom = g("POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))");
    let gj = to_geojson(&geom);
    assert!(gj.contains("\"type\":\"Polygon\""));
    let back = from_geojson(&gj, 4326).unwrap();
    assert_eq!(back.srid, 4326);
    assert_eq!(back.geom, geom);

    let pt = from_geojson("{\"type\":\"Point\",\"coordinates\":[1,2]}", 0).unwrap();
    assert_eq!(pt.geom, Geom::Point(Coord { x: 1.0, y: 2.0 }));
}

#[test]
fn area_length_distance() {
    assert_eq!(area(&g("POLYGON((0 0,4 0,4 4,0 4,0 0))")), 16.0);
    assert_eq!(
        area(&g("POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))")),
        15.0
    );
    assert_eq!(area(&g("LINESTRING(0 0,1 1)")), 0.0);

    assert_eq!(length(&g("LINESTRING(0 0,3 0,3 4)")), 7.0);
    assert_eq!(length(&g("MULTILINESTRING((0 0,3 0),(0 0,0 4))")), 7.0);

    assert_eq!(distance(&g("POINT(0 0)"), &g("POINT(3 4)")), Some(5.0));
    // Point inside a polygon has distance 0.
    assert_eq!(
        distance(&g("POINT(2 2)"), &g("POLYGON((0 0,4 0,4 4,0 4,0 0))")),
        Some(0.0)
    );
    // Disjoint point/polygon: nearest boundary distance.
    assert_eq!(
        distance(&g("POINT(6 2)"), &g("POLYGON((0 0,4 0,4 4,0 4,0 0))")),
        Some(2.0)
    );
}

#[test]
fn distance_sphere_haversine() {
    // ~1 degree of longitude at the equator ≈ 111 km. Use MySQL's default
    // earth radius; assert it is in a sane range.
    let d = distance_sphere(&g("POINT(0 0)"), &g("POINT(1 0)"), DEFAULT_SPHERE_RADIUS).unwrap();
    assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    // Non-point geometry is unsupported → None.
    assert_eq!(
        distance_sphere(
            &g("LINESTRING(0 0,1 1)"),
            &g("POINT(0 0)"),
            DEFAULT_SPHERE_RADIUS
        ),
        None
    );
}

#[test]
fn centroid_envelope_hull() {
    assert_eq!(
        centroid(&g("POLYGON((0 0,4 0,4 4,0 4,0 0))")),
        Some(Coord { x: 2.0, y: 2.0 })
    );
    assert_eq!(
        centroid(&g("MULTIPOINT(0 0,2 0,2 2,0 2)")),
        Some(Coord { x: 1.0, y: 1.0 })
    );

    assert_eq!(
        write_wkt(&envelope(&g("POINT(1 1)")).unwrap()),
        "POINT(1 1)"
    );
    assert_eq!(
        write_wkt(&envelope(&g("POLYGON((0 0,4 0,4 4,0 4,0 0))")).unwrap()),
        "POLYGON((0 0,4 0,4 4,0 4,0 0))"
    );

    // Convex hull of a square plus an interior point is the square (area 1).
    let hull = convex_hull(&g("MULTIPOINT(0 0,1 0,1 1,0 1,0.5 0.5)")).unwrap();
    assert_eq!(area(&hull), 1.0);
    // Collinear points collapse to a line.
    assert_eq!(
        convex_hull(&g("MULTIPOINT(0 0,1 1,2 2)")).unwrap(),
        Geom::LineString(vec![Coord { x: 0.0, y: 0.0 }, Coord { x: 2.0, y: 2.0 }])
    );
}

#[test]
fn mbr_predicates() {
    let poly = g("POLYGON((0 0,4 0,4 4,0 4,0 0))");
    let inside = g("POINT(2 2)");
    let outside = g("POINT(9 9)");
    assert_eq!(mbr_predicate(MbrOp::Contains, &poly, &inside), Some(true));
    assert_eq!(mbr_predicate(MbrOp::Within, &inside, &poly), Some(true));
    assert_eq!(
        mbr_predicate(MbrOp::Intersects, &poly, &outside),
        Some(false)
    );
    assert_eq!(mbr_predicate(MbrOp::Disjoint, &poly, &outside), Some(true));
    let same = g("POLYGON((0 0,4 0,4 4,0 4,0 0))");
    assert_eq!(mbr_predicate(MbrOp::Equals, &poly, &same), Some(true));
}

#[test]
fn st_predicates() {
    let poly = g("POLYGON((0 0,4 0,4 4,0 4,0 0))");
    // Strictly inside vs on boundary.
    assert_eq!(
        st_predicate(StOp::Contains, &poly, &g("POINT(2 2)")),
        Some(true)
    );
    assert_eq!(
        st_predicate(StOp::Contains, &poly, &g("POINT(2 0)")),
        Some(false),
        "a boundary point is not contained"
    );
    assert_eq!(
        st_predicate(StOp::Within, &g("POINT(2 2)"), &poly),
        Some(true)
    );
    assert_eq!(
        st_predicate(StOp::Disjoint, &poly, &g("POINT(9 9)")),
        Some(true)
    );
    assert_eq!(
        st_predicate(StOp::Intersects, &poly, &g("POINT(2 2)")),
        Some(true)
    );

    // Crossing lines.
    assert_eq!(
        st_predicate(
            StOp::Intersects,
            &g("LINESTRING(0 0,4 4)"),
            &g("LINESTRING(0 4,4 0)")
        ),
        Some(true)
    );
    assert_eq!(
        st_predicate(
            StOp::Crosses,
            &g("LINESTRING(0 0,4 4)"),
            &g("LINESTRING(0 4,4 0)")
        ),
        Some(true)
    );

    // Equality.
    assert_eq!(
        st_predicate(StOp::Equals, &g("POINT(1 1)"), &g("POINT(1 1)")),
        Some(true)
    );
    assert_eq!(
        st_predicate(StOp::Equals, &g("POINT(1 1)"), &g("POINT(1 2)")),
        Some(false)
    );
}

#[test]
fn geom_metadata() {
    let p = g("POINT(1 2)");
    assert_eq!(p.type_name(), "POINT");
    assert_eq!(p.dimension(), Some(0));
    assert!(!p.is_empty());

    let poly = g("POLYGON((0 0,4 0,4 4,0 4,0 0))");
    assert_eq!(poly.dimension(), Some(2));
    let m = poly.mbr().unwrap();
    assert_eq!((m.minx, m.miny, m.maxx, m.maxy), (0.0, 0.0, 4.0, 4.0));

    assert!(g("LINESTRING EMPTY").is_empty());
    assert_eq!(g("MULTIPOINT(0 0,1 1,2 2)").num_geometries(), 3);
}
