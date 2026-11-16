//! Unit tests for the geometry codec and algorithms.

use super::types::*;
use super::*;

fn g(wkt: &str) -> Geom {
    parse_wkt(wkt).unwrap_or_else(|e| panic!("parse {wkt}: {}", e.message()))
}

#[test]
fn contains_point_targets_match_topology_across_shapes_and_boundaries() {
    let containers = [
        "POINT(2 2)", "MULTIPOINT(0 0,2 2,4 4)",
        "LINESTRING(0 0,4 4)", "LINESTRING(0 0,2 2,4 0)",
        "LINESTRING(0 0,4 0,4 4,0 0)", "LINESTRING(0 0,0 0)",
        "LINESTRING(0 0,0 0,2 2,2 2,4 4)",
        "POLYGON((0 0,4 0,4 4,0 4,0 0))",
        "POLYGON((0 0,0 4,4 4,4 0,0 0))",
        "POLYGON((0 0,4 0,4 4,2 2,0 4,0 0))",
        "POLYGON((0 0,6 0,6 6,0 6,0 0),(2 2,4 2,4 4,2 4,2 2))",
        "MULTILINESTRING((0 0,2 2),(2 2,4 0))",
        "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((2 2,4 2,4 4,2 4,2 2)))",
        "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1)),((5 0,7 0,7 4,5 4,5 0)))",
        "MULTIPOLYGON(((0 0,2 0,2 4,0 4,0 0)),((2 0,4 0,4 4,2 4,2 0)))",
        "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0)),((2 2,6 2,6 6,2 6,2 2)))",
        "GEOMETRYCOLLECTION(POLYGON((0 0,2 0,2 2,0 2,0 0)),POLYGON((2 0,4 0,4 2,2 2,2 0)),POINT(5 5))",
        "LINESTRING EMPTY", "POLYGON EMPTY", "MULTIPOINT EMPTY", "GEOMETRYCOLLECTION EMPTY",
    ];
    let mut targets = Vec::new();
    for x in -2..=14 {
        for y in -2..=14 {
            targets.push(Geom::Point(Coord {
                x: x as f64 / 2.0,
                y: y as f64 / 2.0,
            }));
        }
    }
    targets.extend([
        g("MULTIPOINT EMPTY"),
        g("MULTIPOINT(0 0,2 2)"),
        g("MULTIPOINT(0 0,4 0)"),
        g("MULTIPOINT(2 2,7 7)"),
        g("MULTIPOINT(2 2,2 2)"),
        g("MULTIPOINT(1 1,3 3,4 4)"),
        g("MULTIPOINT(1 1,2 2)"),
        g("MULTIPOINT(1 1,3 3)"),
        g("MULTIPOINT(1 1,6 3)"),
        g("LINESTRING(1 1,3 3)"),
        g("POLYGON((1 1,3 1,3 3,1 3,1 1))"),
    ]);
    for text in containers {
        let container = g(text);
        for target in &targets {
            let expected = if container.is_empty() || target.is_empty() {
                None
            } else {
                Some(super::relate::contains_topology(&container, target))
            };
            assert_eq!(
                st_predicate(StOp::Contains, &container, target),
                expected,
                "{text} contains {target:?}"
            );
            assert_eq!(
                st_predicate(StOp::Within, target, &container),
                expected,
                "{target:?} within {text}"
            );
            let intersection = if container.is_empty() || target.is_empty() {
                None
            } else {
                Some(super::relate::intersects_reference(&container, target))
            };
            assert_eq!(
                st_predicate(StOp::Intersects, &container, target),
                intersection,
                "{text} intersects {target:?}"
            );
            assert_eq!(
                st_predicate(StOp::Intersects, target, &container),
                intersection,
                "{target:?} intersects {text}"
            );
            assert_eq!(
                st_predicate(StOp::Disjoint, &container, target),
                intersection.map(|value| !value),
                "{text} disjoint {target:?}"
            );
        }
    }
}

#[test]
fn contains_point_targets_match_topology_at_extreme_scales_and_near_edges() {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5fac1a1);
    for text in [
        "POLYGON((0 0,8 0,8 8,0 8,0 0),(2 2,5 2,5 5,2 5,2 2))",
        "POLYGON((0 0,8 3,5 8,4 4,0 8,0 0))",
        "LINESTRING(0 0,4 4,8 0,8 8)",
        "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1)),((5 0,8 0,8 8,5 8,5 0)))",
    ] {
        for (scale, shift) in [
            (1e-100, 0.0),
            (1e-8, 1234.0),
            (1.0, 0.0),
            (1e50, 1e51),
            (1e100, 0.0),
        ] {
            let mut container = g(text);
            container.for_each_coord_mut(&mut |c| {
                c.x = c.x * scale + shift;
                c.y = c.y * scale - shift;
            });
            let mut points = container.all_coords();
            for _ in 0..100 {
                points.push(Coord {
                    x: rng.gen_range(-1.0..9.0) * scale + shift,
                    y: rng.gen_range(-1.0..9.0) * scale - shift,
                });
            }
            for x in [0.0, 2.0, 4.0, 5.0, 8.0] {
                for delta in [-f64::EPSILON * 8.0, 0.0, f64::EPSILON * 8.0] {
                    points.push(Coord {
                        x: (x + delta) * scale + shift,
                        y: 4.0 * scale - shift,
                    });
                }
            }
            for point in points {
                let target = Geom::Point(point);
                assert_eq!(
                    st_predicate(StOp::Contains, &container, &target),
                    Some(super::relate::contains_topology(&container, &target)),
                    "{text}, scale {scale}, shift {shift}, point {point:?}"
                );
            }
        }
    }
}

#[test]
fn borrowed_spatial_predicates_preserve_owned_results_and_errors() {
    let mut values = [
        "POINT(1 1)",
        "POINT(0 0)",
        "MULTIPOINT(1 1,2 2)",
        "LINESTRING(0 0,4 4)",
        "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))",
        "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((2 2,4 2,4 4,2 4,2 2)))",
        "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,4 4))",
        "GEOMETRYCOLLECTION EMPTY",
    ]
    .into_iter()
    .map(|wkt| {
        crate::types::Value::Geometry(
            write_geometry(&Geometry {
                srid: 0,
                geom: g(wkt),
            })
            .unwrap(),
        )
    })
    .collect::<Vec<_>>();
    for srid in [3857, 4326] {
        values.push(crate::types::Value::Geometry(
            write_geometry(&Geometry {
                srid,
                geom: g("POINT(1 1)"),
            })
            .unwrap(),
        ));
        values.push(crate::types::Value::Geometry(
            write_geometry(&Geometry {
                srid,
                geom: g("POLYGON((0 0,2 0,2 2,0 2,0 0))"),
            })
            .unwrap(),
        ));
    }
    values.extend([
        crate::types::Value::Null,
        crate::types::Value::Bytes(vec![0]),
        crate::types::Value::String("POINT(1 1)".into()),
    ]);
    for name in [
        "MBRCONTAINS",
        "MBRWITHIN",
        "MBRINTERSECTS",
        "MBRDISJOINT",
        "MBREQUALS",
        "MBRTOUCHES",
        "MBROVERLAPS",
        "MBRCOVEREDBY",
        "MBRCOVERS",
        "ST_INTERSECTS",
        "ST_DISJOINT",
        "ST_CONTAINS",
        "ST_WITHIN",
        "ST_EQUALS",
        "ST_TOUCHES",
        "ST_CROSSES",
        "ST_OVERLAPS",
    ] {
        let predicate = sql::function_spec(name)
            .unwrap()
            .binary_predicate()
            .unwrap();
        for left in &values {
            for right in &values {
                let expected = sql::eval(name, name, &[left.clone(), right.clone()]);
                let actual = predicate.eval(name, left, right);
                match (expected, actual) {
                    (Ok(expected), Ok(actual)) => {
                        assert_eq!(actual, expected, "{name}: {left:?} / {right:?}")
                    }
                    (Err(expected), Err(actual)) => {
                        assert_eq!(actual.code(), expected.code());
                        assert_eq!(actual.message(), expected.message());
                    }
                    (expected, actual) => panic!("{name}: owned {expected:?}, borrowed {actual:?}"),
                }
            }
        }
    }
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
    assert_eq!(write_geometry(&parsed).unwrap(), internal);
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
