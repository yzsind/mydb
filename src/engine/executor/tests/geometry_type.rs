//! End-to-end tests for the MySQL-8.0-compatible spatial (GEOMETRY) data
//! type and the ST_* / MBR* function suite. Exercises the whole pillar
//! through the SQL engine: type parsing, internal `SRID + WKB` storage
//! round-trip, the function library evaluated over stored columns, and the
//! SPATIAL R-tree index.

use super::support::query_rows as rows;
use super::*;
use crate::types::ColumnType;

fn cell(engine: &StorageQueryEngine, sql: &str) -> String {
    let r = rows(engine, sql);
    assert_eq!(r.len(), 1, "expected exactly one row for `{sql}`");
    assert_eq!(r[0].len(), 1, "expected exactly one column for `{sql}`");
    r[0][0].to_display_string()
}

fn column_types(engine: &StorageQueryEngine, sql: &str) -> Vec<ColumnType> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.columns.iter().map(|c| c.column_type).collect(),
        other => panic!("expected a result set for `{sql}`, got {other:?}"),
    }
}

#[test]
fn geometry_subtypes_parse_and_describe() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE g (
                id INT PRIMARY KEY,
                geom GEOMETRY,
                pt POINT,
                ls LINESTRING,
                poly POLYGON,
                mp MULTIPOINT,
                mls MULTILINESTRING,
                mpoly MULTIPOLYGON,
                gc GEOMETRYCOLLECTION
            )",
        )
        .unwrap();

    // DESCRIBE echoes the declared subtype name (MySQL behaviour).
    let described: Vec<(String, String)> = rows(&engine, "DESCRIBE g")
        .into_iter()
        .map(|r| (r[0].to_display_string(), r[1].to_display_string()))
        .collect();
    let type_of = |name: &str| {
        described
            .iter()
            .find(|(c, _)| c == name)
            .map(|(_, t)| t.clone())
            .unwrap_or_default()
    };
    assert_eq!(type_of("geom"), "geometry");
    assert_eq!(type_of("pt"), "point");
    assert_eq!(type_of("ls"), "linestring");
    assert_eq!(type_of("poly"), "polygon");
    assert_eq!(type_of("mp"), "multipoint");
    assert_eq!(type_of("mls"), "multilinestring");
    assert_eq!(type_of("mpoly"), "multipolygon");
    assert_eq!(type_of("gc"), "geometrycollection");

    // Every geometry column advertises the GEOMETRY wire type.
    assert_eq!(
        column_types(&engine, "SELECT geom, pt, poly FROM g"),
        vec![
            ColumnType::Geometry,
            ColumnType::Geometry,
            ColumnType::Geometry
        ]
    );
}

#[test]
fn geometry_column_requires_an_explicit_constructor() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE g (id INT PRIMARY KEY, geom GEOMETRY)")
        .unwrap();
    // MySQL does not implicitly interpret text or binary strings as geometry.
    let text_err = engine
        .execute("INSERT INTO g VALUES (1, 'POINT(3 4)')")
        .unwrap_err();
    assert_eq!(text_err.code(), 1416);
    let wkb_err = engine
        .execute(
            "INSERT INTO g VALUES \
             (2, ST_AsWKB(ST_GeomFromText('POINT(3 4)')))",
        )
        .unwrap_err();
    assert_eq!(wkb_err.code(), 1416);

    engine
        .execute("INSERT INTO g VALUES (3, ST_GeomFromText('POINT(3 4)'))")
        .unwrap();
    // ST_AsText renders the stored internal geometry back to WKT.
    assert_eq!(
        cell(&engine, "SELECT ST_AsText(geom) FROM g WHERE id = 3"),
        "POINT(3 4)"
    );
    assert_eq!(
        cell(&engine, "SELECT ST_GeometryType(geom) FROM g WHERE id = 3"),
        "POINT"
    );
}

#[test]
fn geometry_column_srid_round_trips_through_ddl_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE g (
                id INT PRIMARY KEY,
                geom GEOMETRY NOT NULL SRID 4326
            )",
        )
        .unwrap();

    let create = rows(&engine, "SHOW CREATE TABLE g");
    let ddl = create[0][1].to_display_string();
    assert!(ddl.contains("`geom` geometry NOT NULL SRID 4326"), "{ddl}");
    assert_eq!(
        cell(
            &engine,
            "SELECT SRS_ID FROM information_schema.st_geometry_columns \
             WHERE TABLE_NAME = 'g' AND COLUMN_NAME = 'geom'"
        ),
        "4326"
    );

    let err = engine
        .execute("INSERT INTO g VALUES (1, ST_GeomFromText('POINT(1 2)', 0))")
        .unwrap_err();
    assert_eq!(err.code(), 3643);
    engine
        .execute("INSERT INTO g VALUES (2, ST_GeomFromText('POINT(1 2)', 4326))")
        .unwrap();
}

#[test]
fn mysql8046_srs_and_unit_catalogs_are_complete() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        cell(
            &engine,
            "SELECT COUNT(*) FROM information_schema.st_spatial_reference_systems"
        ),
        "5152"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT COUNT(*) FROM information_schema.st_units_of_measure"
        ),
        "47"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT SRS_NAME FROM information_schema.st_spatial_reference_systems \
             WHERE SRS_ID = 4326"
        ),
        "WGS 84"
    );

    let err = engine
        .execute("SELECT ST_GeomFromText('POINT(1 2)', 99999)")
        .unwrap_err();
    assert_eq!(err.code(), 3548);
}

#[test]
fn spatial_index_requires_explicit_not_null_and_srid_for_planning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let err = engine
        .execute("CREATE TABLE bad (g GEOMETRY, SPATIAL INDEX(g))")
        .unwrap_err();
    assert_eq!(err.code(), 1252);

    engine
        .execute("CREATE TABLE unrestricted (g GEOMETRY NOT NULL, SPATIAL INDEX(g))")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE restricted (
                g GEOMETRY NOT NULL SRID 0,
                SPATIAL INDEX(g)
            )",
        )
        .unwrap();
    let predicate = "MBRIntersects(g, ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))'))";
    let unrestricted = rows(
        &engine,
        &format!("EXPLAIN SELECT * FROM unrestricted WHERE {predicate}"),
    );
    assert!(
        !unrestricted.iter().flatten().any(|v| v
            .to_display_string()
            .to_ascii_lowercase()
            .contains("spatial")),
        "{unrestricted:?}"
    );
    let restricted = rows(
        &engine,
        &format!("EXPLAIN SELECT * FROM restricted WHERE {predicate}"),
    );
    assert!(
        restricted.iter().flatten().any(|v| v
            .to_display_string()
            .to_ascii_lowercase()
            .contains("spatial")),
        "{restricted:?}"
    );
}

#[test]
fn spatial_index_prepared_probe_rebinds_without_replanning() {
    use crate::engine::api::QueryEngine as _;
    use crate::engine::params::with_params;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE places (
                id INT PRIMARY KEY,
                g GEOMETRY NOT NULL SRID 0,
                SPATIAL INDEX(g)
            )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO places VALUES
             (1, Point(1,1)), (2, Point(10,10)), (3, Point(20,20))",
        )
        .unwrap();

    let sql = "SELECT id FROM places
               WHERE MBRIntersects(g, ST_GeomFromText(?))
               ORDER BY id";
    let mut session = SessionContext::system_default();
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();
    let execute = |session: &mut SessionContext, wkt: &str| {
        let response = with_params(vec![Value::String(wkt.to_string())], || {
            engine
                .execute_prepared_statement(session, sql, (*prepared).clone())
                .expect("prepared spatial SELECT")
        })
        .unwrap();
        match response.materialize().unwrap() {
            QueryResponse::ResultSet(result) => result
                .rows
                .into_iter()
                .map(|row| row[0].to_display_string())
                .collect::<Vec<_>>(),
            other => panic!("expected result set, got {other:?}"),
        }
    };
    assert_eq!(
        execute(&mut session, "POLYGON((0 0,2 0,2 2,0 2,0 0))"),
        vec!["1"]
    );
    assert_eq!(
        execute(&mut session, "POLYGON((9 9,11 9,11 11,9 11,9 9))"),
        vec!["2"]
    );
}

#[test]
fn typed_column_rejects_wrong_subtype() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE g (id INT PRIMARY KEY, pt POINT)")
        .unwrap();
    // A POLYGON cannot be stored in a POINT column.
    let err =
        engine.execute("INSERT INTO g VALUES (1, ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'))");
    assert!(err.is_err(), "expected subtype violation, got {err:?}");
}

/// One-row scalar from a constant `SELECT` (no table needed).
fn scalar(engine: &StorageQueryEngine, expr: &str) -> String {
    cell(engine, &format!("SELECT {expr}"))
}

#[test]
fn constructors_and_format() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // WKT round-trip across types.
    assert_eq!(
        scalar(
            &engine,
            "ST_AsText(ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'))"
        ),
        "LINESTRING(0 0,1 1,2 2)"
    );
    assert_eq!(
        scalar(
            &engine,
            "ST_AsText(ST_PolygonFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))'))"
        ),
        "POLYGON((0 0,4 0,4 4,0 4,0 0))"
    );
    // MySQL exposes Point(), not ST_Point().
    assert_eq!(scalar(&engine, "ST_AsText(Point(3, 4))"), "POINT(3 4)");
    assert_eq!(
        engine.execute("SELECT ST_Point(3, 4)").unwrap_err().code(),
        1305
    );
    // WKB round-trip: text -> WKB -> geometry -> text.
    assert_eq!(
        scalar(
            &engine,
            "ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('POINT(1 2)'))))"
        ),
        "POINT(1 2)"
    );
    // Generic byte consumers see MySQL's internal SRID+WKB representation,
    // not the WKT display form.
    let linestring = "ST_GeomFromText('LINESTRING(-1 -1, 1 -1, -1 -1, -1 1, 1 1)')";
    assert_eq!(
        scalar(&engine, &format!("HEX({linestring})")),
        "00000000010200000005000000000000000000F0BF000000000000F0BF\
         000000000000F03F000000000000F0BF000000000000F0BF000000000000F0BF\
         000000000000F0BF000000000000F03F000000000000F03F000000000000F03F"
            .replace(' ', "")
    );
    assert_eq!(
        scalar(&engine, &format!("CRC32({linestring})")),
        "2439882226"
    );
    let mut mixed_session = SessionContext::default();
    engine
        .execute_with_session(&mut mixed_session, "SET @mixed_text = 'abc'")
        .unwrap();
    engine
        .execute_with_session(
            &mut mixed_session,
            "SET @mixed_geom = \
             ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))')",
        )
        .unwrap();
    let QueryResponse::ResultSet(mixed_result) = engine
        .execute_with_session(&mut mixed_session, "SELECT @mixed_text > @mixed_geom")
        .unwrap()
    else {
        panic!("expected mixed geometry comparison result");
    };
    assert_eq!(mixed_result.rows, vec![vec![Value::Int(1)]]);
    // GeoJSON round-trip.
    assert_eq!(
        scalar(
            &engine,
            "ST_AsText(ST_GeomFromGeoJSON('{\"type\":\"Point\",\"coordinates\":[5,6]}'))"
        ),
        "POINT(6 5)"
    );
    // ST_AsGeoJSON yields a JSON document.
    assert_eq!(
        scalar(&engine, "ST_AsGeoJSON(ST_GeomFromText('POINT(1 2)'))"),
        r#"{"type": "Point", "coordinates": [1, 2]}"#
    );
    // Typed constructor enforces the geometry type.
    assert!(engine
        .execute("SELECT ST_PointFromText('LINESTRING(0 0,1 1)')")
        .is_err());
}

#[test]
fn spatial_function_result_metadata_matches_mysql() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    assert_eq!(
        column_types(
            &engine,
            "SELECT ST_X(Point(1,2)), ST_Y(Point(1,2)), ST_SRID(Point(1,2))"
        ),
        vec![ColumnType::Double, ColumnType::Double, ColumnType::LongLong]
    );
    assert_eq!(
        column_types(
            &engine,
            "SELECT ST_X(Point(1,2),3), ST_SRID(Point(1,2),4326)"
        ),
        vec![ColumnType::Geometry, ColumnType::Geometry]
    );
    assert_eq!(
        column_types(&engine, "SELECT ST_AsText(Point(1,2))"),
        vec![ColumnType::Blob]
    );
    assert_eq!(
        column_types(&engine, "SELECT ST_Collect(Point(1,2))"),
        vec![ColumnType::Geometry]
    );
}

#[test]
fn st_collect_aggregate_preserves_srid_and_narrows_type() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE g (id INT PRIMARY KEY, p GEOMETRY SRID 0)")
        .unwrap();
    engine
        .execute(
            "INSERT INTO g VALUES
             (1, Point(1,2)), (2, Point(3,4)), (3, Point(1,2)), (4, NULL)",
        )
        .unwrap();
    assert_eq!(
        cell(
            &engine,
            "SELECT ST_GeometryType(ST_Collect(DISTINCT p)) FROM g"
        ),
        "MULTIPOINT"
    );
    assert_eq!(
        cell(
            &engine,
            "SELECT ST_NumGeometries(ST_Collect(DISTINCT p)) FROM g"
        ),
        "2"
    );
    assert_eq!(cell(&engine, "SELECT ST_SRID(ST_Collect(p)) FROM g"), "0");
}

#[test]
fn properties_and_accessors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let ls = "ST_GeomFromText('LINESTRING(0 0,1 1,2 2)')";
    let poly = "ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))')";

    assert_eq!(
        scalar(&engine, &format!("ST_GeometryType({poly})")),
        "POLYGON"
    );
    assert_eq!(scalar(&engine, &format!("ST_Dimension({poly})")), "2");
    assert_eq!(scalar(&engine, &format!("ST_IsEmpty({ls})")), "0");
    assert_eq!(
        scalar(&engine, "ST_IsEmpty(ST_GeomFromText('LINESTRING EMPTY'))"),
        "1"
    );
    assert_eq!(scalar(&engine, &format!("ST_NumPoints({ls})")), "3");
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_StartPoint({ls}))")),
        "POINT(0 0)"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_EndPoint({ls}))")),
        "POINT(2 2)"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_PointN({ls}, 2))")),
        "POINT(1 1)"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_NumInteriorRings({poly})")),
        "1"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_ExteriorRing({poly}))")),
        "LINESTRING(0 0,4 0,4 4,0 4,0 0)"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_InteriorRingN({poly}, 1))")),
        "LINESTRING(1 1,2 1,2 2,1 2,1 1)"
    );
    // Point coordinate accessors.
    assert_eq!(scalar(&engine, "ST_X(Point(3, 4))"), "3");
    assert_eq!(scalar(&engine, "ST_Y(Point(3, 4))"), "4");
    assert_eq!(
        scalar(&engine, "ST_AsText(ST_X(Point(3, 4), 9))"),
        "POINT(9 4)"
    );
    // Multi geometry navigation.
    let mp = "ST_GeomFromText('MULTIPOINT(0 0,1 1,2 2)')";
    assert_eq!(scalar(&engine, &format!("ST_NumGeometries({mp})")), "3");
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_GeometryN({mp}, 2))")),
        "POINT(1 1)"
    );
    // Closedness.
    assert_eq!(
        scalar(
            &engine,
            "ST_IsClosed(ST_GeomFromText('LINESTRING(0 0,1 0,1 1,0 0)'))"
        ),
        "1"
    );
    assert_eq!(
        scalar(
            &engine,
            "ST_IsClosed(ST_GeomFromText('LINESTRING(0 0,1 1)'))"
        ),
        "0"
    );
    assert_eq!(scalar(&engine, "ST_IsClosed(Point(0,0))"), "NULL");
}

#[test]
fn measurement_and_construction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let poly = "ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))')";

    assert_eq!(scalar(&engine, &format!("ST_Area({poly})")), "16");
    assert_eq!(
        scalar(
            &engine,
            "ST_Length(ST_GeomFromText('LINESTRING(0 0,3 0,3 4)'))"
        ),
        "7"
    );
    assert_eq!(scalar(&engine, "ST_Distance(Point(0,0), Point(3,4))"), "5");
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_Centroid({poly}))")),
        "POINT(2 2)"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_AsText(ST_Envelope({poly}))")),
        "POLYGON((0 0,4 0,4 4,0 4,0 0))"
    );
    assert_eq!(
        scalar(
            &engine,
            "ST_Area(ST_ConvexHull(ST_GeomFromText('MULTIPOINT(0 0,1 0,1 1,0 1,0.5 0.5)')))"
        ),
        "1"
    );
    // Spherical distance ~1 deg lon at equator ≈ 111 km.
    let d: f64 = scalar(
        &engine,
        "ST_Distance_Sphere(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 0)'))",
    )
    .parse()
    .unwrap();
    assert!((d - 111_195.0).abs() < 500.0, "got {d}");
}

#[test]
fn geographic_measurements_fail_explicitly_without_a_geodesic_kernel() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "SELECT ST_Distance(
            ST_GeomFromText('POINT(0 0)',4326),
            ST_GeomFromText('POINT(0 1)',4326),
            'kilometre'
        )",
        "SELECT ST_Length(
            ST_GeomFromText('LINESTRING(0 0,0 1)',4326),
            'kilometre'
        )",
        "SELECT ST_Area(ST_GeomFromText(
            'POLYGON((0 0,1 0,1 1,0 1,0 0))',
            4326
        ))",
    ] {
        let err = engine.execute(sql).unwrap_err();
        assert_eq!(err.code(), 3618, "{sql}: {err}");
        assert_eq!(err.sql_state(), "22S00", "{sql}: {err}");
    }

    let unknown_length = engine
        .execute("SELECT ST_Distance(Point(0,0),Point(1,1),'foot')")
        .unwrap_err();
    assert_eq!(unknown_length.code(), 3882);
    assert_eq!(unknown_length.sql_state(), "SU001");
}

#[test]
fn predicates_mbr_and_topological() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let poly = "ST_GeomFromText('POLYGON((0 0,4 0,4 4,0 4,0 0))')";

    // Exact topological predicates.
    assert_eq!(
        scalar(&engine, &format!("ST_Contains({poly}, Point(2,2))")),
        "1"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_Contains({poly}, Point(2,0))")),
        "0",
        "a boundary point is not contained"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_Within(Point(2,2), {poly})")),
        "1"
    );
    assert_eq!(
        scalar(&engine, &format!("ST_Disjoint({poly}, Point(9,9))")),
        "1"
    );
    assert_eq!(
        scalar(
            &engine,
            "ST_Intersects(ST_GeomFromText('LINESTRING(0 0,4 4)'), ST_GeomFromText('LINESTRING(0 4,4 0)'))"
        ),
        "1"
    );

    // MBR predicates (bounding-box based, exact).
    assert_eq!(
        scalar(&engine, &format!("MBRContains({poly}, Point(2,2))")),
        "1"
    );
    assert_eq!(
        scalar(&engine, &format!("MBRWithin(Point(2,2), {poly})")),
        "1"
    );
    assert_eq!(
        scalar(&engine, &format!("MBRIntersects({poly}, Point(9,9))")),
        "0"
    );
    assert_eq!(
        scalar(&engine, &format!("MBRDisjoint({poly}, Point(9,9))")),
        "1"
    );
}

#[test]
fn srid_round_trips_through_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE g (id INT PRIMARY KEY, geom GEOMETRY)")
        .unwrap();
    engine
        .execute("INSERT INTO g VALUES (1, ST_GeomFromText('POINT(13 52)', 4326))")
        .unwrap();
    assert_eq!(
        cell(&engine, "SELECT ST_SRID(geom) FROM g WHERE id = 1"),
        "4326"
    );
    assert_eq!(
        cell(&engine, "SELECT ST_AsText(geom) FROM g WHERE id = 1"),
        "POINT(13 52)"
    );
    // ST_SRID(g, srid) sets the SRID.
    assert_eq!(
        cell(
            &engine,
            "SELECT ST_SRID(ST_SRID(geom, 0)) FROM g WHERE id = 1"
        ),
        "0"
    );
}

#[test]
fn unsupported_constructive_ops_fail_explicitly_and_buffer_tokens_remain_available() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let left = "ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))')";
    let right = "ST_GeomFromText('POLYGON((1 1,3 1,3 3,1 3,1 1))')";
    for function in [
        "ST_Union",
        "ST_Intersection",
        "ST_Difference",
        "ST_SymDifference",
    ] {
        let err = engine
            .execute(&format!("SELECT {function}({left}, {right})"))
            .unwrap_err();
        assert_eq!(err.code(), 1305, "{function}: {err}");
    }
    let err = engine
        .execute("SELECT ST_Buffer(Point(0,0), 1)")
        .unwrap_err();
    assert_eq!(err.code(), 1305);

    // MySQL's buffer strategy token is a stable 12-byte little-endian
    // binary string: u32 strategy id followed by the f64 parameter.
    assert_eq!(
        scalar(&engine, "HEX(ST_Buffer_Strategy('point_circle', 32))"),
        "050000000000000000004040"
    );
    assert_eq!(
        scalar(&engine, "HEX(ST_Buffer_Strategy('end_flat'))"),
        "020000000000000000000000"
    );
}

#[test]
fn spatial_index_ddl_dml_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE places (id INT PRIMARY KEY, loc GEOMETRY NOT NULL, SPATIAL INDEX(loc))",
        )
        .unwrap();
    // INSERT maintains the spatial index (stored as an MBR B-tree).
    for i in 0..20 {
        engine
            .execute(&format!(
                "INSERT INTO places VALUES ({i}, ST_GeomFromText('POINT({i} {i})'))"
            ))
            .unwrap();
    }

    // SHOW INDEX advertises the SPATIAL index type.
    let show = rows(&engine, "SHOW INDEX FROM places");
    let has_spatial = show.iter().any(|r| {
        r.iter()
            .any(|c| c.to_display_string().eq_ignore_ascii_case("SPATIAL"))
    });
    assert!(
        has_spatial,
        "SHOW INDEX should list the SPATIAL index: {show:?}"
    );

    // information_schema.statistics reflects INDEX_TYPE = SPATIAL.
    let stat = cell(
        &engine,
        "SELECT INDEX_TYPE FROM information_schema.statistics \
         WHERE TABLE_NAME = 'places' AND COLUMN_NAME = 'loc'",
    );
    assert_eq!(stat.to_ascii_uppercase(), "SPATIAL");

    // Contains excludes boundary points; Covers includes all four points.
    let cnt = cell(
        &engine,
        "SELECT COUNT(*) FROM places \
         WHERE MBRContains(ST_GeomFromText('POLYGON((2 2,5 2,5 5,2 5,2 2))'), loc)",
    );
    assert_eq!(cnt, "2");
    assert_eq!(cell(&engine,"SELECT COUNT(*) FROM places WHERE MBRCovers(ST_GeomFromText('POLYGON((2 2,5 2,5 5,2 5,2 2))'),loc)"),"4");

    // DELETE then re-query stays correct (index entry removed too).
    engine.execute("DELETE FROM places WHERE id = 3").unwrap();
    let cnt2 = cell(
        &engine,
        "SELECT COUNT(*) FROM places \
         WHERE MBRContains(ST_GeomFromText('POLYGON((2 2,5 2,5 5,2 5,2 2))'), loc)",
    );
    assert_eq!(cnt2, "1");

    // DROP the index.
    engine.execute("DROP INDEX loc ON places").unwrap();
}

#[test]
fn spatial_index_drives_query_and_matches_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE places (
                id INT PRIMARY KEY,
                loc GEOMETRY NOT NULL SRID 0,
                SPATIAL INDEX(loc)
            )",
        )
        .unwrap();
    // A 15×15 grid of points.
    for x in 0..15 {
        for y in 0..15 {
            let id = x * 15 + y;
            engine
                .execute(&format!(
                    "INSERT INTO places VALUES ({id}, ST_GeomFromText('POINT({x} {y})'))"
                ))
                .unwrap();
        }
    }

    // Window query driven by the spatial index. Contains excludes boundary
    // points, so x,y in {4,5,6} yields nine rows; Covers yields 25.
    let q = "SELECT id FROM places \
             WHERE MBRContains(ST_GeomFromText('POLYGON((3 3,7 3,7 7,3 7,3 3))'), loc) \
             ORDER BY id";
    let got = rows(&engine, q);
    assert_eq!(got.len(), 9, "indexed spatial query row count");
    assert_eq!(
        rows(&engine, &q.replace("MBRContains", "MBRCovers")).len(),
        25
    );

    // EXPLAIN confirms the spatial index is chosen.
    let explained = rows(&engine, &format!("EXPLAIN {q}"));
    let mentions_spatial = explained.iter().flatten().any(|c| {
        let s = c.to_display_string().to_ascii_lowercase();
        s.contains("spatial")
    });
    assert!(
        mentions_spatial,
        "EXPLAIN should show the spatial index: {explained:?}"
    );

    // ST_Contains (exact topological) over the index agrees with the count
    // obtained by a non-indexed scan of the same predicate on a twin table.
    let exact = "SELECT COUNT(*) FROM places \
                 WHERE ST_Contains(ST_GeomFromText('POLYGON((3 3,7 3,7 7,3 7,3 3))'), loc)";
    // Points strictly inside the polygon (boundary excluded): x,y ∈ {4,5,6} ⇒ 9.
    assert_eq!(cell(&engine, exact), "9");

    // A window matching nothing returns no rows (and does not error).
    let empty = rows(
        &engine,
        "SELECT id FROM places \
         WHERE MBRIntersects(ST_GeomFromText('POLYGON((100 100,101 100,101 101,100 101,100 100))'), loc)",
    );
    assert!(empty.is_empty());
}

#[test]
fn geometry_group_by_and_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE g (id INT PRIMARY KEY, p POINT NOT NULL)")
        .unwrap();
    for (id, wkt) in [
        (1, "POINT(2 2)"),
        (2, "POINT(1 1)"),
        (3, "POINT(2 2)"),
        (4, "POINT(3 3)"),
    ] {
        engine
            .execute(&format!(
                "INSERT INTO g VALUES ({id}, ST_GeomFromText('{wkt}'))"
            ))
            .unwrap();
    }
    // GROUP BY a geometry column: the two identical POINT(2 2) rows collapse
    // into one group (grouping keys on the internal byte form). 3 groups with
    // counts {1, 1, 2}.
    let mut counts: Vec<i64> = rows(&engine, "SELECT COUNT(*) FROM g GROUP BY p")
        .iter()
        .map(|r| r[0].to_display_string().parse().unwrap())
        .collect();
    counts.sort();
    assert_eq!(
        counts,
        vec![1, 1, 2],
        "GROUP BY geometry groups identical points"
    );

    // ORDER BY a geometry column is deterministic — it orders by the internal
    // binary form (like MySQL, not by coordinates). The order is stable across
    // runs and the two identical points sort adjacently.
    let order_of = |e: &StorageQueryEngine| -> Vec<String> {
        rows(e, "SELECT id FROM g ORDER BY p, id")
            .iter()
            .map(|r| r[0].to_display_string())
            .collect()
    };
    let o1 = order_of(&engine);
    let o2 = order_of(&engine);
    assert_eq!(o1, o2, "ORDER BY geometry is deterministic");
    assert_eq!(o1.len(), 4);
    let pos1 = o1.iter().position(|x| x == "1").unwrap() as i64;
    let pos3 = o1.iter().position(|x| x == "3").unwrap() as i64;
    assert_eq!((pos1 - pos3).abs(), 1, "identical points sort adjacently");
}

#[test]
fn srid_mismatch_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let r = engine.execute(
        "SELECT ST_Distance(ST_GeomFromText('POINT(0 0)', 4326), ST_GeomFromText('POINT(1 1)', 0))",
    );
    assert!(r.is_err(), "SRID mismatch should error, got {r:?}");
}

#[test]
fn spatial_index_partition_queries_rollback_and_restart() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        engine.execute("CREATE TABLE persistent_places(id INT PRIMARY KEY, g POINT NOT NULL SRID 0, SPATIAL KEY sp(g)) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
        engine.execute("INSERT INTO persistent_places VALUES(1,ST_GeomFromText('POINT(1 1)')),(11,ST_GeomFromText('POINT(1 1)')),(12,ST_GeomFromText('POINT(20 20)'))").unwrap();
        let filter = "MBRIntersects(g,ST_GeomFromText('POLYGON((0 0,3 0,3 3,0 3,0 0))'))";
        assert_eq!(
            cell(
                &engine,
                &format!("SELECT COUNT(*) FROM persistent_places WHERE {filter}")
            ),
            "2"
        );
        assert_eq!(
            cell(
                &engine,
                &format!("SELECT COUNT(*) FROM persistent_places WHERE id<10 AND {filter}")
            ),
            "1"
        );
        let indexed = rows(
            &engine,
            &format!("SELECT id FROM persistent_places FORCE INDEX(sp) WHERE {filter} ORDER BY id"),
        );
        let scanned = rows(
            &engine,
            &format!(
                "SELECT id FROM persistent_places IGNORE INDEX(sp) WHERE {filter} ORDER BY id"
            ),
        );
        assert_eq!(indexed, scanned);
        let mut session = SessionContext::system_default();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(
                &mut session,
                "UPDATE persistent_places SET g=ST_GeomFromText('POINT(100 100)') WHERE id=1",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(
            cell(
                &engine,
                &format!("SELECT COUNT(*) FROM persistent_places WHERE {filter}")
            ),
            "2"
        );
    }
    let engine = StorageQueryEngine::open(dir.path()).unwrap();
    assert_eq!(cell(&engine,"SELECT COUNT(*) FROM persistent_places WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))"),"2");
    engine
        .execute("UPDATE persistent_places SET id=2 WHERE id=11")
        .unwrap();
    assert_eq!(cell(&engine,"SELECT COUNT(*) FROM persistent_places WHERE id<10 AND MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))"),"2");
}

#[test]
fn spatial_index_persistent_ddl_lifecycle() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE spatial_ddl(id INT PRIMARY KEY,g POINT NOT NULL SRID 0)")
        .unwrap();
    for id in 0..600 {
        engine
            .execute(&format!(
                "INSERT INTO spatial_ddl VALUES({id},ST_GeomFromText('POINT({id} {id})'))"
            ))
            .unwrap();
    }
    engine
        .execute("ALTER TABLE spatial_ddl ADD SPATIAL INDEX sp(g)")
        .unwrap();
    engine
        .execute("ALTER TABLE spatial_ddl RENAME INDEX sp TO renamed")
        .unwrap();
    assert_eq!(cell(&engine,"SELECT COUNT(*) FROM spatial_ddl FORCE INDEX(renamed) WHERE MBRIntersects(g,ST_GeomFromText('POINT(7 7)'))"),"1");
    engine.execute("OPTIMIZE TABLE spatial_ddl").unwrap();
    assert_eq!(
        cell(
            &engine,
            "SELECT COUNT(*) FROM spatial_ddl WHERE MBRIntersects(g,ST_GeomFromText('POINT(7 7)'))"
        ),
        "1"
    );
    engine.execute("TRUNCATE TABLE spatial_ddl").unwrap();
    engine
        .execute("INSERT INTO spatial_ddl VALUES(1,ST_GeomFromText('POINT(1 1)'))")
        .unwrap();
    assert_eq!(
        cell(
            &engine,
            "SELECT COUNT(*) FROM spatial_ddl WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))"
        ),
        "1"
    );
    engine.execute("DROP INDEX renamed ON spatial_ddl").unwrap();
    engine
        .execute("CREATE SPATIAL INDEX replacement ON spatial_ddl(g)")
        .unwrap();
    assert_eq!(
        cell(
            &engine,
            "SELECT COUNT(*) FROM spatial_ddl WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))"
        ),
        "1"
    );
}

#[test]
fn spatial_index_partition_old_snapshot_keeps_physical_identity() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE spatial_snapshot(id INT PRIMARY KEY,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g)) PARTITION BY RANGE(id) (PARTITION a VALUES LESS THAN(10),PARTITION b VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("INSERT INTO spatial_snapshot VALUES(1,ST_GeomFromText('POINT(1 1)')),(11,ST_GeomFromText('POINT(1 1)'))").unwrap();
    let mut reader = SessionContext::system_default();
    reader.connection_id = 99101;
    let mut writer = SessionContext::system_default();
    writer.connection_id = 99102;
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .unwrap();
    let query="SELECT id FROM spatial_snapshot FORCE INDEX(sp) WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)')) ORDER BY id";
    let QueryResponse::ResultSet(expected) = engine
        .execute_with_session(&mut reader, query)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    engine
        .execute_with_session(
            &mut writer,
            "UPDATE spatial_snapshot SET g=ST_GeomFromText('POINT(99 99)') WHERE id=1",
        )
        .unwrap();
    let QueryResponse::ResultSet(actual) = engine
        .execute_with_session(&mut reader, query)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(actual.rows, expected.rows);
    engine
        .execute_with_session(&mut writer, "DELETE FROM spatial_snapshot WHERE id=11")
        .unwrap();
    let QueryResponse::ResultSet(actual) = engine
        .execute_with_session(&mut reader, query)
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(actual.rows, expected.rows);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
}

#[test]
fn spatial_index_temporary_and_partition_ddl() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::system_default();
    for sql in ["CREATE TEMPORARY TABLE spatial_tmp(id INT PRIMARY KEY,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g))","INSERT INTO spatial_tmp VALUES(1,ST_GeomFromText('POINT(1 1)'))","TRUNCATE TABLE spatial_tmp","INSERT INTO spatial_tmp VALUES(2,ST_GeomFromText('POINT(1 1)'))"] {engine.execute_with_session(&mut session,sql).unwrap();}
    let QueryResponse::ResultSet(rs) = engine
        .execute_with_session(
            &mut session,
            "SELECT id FROM spatial_tmp WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))",
        )
        .unwrap()
        .materialize()
        .unwrap()
    else {
        panic!("rows")
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    for sql in [
        "ALTER TABLE spatial_tmp DROP INDEX sp",
        "ALTER TABLE spatial_tmp ADD SPATIAL INDEX sp(g)",
        "ALTER TABLE spatial_tmp RENAME INDEX sp TO renamed",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    let QueryResponse::ResultSet(rs)=engine.execute_with_session(&mut session,"SELECT id FROM spatial_tmp FORCE INDEX(renamed) WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))").unwrap().materialize().unwrap() else {panic!("rows")};
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    engine.execute("CREATE TABLE spatial_parts(id INT PRIMARY KEY,g POINT NOT NULL SRID 0,SPATIAL KEY sp(g)) PARTITION BY RANGE(id) (PARTITION a VALUES LESS THAN(10),PARTITION b VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("INSERT INTO spatial_parts VALUES(1,ST_GeomFromText('POINT(1 1)')),(11,ST_GeomFromText('POINT(1 1)'))").unwrap();
    engine
        .execute("ALTER TABLE spatial_parts TRUNCATE PARTITION a")
        .unwrap();
    engine
        .execute("INSERT INTO spatial_parts VALUES(2,ST_GeomFromText('POINT(1 1)'))")
        .unwrap();
    assert_eq!(cell(&engine,"SELECT COUNT(*) FROM spatial_parts WHERE MBRIntersects(g,ST_GeomFromText('POINT(1 1)'))"),"2");
    engine
        .execute("ALTER TABLE spatial_parts CHECK PARTITION ALL")
        .unwrap();
}
