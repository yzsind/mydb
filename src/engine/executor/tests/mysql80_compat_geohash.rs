//! MySQL 8.0 ST_GeoHash/ST_LatFromGeoHash/ST_LongFromGeoHash compatibility.
//!
//! Cover Phase H(G7) GeoHash encoding/decoding in docs/expr-mysql80-compat-plan.md.
//! Use standard documented vectors: GeoHash is deterministic and MySQL uses
//! the same standard encoding, so compatibility can be verified without a running MySQL:
//! MySQL vector ST_GeoHash(180,0,10) = 'xbpbpbpbpb'.
//! Classic vector longitude=-5.6, latitude=42.6, length=5 gives 'ezs42'.
//! Decode to the cell center and verify encoding round trips.
use super::*;

fn scalar(engine: &StorageQueryEngine, sql: &str) -> Value {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows.len(), 1, "expected 1 row for `{sql}`");
            r.rows[0][0].clone()
        }
        other => panic!("expected result set for `{sql}`, got {other:?}"),
    }
}

fn as_f64(v: &Value) -> f64 {
    match v {
        Value::Double(d) => *d,
        Value::Float(f) => *f as f64,
        other => panic!("expected float, got {other:?}"),
    }
}

#[test]
fn geohash_encode_mysql_doc_vector() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // From the MySQL 8.0 reference manual.
    assert_eq!(
        scalar(&engine, "SELECT ST_GeoHash(180, 0, 10)"),
        Value::String("xbpbpbpbpb".to_string())
    );
}

#[test]
fn geohash_encode_classic_vector() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // lon=-5.6, lat=42.6 → 'ezs42' (canonical geohash example).
    assert_eq!(
        scalar(&engine, "SELECT ST_GeoHash(-5.6, 42.6, 5)"),
        Value::String("ezs42".to_string())
    );
}

#[test]
fn geohash_encode_from_point() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // POINT(x, y) = POINT(longitude, latitude).
    assert_eq!(
        scalar(&engine, "SELECT ST_GeoHash(POINT(-5.6, 42.6), 5)"),
        Value::String("ezs42".to_string())
    );
}

#[test]
fn geohash_decode_center() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let lat = as_f64(&scalar(&engine, "SELECT ST_LatFromGeoHash('ezs42')"));
    let lon = as_f64(&scalar(&engine, "SELECT ST_LongFromGeoHash('ezs42')"));
    assert!((lat - 42.6).abs() < 0.1, "lat={lat}");
    assert!((lon + 5.6).abs() < 0.1, "lon={lon}");
}

#[test]
fn geohash_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // Encode a precise point then decode: the cell centre must be close.
    let h = scalar(&engine, "SELECT ST_GeoHash(13.361389, 52.520008, 12)");
    let Value::String(hash) = h else {
        panic!("expected string");
    };
    let lat = as_f64(&scalar(
        &engine,
        &format!("SELECT ST_LatFromGeoHash('{hash}')"),
    ));
    let lon = as_f64(&scalar(
        &engine,
        &format!("SELECT ST_LongFromGeoHash('{hash}')"),
    ));
    assert!((lat - 52.520008).abs() < 1e-4, "lat={lat}");
    assert!((lon - 13.361389).abs() < 1e-4, "lon={lon}");
}

#[test]
fn geohash_longitude_out_of_range_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine.execute("SELECT ST_GeoHash(200, 0, 10)").unwrap_err();
    assert_eq!(err.code(), 3616, "got {err:?}");
}

#[test]
fn distance_sphere_nonpositive_radius_uses_dedicated_error() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine
        .execute("SELECT ST_DISTANCE_SPHERE(POINT(0,0), POINT(1,1), -1)")
        .unwrap_err();
    assert_eq!(
        (err.code(), err.sql_state().as_ref()),
        (3706, "22003"),
        "got {err:?}"
    );
}

#[test]
fn geometry_constructor_validates_srid_before_wkt() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine
        .execute(
            "SELECT ST_MULTIPOLYGONFROMTEXT(\
             '4294967294.1',\
             999999999999999999999999999999999999999999999999999999999999999999999999999999999)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1690, "got {err:?}");
}

#[test]
fn geohash_invalid_string_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // 'a' and 'i','l','o' are not in the geohash base32 alphabet.
    let err = engine
        .execute("SELECT ST_LatFromGeoHash('ail')")
        .unwrap_err();
    assert_eq!(err.code(), 3618, "got {err:?}");
}

#[test]
fn geohash_null_returns_null() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    assert!(matches!(
        scalar(&engine, "SELECT ST_GeoHash(NULL, 0, 10)"),
        Value::Null
    ));
    assert!(matches!(
        scalar(&engine, "SELECT ST_LatFromGeoHash(NULL)"),
        Value::Null
    ));
}
