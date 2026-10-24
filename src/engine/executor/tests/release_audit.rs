use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let result = engine
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    let values = match result {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut rows = Vec::new();
            while let Some(row) = r.cursor.next_row().unwrap() {
                rows.push(row);
            }
            rows
        }
        other => panic!("unexpected result: {other:?}"),
    };
    values
        .iter()
        .map(|row| row.iter().map(Value::to_display_string).collect())
        .collect()
}

#[test]
fn release_audit_decimal_unsigned_and_temporal_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE audit_bounds (id BIGINT UNSIGNED PRIMARY KEY, d DECIMAL(20,4), dt DATE, KEY(d), KEY(dt))").unwrap();
    engine.execute("INSERT INTO audit_bounds VALUES (1,0.1,'2024-02-28'),(2,0.2,'2024-02-29'),(3,-0.3,'2024-03-01')").unwrap();
    for (predicate, expected) in [
        ("d > 0.09999999", "1,2"),
        ("d < 0.10000001", "1,3"),
        ("d > 0.09999999 AND d < 0.10000001", "1"),
        ("id = -1", ""),
        ("id IN (-1,2)", "2"),
        ("dt < '2024-02-29 00:00:01'", "1,2"),
        ("dt >= '2024-02-29 00:00:01'", "3"),
    ] {
        for hint in ["", "IGNORE INDEX (PRIMARY,d,dt)"] {
            let actual = rows(
                &engine,
                &format!("SELECT id FROM audit_bounds {hint} WHERE {predicate} ORDER BY id"),
            );
            assert_eq!(
                actual
                    .iter()
                    .map(|r| r[0].as_str())
                    .collect::<Vec<_>>()
                    .join(","),
                expected,
                "{hint} {predicate}"
            );
        }
    }
    engine
        .execute("UPDATE audit_bounds SET dt='2025-01-01' WHERE d > 0.09999999 AND d < 0.10000001")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id FROM audit_bounds WHERE dt='2025-01-01'")[0][0],
        "1"
    );
    engine
        .execute("DELETE FROM audit_bounds WHERE d < 0.10000001")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT id FROM audit_bounds")[0][0], "2");
}

#[test]
fn release_audit_streaming_aggregate_retains_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE audit_groups (id INT PRIMARY KEY, k INT NOT NULL, KEY(k))")
        .unwrap();
    let values = (0..60)
        .map(|i| format!("({i},{})", i % 10))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO audit_groups VALUES {values}"))
        .unwrap();
    let actual = rows(
        &engine,
        "SELECT k,COUNT(*) FROM audit_groups GROUP BY k ORDER BY k LIMIT 3 OFFSET 4",
    );
    assert_eq!(actual, vec![vec!["4", "6"], vec!["5", "6"], vec!["6", "6"]]);
}

#[test]
fn release_audit_explicit_collation_and_mixed_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE audit_coll (id INT PRIMARY KEY, s VARCHAR(30) COLLATE utf8mb4_0900_ai_ci)").unwrap();
    engine
        .execute("INSERT INTO audit_coll VALUES (1,'abc'),(2,'ABC'),(3,'abc ')")
        .unwrap();
    for (predicate, expected) in [
        ("s COLLATE utf8mb4_bin='ABC'", "2"),
        ("s COLLATE utf8mb4_bin='abc '", "1,3"),
        ("s=_binary'ABC'", "1,2"),
        ("s COLLATE utf8mb4_0900_as_cs LIKE 'A%'", "2"),
        ("s COLLATE utf8mb4_bin IN ('abc','ABC')", "1,2,3"),
    ] {
        let actual = rows(
            &engine,
            &format!("SELECT id FROM audit_coll WHERE {predicate} ORDER BY id"),
        );
        assert_eq!(
            actual
                .iter()
                .map(|r| r[0].as_str())
                .collect::<Vec<_>>()
                .join(","),
            expected,
            "{predicate}"
        );
    }
}

#[test]
fn release_audit_mixed_charset_join_is_order_independent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE audit_pad(s VARCHAR(8) CHARACTER SET latin1)")
        .unwrap();
    engine
        .execute("CREATE TABLE audit_nopad(s VARCHAR(8) COLLATE utf8mb4_0900_ai_ci)")
        .unwrap();
    engine
        .execute("INSERT INTO audit_pad VALUES ('abc'),('xyz')")
        .unwrap();
    engine
        .execute("INSERT INTO audit_nopad VALUES ('abc '),('abc')")
        .unwrap();
    for from in [
        "audit_pad STRAIGHT_JOIN audit_nopad",
        "audit_nopad STRAIGHT_JOIN audit_pad",
    ] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT COUNT(*) FROM {from} ON audit_pad.s=audit_nopad.s")
            )[0][0],
            "1"
        );
    }
}

#[test]
fn round2_contradictory_update_and_unique_shift() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE r2(id INT PRIMARY KEY, v INT UNIQUE)")
        .unwrap();
    engine
        .execute("INSERT INTO r2 VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    for predicate in ["id=1 AND id=2", "id=2 AND id=NULL", "id=2 AND id=1"] {
        engine
            .execute(&format!("UPDATE r2 SET v=999 WHERE {predicate}"))
            .unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT v FROM r2 ORDER BY id"),
        vec![vec!["10"], vec!["20"], vec!["30"]]
    );
    for ignore in ["", "IGNORE"] {
        engine
            .execute(&format!("UPDATE {ignore} r2 SET id=id-1,v=v-1"))
            .unwrap();
    }
    assert_eq!(
        rows(&engine, "SELECT id,v FROM r2 ORDER BY id"),
        vec![vec!["-1", "8"], vec!["0", "18"], vec!["1", "28"]]
    );
}

#[test]
fn round2_outer_join_not_in() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE r2a(id INT PRIMARY KEY)",
        "CREATE TABLE r2b(id INT PRIMARY KEY, flag INT NOT NULL)",
        "CREATE TABLE r2s(k INT NOT NULL)",
        "INSERT INTO r2a VALUES (1),(2)",
        "INSERT INTO r2b VALUES (1,10)",
        "INSERT INTO r2s VALUES (10)",
    ] {
        engine.execute(sql).unwrap();
    }
    assert!(rows(&engine,"SELECT r2a.id FROM r2a LEFT JOIN r2b ON r2a.id=r2b.id WHERE r2b.flag NOT IN (SELECT k FROM r2s)").is_empty());
    engine.execute("DELETE FROM r2s").unwrap();
    assert_eq!(rows(&engine,"SELECT r2a.id FROM r2a LEFT JOIN r2b ON r2a.id=r2b.id WHERE r2b.flag NOT IN (SELECT k FROM r2s) ORDER BY r2a.id"),vec![vec!["1"],vec!["2"]]);
}

#[test]
fn round2_partition_returning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE r2p(id INT NOT NULL,p INT NOT NULL,PRIMARY KEY(id,p)) PARTITION BY HASH(p) PARTITIONS 2").unwrap();
    engine.execute("INSERT INTO r2p VALUES(1,0)").unwrap();
    assert_eq!(
        rows(
            &engine,
            "INSERT INTO r2p VALUES(2,1),(3,0),(4,1) RETURNING *"
        ),
        vec![vec!["2", "1"], vec!["3", "0"], vec!["4", "1"]]
    );
}

#[test]
fn round2_setop_collations() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for (collation, right, union_count, except_count, intersect_count) in [
        ("utf8mb4_bin", "A", 2, 1, 0),
        ("utf8mb4_general_ci", "a ", 1, 0, 1),
        ("utf8mb4_0900_ai_ci", "a ", 2, 1, 0),
    ] {
        engine.execute("DROP TABLE IF EXISTS r2l,r2r").unwrap();
        for name in ["r2l", "r2r"] {
            engine
                .execute(&format!(
                    "CREATE TABLE {name}(c VARCHAR(8) COLLATE {collation})"
                ))
                .unwrap();
        }
        engine.execute("INSERT INTO r2l VALUES('a')").unwrap();
        engine
            .execute(&format!("INSERT INTO r2r VALUES('{right}')"))
            .unwrap();
        for (op, count) in [
            ("UNION", union_count),
            ("EXCEPT", except_count),
            ("INTERSECT", intersect_count),
        ] {
            assert_eq!(
                rows(
                    &engine,
                    &format!("SELECT c FROM r2l {op} SELECT c FROM r2r")
                )
                .len(),
                count,
                "{collation} {op}"
            );
        }
    }
}

#[test]
fn round2_spatial_options_and_topology() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            "SELECT ST_Latitude(ST_GeomFromText('POINT(8.9 50.8)',4326,'axis-order=long-lat'))"
        )[0][0],
        "50.8"
    );
    assert!(engine.execute("SELECT ST_Distance_Sphere(ST_GeomFromText('POINT(0 0)',0),ST_GeomFromText('POINT(1 1)',4326))").is_err());
    let a = "ST_GeomFromText('POLYGON((0 4,10 6,10 6.4,0 4.4,0 4))')";
    let b = "ST_GeomFromText('POLYGON((4 -3,6 10,6.4 10,4.4 -3,4 -3))')";
    assert_eq!(
        rows(
            &engine,
            &format!("SELECT ST_Overlaps({a},{b}),ST_Touches({a},{b})")
        ),
        vec![vec!["1", "0"]]
    );
    assert_eq!(rows(&engine,"SELECT ST_Crosses(ST_GeomFromText('LINESTRING(0 0,4 4)'),ST_GeomFromText('LINESTRING(0 4,4 0)'))"),vec![vec!["1"]]);
}

#[test]
fn round2_batch_unique_release_to_null_and_ignore_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE r2n(id INT PRIMARY KEY,u INT UNIQUE)")
        .unwrap();
    engine
        .execute("INSERT INTO r2n VALUES(1,1),(2,2),(3,3)")
        .unwrap();
    engine
        .execute("UPDATE r2n SET u=CASE WHEN id=1 THEN NULL ELSE u-1 END")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,u FROM r2n ORDER BY id"),
        vec![vec!["1", "NULL"], vec!["2", "1"], vec!["3", "2"]]
    );
    engine.execute("UPDATE IGNORE r2n SET u=0").unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,u FROM r2n ORDER BY id"),
        vec![vec!["1", "0"], vec!["2", "1"], vec!["3", "2"]]
    );
    assert!(engine.execute("UPDATE r2n SET u=10").is_err());
    assert_eq!(
        rows(&engine, "SELECT id,u FROM r2n ORDER BY id"),
        vec![vec!["1", "0"], vec!["2", "1"], vec!["3", "2"]]
    );
}
