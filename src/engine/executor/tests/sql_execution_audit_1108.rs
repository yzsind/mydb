use super::*;

#[test]
fn audit1108_outer_aggregate_retains_exact_and_string_result_domains() {
    let (_dir,engine)=fixture(&[
        "CREATE TABLE own_domain(id INT PRIMARY KEY,n DECIMAL(30,4),s VARCHAR(8) CHARACTER SET latin1 COLLATE latin1_bin)",
        "INSERT INTO own_domain VALUES(1,9007199254740993.1250,'å'),(2,0.1250,'z')",
        "CREATE TABLE own_inner(id INT PRIMARY KEY,n DOUBLE)",
        "INSERT INTO own_inner VALUES(1,1.5),(2,2.25)",
    ]);
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT (SELECT SUM(d.n) FROM own_inner LIMIT 1) FROM own_domain d")
        .and_then(QueryResponse::materialize)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        result.rows[0][0].to_display_string(),
        "9007199254740993.2500"
    );
    assert!(
        matches!(result.columns[0].column_type, ColumnType::Decimal { .. }),
        "{:?}",
        result.columns[0]
    );
    assert_eq!(result.columns[0].source.decimals, Some(4));
    let mut session = SessionContext::default();
    for query in [
        "SELECT SUM((SELECT SUM(d.n) FROM own_inner LIMIT 1)) FROM own_domain d",
        "SELECT SUM(ANY_VALUE((SELECT COUNT(d.n) FROM own_inner LIMIT 1))) FROM own_domain d",
    ] {
        assert_eq!(
            QueryEngine::describe_prepared_statement(&engine, &mut session, query)
                .unwrap_err()
                .code(),
            1111,
            "{query}"
        );
    }
    for query in [
        "SELECT (SELECT SUM(d.n) FROM own_inner LIMIT ?) FROM own_domain d",
        "SELECT (SELECT SUM(d.n)+SUM(a.id) FROM own_inner a) FROM own_domain d",
        "SELECT (SELECT SUM(d.n) FROM own_inner LIMIT ?) FROM own_domain d JOIN own_inner i ON i.id=d.id",
    ] {
        let columns=QueryEngine::describe_prepared_statement(&engine,&mut session,query).unwrap();
        assert!(matches!(columns[0].column_type,ColumnType::Decimal{..}),"{query}: {:?}",columns[0]);
        assert_eq!(columns[0].source.decimals,Some(4),"{query}");
    }
    engine.execute("CREATE VIEW own_domain_view AS SELECT (SELECT SUM(d.n) FROM own_inner LIMIT 1) s FROM own_domain d").unwrap();
    assert_eq!(
        rows(&engine, "SHOW COLUMNS FROM own_domain_view")[0][1],
        "decimal(52,4)"
    );
    assert_eq!(
        rows(&engine, "SELECT * FROM own_domain_view"),
        vec![vec!["9007199254740993.2500"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT (SELECT SUM(d.n)+SUM(a.id) FROM own_inner a) FROM own_domain d"
        ),
        vec![vec!["9007199254740996.2500"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT (SELECT SUM(d.n+a.id) FROM own_inner a) FROM own_domain d ORDER BY d.id"
        ),
        vec![vec!["18014398509481989.2500"], vec!["3.2500"]]
    );
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    for query in [
        "SELECT d.id IN(SELECT SUM(d.n) FROM own_inner) FROM own_domain d",
        "SELECT d.id>ANY(SELECT SUM(d.n) FROM own_inner) FROM own_domain d",
    ] {
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, query)
            .and_then(QueryResponse::materialize)
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(result.rows.len(), 1, "{query}");
        assert_eq!(result.rows[0][0].to_display_string(), "0", "{query}");
    }
    let QueryResponse::ResultSet(local) = engine
        .execute("SELECT (SELECT SUM(n) FROM own_inner) FROM own_domain d")
        .and_then(QueryResponse::materialize)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(local.columns[0].column_type, ColumnType::Double);
    assert_eq!(
        local
            .rows
            .iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>(),
        vec!["3.75", "3.75"]
    );

    assert_eq!(rows(&engine,"SELECT (SELECT HEX(MIN(d.s)) FROM own_inner LIMIT 1),(SELECT COLLATION(MIN(d.s)) FROM own_inner LIMIT 1) FROM own_domain d"),vec![vec!["7A","latin1_bin"]]);
}

#[test]
fn audit1108_aggregate_ownership_preserves_inner_rows_and_local_aggregates() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    for cache in [None, Some("off")] {
        for decorrelate in [None, Some("off")] {
            with_test_env(
                &[
                    ("MYDB_SUBQUERY_PLAN_CACHE", cache),
                    ("MYDB_DECORRELATE", decorrelate),
                ],
                || {
                    let (_dir, engine) = fixture(&[
                        "CREATE TABLE own_a(id INT PRIMARY KEY,k INT,v INT)",
                        "INSERT INTO own_a VALUES(1,10,1),(2,20,100),(3,NULL,50),(4,10,7)",
                        "CREATE TABLE own_b(id INT PRIMARY KEY,k INT,x INT)",
                        "INSERT INTO own_b VALUES(1,10,1),(2,10,2),(3,20,3),(4,NULL,4),(5,30,NULL)",
                    ]);
                    for mode in [VecMode::Off, VecMode::On] {
                        with_vec_mode(mode, || {
                            for (sql, expected) in [
                            ("SELECT (SELECT SUM(b.x) FROM own_a LIMIT 1) c FROM own_b b",vec![vec!["10"]]),
                            ("SELECT (SELECT MAX(b.x)+COUNT(*) FROM own_a) c FROM own_b b",vec![vec!["8"]]),
                            ("SELECT k FROM own_b b GROUP BY k HAVING (SELECT SUM(b.x) FROM own_a LIMIT 1)>2 ORDER BY k",vec![vec!["NULL"],vec!["10"],vec!["20"]]),
                            ("SELECT k,(SELECT COUNT(b.x) FROM own_a LIMIT 1) c FROM own_b b GROUP BY k ORDER BY k",vec![vec!["NULL","1"],vec!["10","2"],vec!["20","1"],vec!["30","0"]]),
                            ("SELECT (SELECT SUM(x) FROM own_a LIMIT 1) c FROM own_b b",vec![vec!["10"]]),
                            ("SELECT (SELECT SUM(b.x)+SUM(a.v) FROM own_a a) c FROM own_b b",vec![vec!["168"]]),
                            ("SELECT (SELECT SUM(b.x) FROM own_a LIMIT 1 OFFSET 1) c FROM own_b b",vec![vec!["10"]]),
                            ("SELECT (SELECT MAX(b.x)+COUNT(*) FROM own_a WHERE id<0) c FROM own_b b",vec![vec!["4"]]),
                            ("SELECT (SELECT SUM(b.x) FROM own_a LIMIT 1) c,COUNT(*) FROM own_b b",vec![vec!["10","5"]]),
                            ("SELECT (SELECT SUM(b.x) FROM own_a WHERE id<0) c FROM own_b b",vec![vec!["NULL"]]),
                            ("SELECT (SELECT COUNT(b.x) FROM own_a WHERE id<0) c FROM own_b b",vec![vec!["NULL"]]),
                        ] {
                            assert_eq!(rows(&engine,sql),expected,"cache={cache:?} decorrelate={decorrelate:?} {mode:?} {sql}");
                        }
                            for sql in ["SELECT 1 FROM own_b b ORDER BY (SELECT SUM(b.x) FROM own_a LIMIT 1)","SELECT k FROM own_b b ORDER BY (SELECT SUM(b.x) FROM own_a LIMIT 1)"] {
                                assert_eq!(engine.execute(sql).unwrap_err().code(),3029,"{sql}");
                            }
                            assert_eq!(
                                engine
                                    .execute("SELECT (SELECT SUM(b.x) FROM own_a) c FROM own_b b")
                                    .and_then(QueryResponse::materialize)
                                    .unwrap_err()
                                    .code(),
                                1242
                            );
                            assert_eq!(engine.execute("SELECT b.k,(SELECT SUM(b.x) FROM own_a LIMIT 1) c FROM own_b b").unwrap_err().code(),1140);
                            for sql in [
                                "SELECT b.id IN(SELECT SUM(b.x) FROM own_a) FROM own_b b",
                                "SELECT b.id>ANY(SELECT SUM(b.x) FROM own_a) FROM own_b b",
                            ] {
                                assert_eq!(engine.execute(sql).unwrap_err().code(), 1140, "{sql}");
                            }
                            assert_eq!(rows(&engine,"SELECT (SELECT COUNT(b.x) FROM own_a LIMIT 1),COUNT(*) FROM own_b b WHERE id<0"),vec![vec!["0","0"]]);
                            assert_eq!(rows(&engine,"SELECT (SELECT COUNT(DISTINCT b.k,b.x) FROM own_a LIMIT 1),(SELECT SUM(DISTINCT b.x) FROM own_a LIMIT 1),(SELECT AVG(b.x) FROM own_a LIMIT 1),(SELECT BIT_OR(b.x) FROM own_a LIMIT 1) FROM own_b b"),vec![vec!["3","10","2.5000","7"]]);
                            assert_eq!(rows(&engine,"SELECT EXISTS(SELECT SUM(b.x) FROM own_a),EXISTS(SELECT SUM(b.x) FROM own_a WHERE id<0),10 IN(SELECT SUM(b.x) FROM own_a),10=ANY(SELECT SUM(b.x) FROM own_a) FROM own_b b"),vec![vec!["1","0","1","1"]]);
                            assert_eq!(rows(&engine,"SELECT b.k,b.k IN(SELECT SUM(b.x) FROM own_a),b.k>ANY(SELECT SUM(b.x) FROM own_a) FROM own_b b GROUP BY b.k WITH ROLLUP"),vec![vec!["NULL","NULL","NULL"],vec!["10","0","1"],vec!["20","0","1"],vec!["30","NULL","NULL"],vec!["NULL","NULL","NULL"]]);
                            for sql in ["SELECT SUM((SELECT SUM(b.x) FROM own_a LIMIT 1)) FROM own_b b","SELECT SUM((SELECT COUNT(b.x) FROM own_a LIMIT 1)) FROM own_b b","SELECT SUM(ANY_VALUE((SELECT SUM(b.x) FROM own_a LIMIT 1))) FROM own_b b"] {
                                assert_eq!(engine.execute(sql).unwrap_err().code(),1111,"{sql}");
                            }
                            assert_eq!(rows(&engine,"SELECT SUM((SELECT SUM(a.v) FROM own_a a WHERE a.id=b.id)) FROM own_b b"),vec![vec!["158"]]);
                            assert_eq!(rows(&engine,"SELECT ANY_VALUE(SUM(b.x)),ANY_VALUE((SELECT SUM(b.x) FROM own_a LIMIT 1)),ANY_VALUE(SUM(b.x)+b.k),ANY_VALUE(SUM(b.x)+(SELECT SUM(a.v) FROM own_a a WHERE a.k=b.k)) FROM own_b b"),vec![vec!["10","10","20","18"]]);
                            assert_eq!(rows(&engine,"SELECT (SELECT SUM(x) FROM own_b LIMIT 1) c FROM own_b b ORDER BY b.id"),vec![vec!["10"];5]);
                            assert_eq!(rows(&engine,"SELECT (SELECT SUM(b.x+a.v) FROM own_a a) c FROM own_b b ORDER BY b.id"),vec![vec!["162"],vec!["166"],vec!["170"],vec!["174"],vec!["NULL"]]);
                        });
                    }
                },
            );
        }
    }
}

#[test]
fn audit1108_outer_aggregate_binding_covers_nested_queries_rollup_and_cache_keys() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    for cache in [None, Some("off")] {
        with_test_env(&[("MYDB_SUBQUERY_PLAN_CACHE", cache)], || {
            let (_dir, engine) = fixture(&[
                "CREATE TABLE own_a(id INT PRIMARY KEY,k INT,v INT)",
                "INSERT INTO own_a VALUES(1,10,1),(2,20,100),(3,NULL,50),(4,10,7)",
                "CREATE TABLE own_b(id INT PRIMARY KEY,k INT,x BIGINT)",
                "INSERT INTO own_b VALUES(1,10,1),(2,10,2),(3,20,3),(4,NULL,4),(5,30,NULL)",
            ]);
            for mode in [VecMode::Off, VecMode::On] {
                with_vec_mode(mode, || {
                    assert_eq!(rows(&engine,"SELECT b.k,(SELECT SUM(b.k) FROM own_a LIMIT 1) c FROM own_b b GROUP BY b.k WITH ROLLUP"),vec![vec!["NULL","NULL"],vec!["10","20"],vec!["20","20"],vec!["30","30"],vec!["NULL","70"]]);
                    for sql in [
                        "SELECT (SELECT (SELECT SUM(b.x) FROM own_a LIMIT 1)) c FROM own_b b",
                        "SELECT (SELECT (SELECT SUM(b.x) FROM own_a LIMIT 1) FROM own_a LIMIT 1) c FROM own_b b",
                    ] {assert_eq!(rows(&engine,sql),vec![vec!["10"]],"{sql}");}
                    assert_eq!(rows(&engine,"SELECT b.k,(SELECT SUM(b.x) FROM own_a LIMIT 1) c FROM own_b b GROUP BY b.k WITH ROLLUP"),vec![vec!["NULL","4"],vec!["10","3"],vec!["20","3"],vec!["30","NULL"],vec!["NULL","10"]]);
                    assert_eq!(rows(&engine,"SELECT b.k,(SELECT SUM(b.x) FROM own_a LIMIT 1) c FROM own_b b GROUP BY b.k ORDER BY c DESC"),vec![vec!["NULL","4"],vec!["10","3"],vec!["20","3"],vec!["30","NULL"]]);
                    assert_eq!(rows(&engine,"SELECT (SELECT COUNT(b.x) FROM own_a a WHERE a.k=b.k LIMIT 1) c FROM own_b b GROUP BY b.k ORDER BY b.k"),vec![vec!["NULL"],vec!["2"],vec!["1"],vec!["NULL"]]);
                });
            }
            engine
                .execute("UPDATE own_b SET x=9007199254740993 WHERE id=1")
                .unwrap();
            for _ in 0..3 {
                assert_eq!(
                    rows(
                        &engine,
                        "SELECT (SELECT SUM(b.x) FROM own_a LIMIT 1) c FROM own_b b"
                    ),
                    vec![vec!["9007199254741002"]]
                );
            }
        });
    }
}

#[test]
fn audit1108_wkt_coordinates_keep_signed_zero_and_compact_extreme_numbers() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&["CREATE TABLE wkt_coordinates(id INT PRIMARY KEY,g GEOMETRY)"]);
    let geometries = [
        "POINT(-0 0)",
        "POINT(1e15 1e-16)",
        "POINT(100000000000000 0.000000000000001)",
        "POINT(1e300 -1e-300)",
        "LINESTRING(-0 1e15,1e-16 2)",
        "POLYGON((0 0,1e15 0,1e15 1,0 0))",
        "MULTIPOINT(1e20 -0,1e-16 2e15)",
        "MULTILINESTRING((-0 1,1e15 2))",
        "MULTIPOLYGON(((0 0,1e15 0,1e15 1,0 0)))",
        "GEOMETRYCOLLECTION(POINT(-0 1e-16),LINESTRING(1 1,1e15 2))",
    ];
    for (id, wkt) in geometries.iter().enumerate() {
        engine
            .execute(&format!(
                "INSERT INTO wkt_coordinates VALUES({id},ST_GeomFromText('{wkt}'))"
            ))
            .unwrap();
    }
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            let actual = rows(&engine, "SELECT ST_AsText(g),ST_AsWKT(ST_GeomFromWKB(ST_AsWKB(g))),HEX(ST_AsWKB(g))=HEX(ST_AsWKB(ST_GeomFromText(ST_AsText(g)))) FROM wkt_coordinates ORDER BY id");
            for (row, expected) in actual.iter().zip(geometries) {
                assert_eq!(
                    row,
                    &vec![expected.to_string(), expected.to_string(), "1".into()],
                    "{mode:?}"
                );
            }
            assert_eq!(actual.len(), geometries.len());
            assert_eq!(
                rows(
                    &engine,
                    "SELECT ST_AsText(POINT(-1e0*0e0,0e0)),ST_AsText(POINT(1e15,1e-16))"
                ),
                vec![vec!["POINT(-0 0)", "POINT(1e15 1e-16)"]]
            );
        });
    }
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE wkt_parameter FROM 'SELECT ST_AsText(ST_GeomFromText(?))'",
        )
        .unwrap();
    for wkt in geometries {
        engine
            .execute_with_session(&mut session, &format!("SET @wkt='{wkt}'"))
            .unwrap();
        let response = engine
            .execute_with_session(&mut session, "EXECUTE wkt_parameter USING @wkt")
            .and_then(QueryResponse::materialize)
            .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!()
        };
        assert_eq!(result.rows[0][0].to_display_string(), wkt);
    }
}

#[test]
fn audit1108_bit_assignments_keep_numeric_domains_and_textual_octets() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE bit_assignment(id INT PRIMARY KEY,b BIT(64))",
        "INSERT INTO bit_assignment VALUES(1,0)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for (value, hex) in [
                ("7", "7"),
                ("-1", "FFFFFFFFFFFFFFFF"),
                ("-2", "FFFFFFFFFFFFFFFE"),
                ("1.9e0", "1"),
                ("-1.9e0", "FFFFFFFFFFFFFFFF"),
                ("2.5e0", "2"),
                ("2.5", "3"),
                ("-0.4e0", "0"),
                ("18446744073709551615", "FFFFFFFFFFFFFFFF"),
                ("18446744073709551615.0", "FFFFFFFFFFFFFFFF"),
                ("'a'", "61"),
                ("'1'", "31"),
                ("'15'", "3135"),
                ("''", "0"),
                ("b'101'", "5"),
                ("_binary X'000000000000000001'", "1"),
                ("CAST(2.5 AS JSON)", "322E35"),
                (r#"CAST('"a"' AS JSON)"#, "226122"),
                ("CAST('true' AS JSON)", "74727565"),
                ("TIME'01:02:03'", "30313A30323A3033"),
                ("CAST('2001' AS YEAR)", "7D1"),
                ("CONVERT(_utf8mb4'å' USING latin1)", "E5"),
                ("_latin1 X'E5'", "E5"),
                ("'å'", "C3A5"),
            ] {
                engine
                    .execute(&format!("UPDATE bit_assignment SET b={value} WHERE id=1"))
                    .unwrap();
                assert_eq!(
                    rows(&engine, "SELECT HEX(b) FROM bit_assignment"),
                    vec![vec![hex]],
                    "{mode:?} {value}"
                );
                engine.execute(&format!("INSERT INTO bit_assignment VALUES(1,{value}) ON DUPLICATE KEY UPDATE b=VALUES(b)")).unwrap();
                assert_eq!(
                    rows(&engine, "SELECT HEX(b) FROM bit_assignment"),
                    vec![vec![hex]],
                    "ODKU {value}"
                );
            }
            for (value, code) in [
                ("-0.4", 1264),
                ("18446744073709551616.0", 1264),
                ("1e100", 1264),
                ("DATE'2020-01-01'", 1406),
                ("TIME'00:00:00.0'", 1406),
                ("'123456789'", 1406),
            ] {
                assert_eq!(
                    engine
                        .execute(&format!("UPDATE bit_assignment SET b={value}"))
                        .unwrap_err()
                        .code(),
                    code,
                    "{value}"
                );
            }
        });
    }
    engine
        .execute("CREATE TABLE bit_narrow(id INT PRIMARY KEY,a BIT(1),b BIT(4))")
        .unwrap();
    for sql in [
        "INSERT INTO bit_narrow VALUES(1,2,0)",
        "INSERT INTO bit_narrow VALUES(1,0,16)",
        "INSERT INTO bit_narrow VALUES(1,0,-1)",
        "INSERT INTO bit_narrow VALUES(1,0,'a')",
        "INSERT INTO bit_narrow VALUES(1,0,1),(2,0,16)",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1406, "{sql}");
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM bit_narrow"),
            vec![vec!["0"]]
        );
    }
}

#[test]
fn audit1108_bit_width_diagnostics_distinguish_nonstrict_from_ignore() {
    let (_dir, engine) = fixture(&["CREATE TABLE bit_diagnostics(b BIT(4))"]);
    let mut session = SessionContext::default();
    for (mode, verb, width_code) in [
        ("", "INSERT", 1264),
        ("STRICT_TRANS_TABLES", "INSERT IGNORE", 1406),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET sql_mode='{mode}'"))
            .unwrap();
        for (value, expected, codes) in [
            ("-1", "F", vec![width_code]),
            ("'a'", "F", vec![width_code]),
            ("-0.4", "0", vec![1264]),
            ("18446744073709551616.0", "F", vec![1264, width_code]),
            ("2.5e0", "2", vec![]),
            ("2.5", "3", vec![]),
            ("''", "0", vec![]),
        ] {
            engine
                .execute_with_session(&mut session, "DELETE FROM bit_diagnostics")
                .unwrap();
            let sql = format!("{verb} INTO bit_diagnostics VALUES({value})");
            engine.execute_with_session(&mut session, &sql).unwrap();
            assert_eq!(
                session
                    .warnings
                    .iter()
                    .map(|w| w.code())
                    .collect::<Vec<_>>(),
                codes,
                "{sql}"
            );
            assert_eq!(
                rows(&engine, "SELECT HEX(b) FROM bit_diagnostics"),
                vec![vec![expected]],
                "{sql}"
            );
        }
    }
}

#[test]
fn audit1108_bit_field_copies_bind_charset_temporal_fsp_and_numeric_type() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE bit_source(id INT PRIMARY KEY,n DECIMAL(25,1),f FLOAT,d DOUBLE,s VARCHAR(5) CHARACTER SET latin1,j JSON,tm TIME)",
        r#"INSERT INTO bit_source VALUES(1,2.5,2.5,1.9,'å','2.5','01:02:03'),(2,1.5,1.9,2.5,'x','"a"','00:00:00')"#,
        "CREATE TABLE bit_copy(id INT PRIMARY KEY,n BIT(4),f BIT(4),d BIT(4),s BIT(64),j BIT(64),tm BIT(64))",
        "CREATE TABLE bit_source_fsp(t TIME(1))","INSERT INTO bit_source_fsp VALUES('00:00:00.0')",
    ]);
    let expected = vec![
        vec!["1", "3", "2", "1", "E5", "322E35", "30313A30323A3033"],
        vec!["2", "2", "1", "2", "78", "226122", "30303A30303A3030"],
    ];
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            engine.execute("DELETE FROM bit_copy").unwrap();
            engine
                .execute("INSERT INTO bit_copy SELECT * FROM bit_source")
                .unwrap();
            assert_eq!(rows(&engine,"SELECT id,HEX(n),HEX(f),HEX(d),HEX(s),HEX(j),HEX(tm) FROM bit_copy ORDER BY id"),expected);
            engine.execute("UPDATE bit_copy p JOIN bit_source s ON p.id=s.id SET p.n=s.n,p.f=s.f,p.d=s.d,p.s=s.s,p.j=s.j,p.tm=s.tm").unwrap();
            assert_eq!(rows(&engine,"SELECT id,HEX(n),HEX(f),HEX(d),HEX(s),HEX(j),HEX(tm) FROM bit_copy ORDER BY id"),expected);
            assert_eq!(
                engine
                    .execute("INSERT INTO bit_copy(id,tm) SELECT 3,t FROM bit_source_fsp")
                    .unwrap_err()
                    .code(),
                1406
            );
            assert_eq!(
                rows(&engine, "SELECT COUNT(*) FROM bit_copy"),
                vec![vec!["2"]]
            );
        });
    }
}

#[test]
fn audit1108_bit_generated_and_binary_charset_identity_survive_rollback_restart() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    engine.execute("CREATE TABLE bit_generated(id INT PRIMARY KEY,s VARCHAR(5) CHARACTER SET latin1,n DECIMAL(25,1),b BIT(64) GENERATED ALWAYS AS(s) STORED,v BIT(64) GENERATED ALWAYS AS(s) VIRTUAL,nb BIT(4) GENERATED ALWAYS AS(n) STORED,bb VARBINARY(8) GENERATED ALWAYS AS(s) STORED,bv VARBINARY(8) GENERATED ALWAYS AS(s) VIRTUAL,KEY(b),KEY(v))").unwrap();
    engine
        .execute("INSERT INTO bit_generated(id,s,n) VALUES(1,'å',2.5)")
        .unwrap();
    let expected = vec![vec!["E5", "E5", "3", "E5", "E5"]];
    assert_eq!(
        rows(
            &engine,
            "SELECT HEX(b),HEX(v),HEX(nb),HEX(bb),HEX(bv) FROM bit_generated"
        ),
        expected
    );
    for index in ["b", "v"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM bit_generated FORCE INDEX({index}) WHERE {index}=229")
            ),
            vec![vec!["1"]]
        );
    }
    let mut session = SessionContext::default();
    for sql in ["BEGIN", "UPDATE bit_generated SET s='ñ',n=1.9 WHERE id=1"] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        engine
            .execute_with_session(&mut session, "UPDATE bit_generated SET n=16 WHERE id=1")
            .unwrap_err()
            .code(),
        1406
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT HEX(b),HEX(v),HEX(nb),HEX(bb),HEX(bv) FROM bit_generated"
        ),
        expected
    );
    drop(engine);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT HEX(b),HEX(v),HEX(nb),HEX(bb),HEX(bv) FROM bit_generated"
        ),
        expected
    );
    for index in ["b", "v"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM bit_generated FORCE INDEX({index}) WHERE {index}=229")
            ),
            vec![vec!["1"]]
        );
    }
}

#[test]
fn audit1108_bit_native_enum_set_sources_keep_member_identity() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE bit_native(id INT PRIMARY KEY,e ENUM('a','b',''),s SET('a','b'),b BIT(64),ge BIT(64) GENERATED ALWAYS AS(e) STORED,gs BIT(64) GENERATED ALWAYS AS(s) VIRTUAL)",
        "INSERT INTO bit_native(id,e,s) VALUES(1,'b','a,b'),(2,'',''),(3,NULL,NULL)",
        "CREATE TABLE bit_native_copy(id INT PRIMARY KEY,e BIT(64),s BIT(64))",
    ]);
    let expected = vec![
        vec!["1", "2", "3"],
        vec!["2", "3", "0"],
        vec!["3", "NULL", "NULL"],
    ];
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,HEX(ge),HEX(gs) FROM bit_native ORDER BY id"
                ),
                expected
            );
            for select in [
                "SELECT id,e,s FROM bit_native",
                "SELECT id,e,s FROM (SELECT id,e,s FROM bit_native) d",
                "WITH q AS(SELECT id,e,s FROM bit_native) SELECT id,e,s FROM q",
            ] {
                engine.execute("DELETE FROM bit_native_copy").unwrap();
                engine
                    .execute(&format!("INSERT INTO bit_native_copy {select}"))
                    .unwrap();
                assert_eq!(
                    rows(
                        &engine,
                        "SELECT id,HEX(e),HEX(s) FROM bit_native_copy ORDER BY id"
                    ),
                    expected,
                    "{mode:?} {select}"
                );
            }
            engine.execute("UPDATE bit_native SET b=e").unwrap();
            assert_eq!(
                rows(&engine, "SELECT HEX(b) FROM bit_native ORDER BY id"),
                vec![vec!["2"], vec!["3"], vec!["NULL"]]
            );
            engine
                .execute(
                    "UPDATE bit_native_copy p JOIN bit_native n ON p.id=n.id SET p.e=n.e,p.s=n.s",
                )
                .unwrap();
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,HEX(e),HEX(s) FROM bit_native_copy ORDER BY id"
                ),
                expected
            );
            engine.execute("DELETE FROM bit_native_copy").unwrap();
            engine.execute("INSERT INTO bit_native_copy SELECT id,CAST(e AS CHAR),CAST(s AS CHAR) FROM bit_native").unwrap();
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,HEX(e),HEX(s) FROM bit_native_copy ORDER BY id"
                ),
                vec![
                    vec!["1", "62", "612C62"],
                    vec!["2", "0", "0"],
                    vec!["3", "NULL", "NULL"]
                ]
            );
        });
    }
}

#[test]
fn audit1108_bit_minmax_strings_do_not_inherit_native_member_domains() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE bit_aggregate_source(id INT PRIMARY KEY,e ENUM('a','b'),s SET('a','b'),KEY(e),KEY(s))",
        "INSERT INTO bit_aggregate_source VALUES(1,'b','a,b')",
        "CREATE TABLE bit_aggregate_copy(e BIT(64),s BIT(64))",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            let QueryResponse::ResultSet(result) = engine
                .execute("SELECT MIN(e),MAX(s) FROM bit_aggregate_source")
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!()
            };
            for column in &result.columns {
                assert_eq!(
                    column.source.flags
                        & (crate::mysql::protocol::ENUM_FLAG | crate::mysql::protocol::SET_FLAG),
                    0
                );
                assert!(column.source.org_table.is_empty());
                assert!(column.source.org_name.is_empty());
                assert_eq!(column.source.wire_type, Some(254));
            }
            for select in [
                "SELECT MAX(e),MAX(s) FROM bit_aggregate_source",
                "SELECT MIN(e),MIN(s) FROM bit_aggregate_source",
                "SELECT IFNULL(e,e),IFNULL(s,s) FROM bit_aggregate_source",
                "SELECT LAG(e,0) OVER(),LAG(s,0) OVER() FROM bit_aggregate_source",
            ] {
                engine.execute("DELETE FROM bit_aggregate_copy").unwrap();
                engine
                    .execute(&format!("INSERT INTO bit_aggregate_copy {select}"))
                    .unwrap();
                assert_eq!(
                    rows(&engine, "SELECT HEX(e),HEX(s) FROM bit_aggregate_copy"),
                    vec![vec!["62", "612C62"]],
                    "{mode:?} {select}"
                );
            }
            engine.execute("DELETE FROM bit_aggregate_copy").unwrap();
            engine.execute("INSERT INTO bit_aggregate_copy SELECT e,s FROM bit_aggregate_source GROUP BY id").unwrap();
            assert_eq!(
                rows(&engine, "SELECT HEX(e),HEX(s) FROM bit_aggregate_copy"),
                vec![vec!["2", "3"]]
            );
        });
    }
}

#[test]
fn audit1108_order_alias_expressions_respect_from_precedence_and_ambiguity() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE order_alias(id INT PRIMARY KEY,c INT,g INT)",
        "INSERT INTO order_alias VALUES(1,1,1),(2,1,1),(3,9,2)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(&engine, "SELECT id,c x FROM order_alias ORDER BY x+0,id"),
                vec![vec!["1", "1"], vec!["2", "1"], vec!["3", "9"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,c+10 x FROM order_alias ORDER BY ABS(X) DESC,id LIMIT 2"
                ),
                vec![vec!["3", "19"], vec!["1", "11"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,c x FROM order_alias ORDER BY CASE WHEN x>2 THEN 0 ELSE 1 END,id"
                ),
                vec![vec!["3", "9"], vec!["1", "1"], vec!["2", "1"]]
            );
            assert_eq!(
                rows(&engine, "SELECT id,-c c FROM order_alias ORDER BY c+0,id"),
                vec![vec!["1", "-1"], vec!["2", "-1"], vec!["3", "-9"]]
            );
            assert_eq!(
                rows(&engine, "SELECT id,-c c FROM order_alias ORDER BY c,id"),
                vec![vec!["3", "-9"], vec!["1", "-1"], vec!["2", "-1"]]
            );
            assert_eq!(
                rows(&engine, "SELECT c x,c x FROM order_alias ORDER BY x+0"),
                vec![vec!["1", "1"], vec!["1", "1"], vec!["9", "9"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT c x,id c FROM order_alias ORDER BY x DESC,id"
                ),
                vec![vec!["9", "3"], vec!["1", "1"], vec!["1", "2"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT c x,id c FROM order_alias ORDER BY 1 DESC,id"
                ),
                vec![vec!["9", "3"], vec!["1", "1"], vec!["1", "2"]]
            );
            assert_eq!(
                rows(&engine, "SELECT id c,c c FROM order_alias ORDER BY c+0,id"),
                vec![vec!["1", "1"], vec!["2", "1"], vec!["3", "9"]]
            );
            for sql in [
            "SELECT id x,c x FROM order_alias ORDER BY x+0",
            "SELECT id x,c x FROM order_alias ORDER BY x",
            "SELECT a.id,a.c c FROM order_alias a JOIN order_alias b ON a.id=b.id ORDER BY ABS(c)",
        ] {assert_eq!(engine.execute(sql).and_then(QueryResponse::materialize).unwrap_err().code(),1052,"{mode:?} {sql}");}
            assert_eq!(rows(&engine,"SELECT a.id,a.c c FROM order_alias a JOIN order_alias b ON a.id=b.id ORDER BY c DESC,a.id LIMIT 1"),vec![vec!["3","9"]]);
            assert_eq!(
                rows(
                    &engine,
                    "SELECT DISTINCT c x FROM order_alias ORDER BY ABS(x) DESC LIMIT 1"
                ),
                vec![vec!["9"]]
            );
            assert_eq!(rows(&engine,"WITH q AS(SELECT id,c x FROM order_alias) SELECT id,x z FROM q ORDER BY ABS(z) DESC,id LIMIT 1"),vec![vec!["3","9"]]);
            assert_eq!(rows(&engine,"SELECT id,c x FROM order_alias ORDER BY (SELECT x FROM (SELECT 1 x) local_scope),id"),vec![vec!["1","1"],vec!["2","1"],vec!["3","9"]]);
        });
    }
}

#[test]
fn audit1108_order_alias_grouping_uses_source_keys_including_rollup_nulls() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE order_group(id INT PRIMARY KEY,c INT,g INT)",
        "INSERT INTO order_group VALUES(1,1,1),(2,1,1),(3,9,2)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    "SELECT g,COUNT(*) c FROM order_group GROUP BY g,order_group.c ORDER BY c+0"
                ),
                vec![vec!["1", "2"], vec!["2", "1"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT g,COUNT(*) c FROM order_group GROUP BY g,order_group.c ORDER BY c"
                ),
                vec![vec!["2", "1"], vec!["1", "2"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT g,COUNT(*) cnt FROM order_group GROUP BY g ORDER BY ABS(cnt),g LIMIT 1"
                ),
                vec![vec!["2", "1"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT c+1 x,COUNT(*) n FROM order_group GROUP BY c+1 ORDER BY ABS(x) DESC"
                ),
                vec![vec!["10", "1"], vec!["2", "2"]]
            );
            for column in ["c", "order_group.c"] {
                let sql=format!("SELECT g,COUNT(*) c,GROUPING(g),GROUPING({column}) FROM order_group GROUP BY g,{column} WITH ROLLUP ORDER BY c+0,g,GROUPING({column})");
                assert_eq!(
                    rows(&engine, &sql),
                    vec![
                        vec!["NULL", "3", "1", "1"],
                        vec!["1", "2", "0", "1"],
                        vec!["2", "1", "0", "1"],
                        vec!["1", "2", "0", "0"],
                        vec!["2", "1", "0", "0"]
                    ],
                    "{mode:?} {sql}"
                );
            }
        });
    }
}

#[test]
fn audit1108_order_alias_windows_prepared_and_view_column_renames() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE order_scopes(id INT PRIMARY KEY,c INT)",
        "INSERT INTO order_scopes VALUES(1,1),(2,1),(3,9)",
        "CREATE VIEW order_view(a,b) AS SELECT id,c x FROM order_scopes ORDER BY ABS(x) DESC,id LIMIT 2",
        "CREATE VIEW order_view_chain(a,b) AS SELECT c x,id c FROM order_scopes ORDER BY x DESC,id LIMIT 1",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(&engine, "SELECT a,b FROM order_view ORDER BY a"),
                vec![vec!["1", "1"], vec!["3", "9"]]
            );
            assert_eq!(
                rows(&engine, "SELECT a,b FROM order_view_chain"),
                vec![vec!["9", "3"]]
            );
            assert_eq!(rows(&engine,"SELECT id,ROW_NUMBER() OVER(ORDER BY c,id) x FROM order_scopes ORDER BY x+0 DESC LIMIT 2"),vec![vec!["3","3"],vec!["2","2"]]);
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,c x FROM order_scopes ORDER BY SUM(x) OVER(),id DESC LIMIT 1"
                ),
                vec![vec!["3", "9"]]
            );
        });
    }
    let mut session = SessionContext::default();
    for sql in ["PREPARE order_alias_ps FROM 'SELECT id,c+? x FROM order_scopes ORDER BY ABS(x)+? DESC,id LIMIT ?'","SET @a=10,@b=0,@n=2"] {
        engine.execute_with_session(&mut session,sql).unwrap();
    }
    let result = engine
        .execute_with_session(&mut session, "EXECUTE order_alias_ps USING @a,@b,@n")
        .and_then(QueryResponse::materialize)
        .unwrap();
    let QueryResponse::ResultSet(result) = result else {
        panic!()
    };
    assert_eq!(
        result
            .rows
            .iter()
            .map(|r| r.iter().map(Value::to_display_string).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec!["3", "19"], vec!["1", "11"]]
    );
}

#[test]
fn audit1108_order_alias_expansion_is_admitted_before_cloning() {
    use crate::engine::executor::mem_budget::{
        with_query_budget, with_test_effective_memory_limit, MemBudget,
    };
    let (_dir, engine) = fixture(&[]);
    let sql = format!("SELECT CONCAT('{}') k ORDER BY k,k,k", "x".repeat(10_000));
    let crate::parser::Statement::Select(mut select) =
        crate::parser::parse_statement(&sql).unwrap()
    else {
        panic!()
    };
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    with_query_budget(Arc::clone(&budget), || {
        with_test_effective_memory_limit(1024, || {
            assert!(
                crate::planner::resolve_order_by_output_refs(engine.database(), &mut select)
                    .is_err()
            );
            assert!(select
                .order_by
                .iter()
                .all(|order| matches!(order.expr, crate::parser::Expr::Identifier(_))));
        });
        assert!(budget.terminal_oom().is_some());
    });
    crate::planner::resolve_order_by_output_refs(engine.database(), &mut select).unwrap();
    assert!(select.order_by.iter().all(|order| order.names_bound));
    assert_eq!(rows(&engine, "SELECT 1"), vec![vec!["1"]]);
}

#[test]
fn audit1108_order_alias_bound_view_state_survives_strict_restart() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    for sql in ["CREATE TABLE order_restart(id INT PRIMARY KEY,c INT)","INSERT INTO order_restart VALUES(1,1),(2,1),(3,9)","CREATE VIEW order_restart_alias(a,b) AS SELECT c x,id c FROM order_restart ORDER BY x DESC,id LIMIT 2","CREATE VIEW order_restart_ordinal(a,b) AS SELECT c x,id c FROM order_restart ORDER BY 1 DESC,id LIMIT 2"] {engine.execute(sql).unwrap();}
    let expected = vec![vec!["9", "3"], vec!["1", "1"]];
    for view in ["order_restart_alias", "order_restart_ordinal"] {
        assert_eq!(rows(&engine, &format!("SELECT a,b FROM {view}")), expected);
    }
    drop(engine);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    for view in ["order_restart_alias", "order_restart_ordinal"] {
        assert_eq!(rows(&engine, &format!("SELECT a,b FROM {view}")), expected);
    }
}

#[test]
fn audit1108_order_alias_and_ordinal_remain_distinct_in_plan_cache() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE order_cache(id INT PRIMARY KEY,c INT)",
        "INSERT INTO order_cache VALUES(1,1),(2,1),(3,9)",
    ]);
    for _ in 0..8 {
        for order in ["x", "1"] {
            assert_eq!(
                rows(
                    &engine,
                    &format!("SELECT c x,id c FROM order_cache ORDER BY {order} DESC,id")
                ),
                vec![vec!["9", "3"], vec!["1", "1"], vec!["1", "2"]]
            );
        }
        assert_eq!(
            rows(
                &engine,
                "SELECT c x,id c FROM order_cache ORDER BY c DESC,id"
            ),
            vec![vec!["9", "3"], vec!["1", "2"], vec!["1", "1"]]
        );
    }
}

#[test]
fn audit1108_enum_set_assignment_checks_numeric_domain_and_token_boundaries() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE enum_set_assign(id INT PRIMARY KEY,e ENUM('a','b','C'),s SET('a','b','c'))",
        "CREATE TABLE enum_set_source(id INT PRIMARY KEY,n DECIMAL(25,1),f DOUBLE,t VARCHAR(30))",
        "INSERT INTO enum_set_source VALUES(1,2.5,2.5e0,'b  '),(2,-0.5,-0.5e0,'C ')",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            engine.execute("DELETE FROM enum_set_assign").unwrap();
            engine
                .execute("INSERT INTO enum_set_assign SELECT id,t,n FROM enum_set_source")
                .unwrap();
            assert_eq!(
                rows(&engine, "SELECT id,e,s FROM enum_set_assign ORDER BY id"),
                vec![vec!["1", "b", "b"], vec!["2", "C", ""]]
            );
            engine
                .execute("UPDATE enum_set_assign a JOIN enum_set_source b ON a.id=b.id SET a.s=b.f")
                .unwrap();
            assert_eq!(
                rows(&engine, "SELECT s FROM enum_set_assign ORDER BY id"),
                vec![vec!["b"], vec![""]]
            );
            for (value, expected) in [
                ("1.9", "a"),
                ("2.5e0", "b"),
                ("7.9", "a,b,c"),
                ("-0.5e0", ""),
                ("'01'", "a"),
                ("'+1'", "a"),
                ("'7'", "a,b,c"),
                ("'a,b '", "a,b"),
                ("'A,a,b'", "a,b"),
                ("''", ""),
            ] {
                engine.execute(&format!("INSERT INTO enum_set_assign VALUES(1,'B ',{value}) ON DUPLICATE KEY UPDATE e=VALUES(e),s=VALUES(s)")).unwrap();
                assert_eq!(
                    rows(&engine, "SELECT e,s FROM enum_set_assign WHERE id=1"),
                    vec![vec!["b", expected]],
                    "{mode:?} {value}"
                );
            }
            for value in [
                "8",
                "9",
                "-1",
                "18446744073709551615",
                "8.1e0",
                "'9'",
                "'1 '",
                "' '",
                "','",
                "'a,'",
                "'a,,b'",
                "'a ,b'",
                "'a, b'",
                "CAST(9 AS JSON)",
                "b'101'",
            ] {
                let sql = format!("INSERT INTO enum_set_assign VALUES(3,'b',{value})");
                assert_eq!(
                    engine.execute(&sql).unwrap_err().code(),
                    1265,
                    "{mode:?} {sql}"
                );
            }
            for text in ["' b'", "'b\\t'", "'b '"] {
                assert_eq!(
                    engine
                        .execute(&format!("UPDATE enum_set_assign SET e={text} WHERE id=1"))
                        .unwrap_err()
                        .code(),
                    1265,
                    "{text}"
                );
            }
            assert_eq!(
                rows(&engine, "SELECT COUNT(*) FROM enum_set_assign"),
                vec![vec!["2"]]
            );
            assert_eq!(
                engine
                    .execute("INSERT INTO enum_set_assign VALUES(3,'a',1),(4,'b',8)")
                    .unwrap_err()
                    .code(),
                1265
            );
            assert_eq!(
                rows(&engine, "SELECT COUNT(*) FROM enum_set_assign"),
                vec![vec!["2"]]
            );
        });
    }
}

#[test]
fn audit1108_enum_set_nonstrict_and_ignore_warn_without_discarding_valid_bits() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE set_loose(id INT PRIMARY KEY,e ENUM('a','b'),s SET('a','b','c'),n BIGINT)",
    ]);
    let mut session = SessionContext::default();
    for (mode, insert) in [("", "INSERT"), ("STRICT_TRANS_TABLES", "INSERT IGNORE")] {
        engine
            .execute_with_session(&mut session, &format!("SET sql_mode='{mode}'"))
            .unwrap();
        for (value, expected) in [
            ("9", "a"),
            ("-1", "a,b,c"),
            ("-2", "b,c"),
            ("-8", ""),
            ("18446744073709551615", "a,b,c"),
            ("'9'", ""),
            ("'a,'", "a"),
            ("'a,,b'", "a,b"),
            ("'a ,b '", "b"),
            ("'z,a'", "a"),
        ] {
            engine
                .execute_with_session(&mut session, "DELETE FROM set_loose")
                .unwrap();
            let sql = format!("{insert} INTO set_loose VALUES(1,'b ',{value},9223372036854775808)");
            engine.execute_with_session(&mut session, &sql).unwrap();
            let mut codes = session
                .warnings
                .iter()
                .map(|w| w.code())
                .collect::<Vec<_>>();
            codes.sort_unstable();
            assert_eq!(codes, vec![1264, 1265], "{sql}");
            assert_eq!(
                rows(&engine, "SELECT e,s,n FROM set_loose"),
                vec![vec!["b", expected, "9223372036854775807"]],
                "{sql}"
            );
        }
    }
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "UPDATE set_loose SET n=18446744073709551615")
            .unwrap_err()
            .code(),
        1264
    );
    assert_eq!(
        rows(&engine, "SELECT n FROM set_loose"),
        vec![vec!["9223372036854775807"]]
    );
}

#[test]
fn audit1108_enum_set_full_width_and_prepared_values_keep_exact_masks() {
    let members = (0..64)
        .map(|i| format!("'m{i}'"))
        .collect::<Vec<_>>()
        .join(",");
    let (_dir, engine) = fixture(&[&format!(
        "CREATE TABLE full_set(id INT PRIMARY KEY,s SET({members}))"
    )]);
    let all = (0..64)
        .map(|i| format!("m{i}"))
        .collect::<Vec<_>>()
        .join(",");
    for value in [
        "-1",
        "18446744073709551615",
        "18446744073709551615.0",
        "'18446744073709551615'",
        "'-1'",
    ] {
        engine.execute("DELETE FROM full_set").unwrap();
        engine
            .execute(&format!("INSERT INTO full_set VALUES(1,{value})"))
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT s FROM full_set"),
            vec![vec![all.clone()]],
            "{value}"
        );
    }
    engine
        .execute("UPDATE full_set SET s=9223372036854775808.0")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT s FROM full_set"), vec![vec!["m63"]]);
    assert_eq!(
        engine
            .execute("UPDATE full_set SET s=18446744073709551616.0")
            .unwrap_err()
            .code(),
        1265
    );
    assert_eq!(rows(&engine, "SELECT s FROM full_set"), vec![vec!["m63"]]);
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE set_ps(s SET('a','b','c'))",
        "PREPARE set_insert FROM 'INSERT INTO set_ps VALUES(?)'",
        "SET @s=2.5e0",
        "EXECUTE set_insert USING @s",
        "SET @s='7'",
        "EXECUTE set_insert USING @s",
        "SET @s='9'",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        engine
            .execute_with_session(&mut session, "EXECUTE set_insert USING @s")
            .unwrap_err()
            .code(),
        1265
    );
    assert_eq!(
        rows(&engine, "SELECT s FROM set_ps ORDER BY s"),
        vec![vec!["b"], vec!["a,b,c"]]
    );
}

#[test]
fn audit1108_enum_set_generated_columns_rollback_and_strict_restart() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    for sql in [
        "CREATE TABLE set_generated(id INT PRIMARY KEY,n DECIMAL(25,1),t VARCHAR(30),s SET('a','b','c') GENERATED ALWAYS AS(n) STORED,v SET('a','b','c') GENERATED ALWAYS AS(n) VIRTUAL,e ENUM('a','b') GENERATED ALWAYS AS(t) STORED,KEY(s),KEY(v))",
        "INSERT INTO set_generated(id,n,t) VALUES(1,2.5,'b  '),(2,1.9,'a ')",
    ] {engine.execute(sql).unwrap();}
    let expected = vec![vec!["1", "b", "b", "b"], vec!["2", "a", "a", "a"]];
    assert_eq!(
        rows(&engine, "SELECT id,s,v,e FROM set_generated ORDER BY id"),
        expected
    );
    for index in ["s", "v"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM set_generated FORCE INDEX({index}) WHERE {index}='b'")
            ),
            vec![vec!["1"]]
        );
    }
    let mut session = SessionContext::default();
    for sql in ["BEGIN", "UPDATE set_generated SET n=3.5,t='a ' WHERE id=1"] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        engine
            .execute_with_session(&mut session, "UPDATE set_generated SET n=9 WHERE id=2")
            .unwrap_err()
            .code(),
        1265
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,s,v,e FROM set_generated ORDER BY id"),
        expected
    );
    drop(engine);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,s,v,e FROM set_generated ORDER BY id"),
        expected
    );
    for index in ["s", "v"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM set_generated FORCE INDEX({index}) WHERE {index}='b'")
            ),
            vec![vec!["1"]]
        );
    }
}

#[test]
fn audit1108_signed_zero_optional_batch_refusal_does_not_cancel_sql() {
    use crate::engine::executor::mem_budget::{
        with_query_budget, with_test_effective_memory_limit, MemBudget,
    };
    use crate::engine::executor::select::plan::IndexEntryProjector;
    let (_dir, engine) = fixture(&["CREATE TABLE zero_optional(d DOUBLE,KEY(d))"]);
    let db = engine.database();
    let schema = db.table("zero_optional").unwrap();
    let index = schema
        .indexes
        .iter()
        .find(|index| index.name == "d")
        .unwrap();
    let table = crate::planner::PlanTable::new(schema.id, Arc::clone(&schema), None);
    let projector = IndexEntryProjector::new(&table, index, None);
    let entries = (0..16)
        .map(|slot_id| {
            (
                crate::storage::IndexKey(smallvec::smallvec![
                    crate::storage::schema::KeyPart::Float64(0x8000_0000_0000_0000,)
                ]),
                crate::storage::RowId {
                    page_id: 1,
                    slot_id,
                },
            )
        })
        .collect::<Vec<_>>();
    let budget = Arc::new(MemBudget::new(1024 * 1024));
    with_query_budget(Arc::clone(&budget), || {
        with_test_effective_memory_limit(1024, || {
            let batch = projector
                .prepare_float_rows(
                    db,
                    crate::storage::TxnView::read_only(
                        0,
                        crate::storage::IsolationLevel::ReadCommitted,
                    ),
                    entries.iter().map(|(key, id)| (key, *id)),
                    || panic!("an accelerator refused admission before accessing the heap"),
                )
                .unwrap();
            assert!(batch.is_none());
            crate::engine::cancel::check_cancelled().unwrap();
        });
        assert!(budget.terminal_oom().is_none());
    });
    assert_eq!(rows(&engine, "SELECT 1"), vec![vec!["1"]]);
}

#[test]
fn audit1108_signed_zero_batches_keep_index_order_nulls_and_old_versions() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE zero_batch(id INT PRIMARY KEY,p INT,f FLOAT,d DOUBLE)",
        "CREATE INDEX kfd ON zero_batch(f,d) INCLUDE(id,p)",
        "CREATE INDEX kip ON zero_batch(p) INCLUDE(id,f,d)",
        "CREATE INDEX kd ON zero_batch(d,id) INCLUDE(p,f)",
        "CREATE TABLE zero_batch_probe(id INT PRIMARY KEY,d DOUBLE)",
        "INSERT INTO zero_batch_probe VALUES(1,0e0),(2,2.5e0)",
    ]);
    let tuples = (0..1024)
        .map(|id| {
            let f = match id % 4 {
                0 => "-1e0*0e0",
                1 => "0e0",
                2 => "NULL",
                _ => "1.25e0",
            };
            let d = match id % 5 {
                0 => "-1e0*0e0",
                1 => "0e0",
                2 => "NULL",
                _ => "2.5e0",
            };
            format!("({id},{},{f},{d})", id % 2)
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zero_batch VALUES{tuples}"))
        .unwrap();
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for predicate in ["p=1", "d=0", "f=0 OR f IS NULL", "d IS NULL"] {
                let expected=rows(&engine,&format!("SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch IGNORE INDEX(kfd,kip,kd) WHERE {predicate} ORDER BY id"));
                for index in ["kfd", "kip", "kd"] {
                    assert_eq!(rows(&engine,&format!("SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch FORCE INDEX({index}) WHERE {predicate} ORDER BY id")),expected,"{mode:?} {index} {predicate}");
                }
            }
            for suffix in [
                "ORDER BY id LIMIT 37",
                "ORDER BY id DESC LIMIT 37",
                "ORDER BY FORMAT(d,2),id LIMIT 50",
            ] {
                assert_eq!(rows(&engine,&format!("SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch FORCE INDEX(kip) WHERE p=1 {suffix}")),rows(&engine,&format!("SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch IGNORE INDEX(kfd,kip,kd) WHERE p=1 {suffix}")));
            }
            assert_eq!(rows(&engine,"SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch FORCE INDEX(kd) WHERE d=0 ORDER BY id DESC LIMIT 73"),rows(&engine,"SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch IGNORE INDEX(kfd,kip,kd) WHERE d=0 ORDER BY id DESC LIMIT 73"));
            assert_eq!(rows(&engine,"SELECT b.id,FORMAT(b.d,2) FROM zero_batch_probe a STRAIGHT_JOIN zero_batch b FORCE INDEX(kd) ON b.d=a.d ORDER BY a.id,b.id"),rows(&engine,"SELECT b.id,FORMAT(b.d,2) FROM zero_batch_probe a STRAIGHT_JOIN zero_batch b IGNORE INDEX(kfd,kip,kd) ON b.d=a.d ORDER BY a.id,b.id"));
        });
    }
    let mut rr = SessionContext::default();
    engine
        .execute_with_session(
            &mut rr,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine.execute_with_session(&mut rr, "BEGIN").unwrap();
    let read = |session: &mut SessionContext, index: &str| {
        let response=engine.execute_with_session(session,&format!("SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch {index} WHERE p=1 ORDER BY id")).and_then(QueryResponse::materialize).unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!()
        };
        result.rows
    };
    let expected = read(&mut rr, "IGNORE INDEX(kfd,kip,kd)");
    engine
        .execute("UPDATE zero_batch SET f=-f,d=-d WHERE id%7=0")
        .unwrap();
    engine
        .execute("DELETE FROM zero_batch WHERE id%11=0")
        .unwrap();
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for index in ["kfd", "kip", "kd"] {
                assert_eq!(
                    read(&mut rr, &format!("FORCE INDEX({index})")),
                    expected,
                    "{mode:?} old {index}"
                );
            }
        });
    }
    engine.execute_with_session(&mut rr, "ROLLBACK").unwrap();
    assert_eq!(rows(&engine,"SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch FORCE INDEX(kip) WHERE p=1 ORDER BY id"),rows(&engine,"SELECT id,FORMAT(f,2),FORMAT(d,2) FROM zero_batch IGNORE INDEX(kfd,kip,kd) WHERE p=1 ORDER BY id"));
}

#[test]
fn audit1108_signed_zero_batches_use_the_scanned_partition_heap() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE zero_batch_part(id INT,p INT,d DOUBLE,PRIMARY KEY(id,p)) PARTITION BY HASH(p) PARTITIONS 2",
        "CREATE INDEX kd ON zero_batch_part(d) INCLUDE(id,p)",
    ]);
    let tuples = (0..128)
        .map(|id| {
            format!(
                "({id},{},{})",
                id % 2,
                if id % 2 == 0 { "-1e0*0e0" } else { "0e0" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO zero_batch_part VALUES{tuples}"))
        .unwrap();
    let expected = rows(
        &engine,
        "SELECT id,p,FORMAT(d,2) FROM zero_batch_part IGNORE INDEX(kd) WHERE d=0 ORDER BY id",
    );
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT id,p,FORMAT(d,2) FROM zero_batch_part FORCE INDEX(kd) WHERE d=0 ORDER BY id"),expected,"{mode:?}")
        });
    }
}

#[test]
fn audit1108_signed_zero_timestamp_changes_precede_before_triggers() {
    let (_dir,engine)=fixture(&[
        "CREATE TABLE zero_ts(id INT PRIMARY KEY,d DOUBLE,tag INT,ts TIMESTAMP DEFAULT '2000-01-01' ON UPDATE CURRENT_TIMESTAMP)",
        "CREATE TABLE zero_ts_seen(n BIGINT)",
        "INSERT INTO zero_ts(id,d,tag) VALUES(1,-1e0*0e0,0)",
        "CREATE TRIGGER zero_ts_observe BEFORE UPDATE ON zero_ts FOR EACH ROW BEGIN INSERT INTO zero_ts_seen VALUES(UNIX_TIMESTAMP(NEW.ts)); SET NEW.tag=OLD.tag; END",
        "CREATE TABLE zero_ts_flip(id INT PRIMARY KEY,d DOUBLE,tag INT,ts TIMESTAMP DEFAULT '2000-01-01' ON UPDATE CURRENT_TIMESTAMP)",
        "INSERT INTO zero_ts_flip(id,d,tag) VALUES(1,-1e0*0e0,0)",
        "CREATE TRIGGER zero_ts_only_flip BEFORE UPDATE ON zero_ts_flip FOR EACH ROW SET NEW.d=-OLD.d",
    ]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET time_zone='+00:00'")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET timestamp=1700000000")
        .unwrap();
    let response = engine
        .execute_with_session(
            &mut session,
            "INSERT INTO zero_ts(id,d,tag) VALUES(1,0e0,0) ON DUPLICATE KEY UPDATE d=VALUES(d)",
        )
        .unwrap();
    assert!(
        matches!(response, QueryResponse::OkAffected { affected_rows: 2 }),
        "{response:?}"
    );
    assert_eq!(
        rows(&engine, "SELECT n FROM zero_ts_seen"),
        vec![vec!["1700000000"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2),UNIX_TIMESTAMP(ts) FROM zero_ts"
        ),
        vec![vec!["0.00", "1700000000"]]
    );
    engine
        .execute_with_session(&mut session, "SET timestamp=1800000000")
        .unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE zero_ts SET d=-1e0*0e0 WHERE id=1")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2),UNIX_TIMESTAMP(ts) FROM zero_ts"
        ),
        vec![vec!["-0.00", "1800000000"]]
    );
    engine
        .execute(
            "CREATE TRIGGER zero_ts_hold BEFORE UPDATE ON zero_ts FOR EACH ROW SET NEW.ts=OLD.ts",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "SET timestamp=1900000000")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO zero_ts(id,d,tag) VALUES(1,0e0,0) ON DUPLICATE KEY UPDATE d=VALUES(d)",
        )
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT n FROM zero_ts_seen ORDER BY n"),
        vec![vec!["1700000000"], vec!["1800000000"], vec!["1900000000"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2),UNIX_TIMESTAMP(ts) FROM zero_ts"
        ),
        vec![vec!["0.00", "1800000000"]]
    );
    engine
        .execute_with_session(&mut session, "UPDATE zero_ts_flip SET tag=tag WHERE id=1")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2),CAST(ts AS CHAR) FROM zero_ts_flip"
        ),
        vec![vec!["0.00", "2000-01-01 00:00:00"]]
    );
    engine.execute_with_session(&mut session,"INSERT INTO zero_ts_flip(id,d,tag) VALUES(1,0e0,0) ON DUPLICATE KEY UPDATE tag=VALUES(tag)").unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2),CAST(ts AS CHAR) FROM zero_ts_flip"
        ),
        vec![vec!["-0.00", "2000-01-01 00:00:00"]]
    );
}

#[test]
fn audit1108_signed_zero_updates_change_stored_float_payloads() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            let (_dir,engine)=fixture(&[
                "CREATE TABLE zero_payload(id INT PRIMARY KEY,f FLOAT,d DOUBLE,KEY kf(f),KEY kd(d))",
                "INSERT INTO zero_payload VALUES(1,-1e0*0e0,-1e0*0e0)",
                "CREATE TABLE zero_generated(id INT PRIMARY KEY,d DOUBLE,g VARCHAR(20) AS(CONCAT(d)) STORED,v VARCHAR(20) AS(CONCAT(d)) VIRTUAL,KEY(g),KEY(v))",
                "INSERT INTO zero_generated(id,d) VALUES(1,-1e0*0e0)",
            ]);
            for (value, affected, expected) in [
                ("0e0", 1, "0.00"),
                ("0e0", 0, "0.00"),
                ("-1e0*0e0", 1, "-0.00"),
            ] {
                let response = engine
                    .execute(&format!(
                        "UPDATE zero_payload SET f={value},d={value} WHERE id=1"
                    ))
                    .unwrap();
                assert!(
                    matches!(response,QueryResponse::OkAffected{affected_rows} if affected_rows==affected),
                    "{value}: {response:?}"
                );
                for access in ["IGNORE INDEX(kf,kd)", "FORCE INDEX(kf)", "FORCE INDEX(kd)"] {
                    assert_eq!(rows(&engine,&format!("SELECT FORMAT(f,2),FORMAT(d,2) FROM zero_payload {access} WHERE f=0 AND d=0")),vec![vec![expected,expected]]);
                }
            }
            let response=engine.execute("INSERT INTO zero_payload VALUES(1,0e0,0e0) ON DUPLICATE KEY UPDATE f=VALUES(f),d=VALUES(d)").unwrap();
            assert!(
                matches!(response, QueryResponse::OkAffected { affected_rows: 2 }),
                "{response:?}"
            );
            let response=engine.execute("INSERT INTO zero_payload VALUES(1,0e0,0e0) ON DUPLICATE KEY UPDATE f=VALUES(f),d=VALUES(d)").unwrap();
            assert!(
                matches!(response, QueryResponse::OkAffected { affected_rows: 0 }),
                "{response:?}"
            );
            for (value, expected) in [("0e0", "0"), ("-1e0*0e0", "-0")] {
                engine
                    .execute(&format!("UPDATE zero_generated SET d={value} WHERE id=1"))
                    .unwrap();
                for column in ["g", "v"] {
                    assert_eq!(rows(&engine,&format!("SELECT id,{column} FROM zero_generated FORCE INDEX({column}) WHERE {column}='{expected}'")),vec![vec!["1",expected]]);
                }
            }
        });
    }
}

#[test]
fn audit1108_signed_zero_updates_preserve_snapshot_and_rollback_versions() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE zero_mvcc(id INT PRIMARY KEY,d DOUBLE,KEY(d))",
        "INSERT INTO zero_mvcc VALUES(1,-1e0*0e0)",
    ]);
    let mut rr = SessionContext::default();
    let mut rc = SessionContext::default();
    engine
        .execute_with_session(
            &mut rr,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut rc,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .unwrap();
    engine.execute_with_session(&mut rr, "BEGIN").unwrap();
    engine.execute_with_session(&mut rc, "BEGIN").unwrap();
    let read = |session: &mut SessionContext, access: &str| {
        let response = engine
            .execute_with_session(
                session,
                &format!("SELECT FORMAT(d,2) FROM zero_mvcc {access} WHERE d=0"),
            )
            .and_then(QueryResponse::materialize)
            .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!()
        };
        result
            .rows
            .into_iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(read(&mut rr, "IGNORE INDEX(d)"), vec!["-0.00"]);
    assert_eq!(read(&mut rc, "FORCE INDEX(d)"), vec!["-0.00"]);
    engine
        .execute("UPDATE zero_mvcc SET d=0e0 WHERE id=1")
        .unwrap();
    for access in ["IGNORE INDEX(d)", "FORCE INDEX(d)"] {
        assert_eq!(read(&mut rr, access), vec!["-0.00"]);
        assert_eq!(read(&mut rc, access), vec!["0.00"]);
    }
    let mut writer = SessionContext::default();
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE zero_mvcc SET d=-1e0*0e0 WHERE id=1")
        .unwrap();
    assert_eq!(read(&mut writer, "FORCE INDEX(d)"), vec!["-0.00"]);
    assert_eq!(read(&mut rc, "FORCE INDEX(d)"), vec!["0.00"]);
    engine
        .execute_with_session(&mut writer, "ROLLBACK")
        .unwrap();
    assert_eq!(read(&mut rc, "FORCE INDEX(d)"), vec!["0.00"]);
    assert_eq!(read(&mut rr, "FORCE INDEX(d)"), vec!["-0.00"]);
    engine.execute_with_session(&mut rr, "ROLLBACK").unwrap();
    engine.execute_with_session(&mut rc, "COMMIT").unwrap();
}

#[test]
fn audit1108_signed_zero_updates_include_prepared_and_trigger_changes() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE zero_prepared(id INT PRIMARY KEY,d DOUBLE,KEY(d))",
        "INSERT INTO zero_prepared VALUES(1,-1e0*0e0)",
        "CREATE TABLE zero_trigger(id INT PRIMARY KEY,d DOUBLE,tag INT,KEY(d))",
        "INSERT INTO zero_trigger VALUES(1,-1e0*0e0,0)",
        "CREATE TRIGGER zero_flip BEFORE UPDATE ON zero_trigger FOR EACH ROW SET NEW.d=-OLD.d",
    ]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE zero_p FROM 'UPDATE zero_prepared SET d=? WHERE id=?'",
        )
        .unwrap();
    for (value, affected, expected) in [
        ("0e0", 1, "0.00"),
        ("0e0", 0, "0.00"),
        ("-1e0*0e0", 1, "-0.00"),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET @d={value},@id=1"))
            .unwrap();
        let response = engine
            .execute_with_session(&mut session, "EXECUTE zero_p USING @d,@id")
            .unwrap();
        assert!(
            matches!(response,QueryResponse::OkAffected{affected_rows} if affected_rows==affected),
            "{response:?}"
        );
        assert_eq!(
            rows(
                &engine,
                "SELECT FORMAT(d,2) FROM zero_prepared FORCE INDEX(d) WHERE d=0"
            ),
            vec![vec![expected]]
        );
    }
    for expected in ["0.00", "-0.00"] {
        let response = engine
            .execute("UPDATE zero_trigger SET tag=tag WHERE id=1")
            .unwrap();
        assert!(
            matches!(response, QueryResponse::OkAffected { affected_rows: 1 }),
            "{response:?}"
        );
        assert_eq!(
            rows(
                &engine,
                "SELECT FORMAT(d,2) FROM zero_trigger FORCE INDEX(d) WHERE d=0"
            ),
            vec![vec![expected]]
        );
    }
    let response = engine
        .execute("INSERT INTO zero_trigger VALUES(1,0e0,0) ON DUPLICATE KEY UPDATE tag=VALUES(tag)")
        .unwrap();
    assert!(
        matches!(response, QueryResponse::OkAffected { affected_rows: 2 }),
        "{response:?}"
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2) FROM zero_trigger FORCE INDEX(d) WHERE d=0"
        ),
        vec![vec!["0.00"]]
    );
}

#[test]
fn audit1108_signed_zero_update_payload_survives_restart() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE zero_restart(id INT PRIMARY KEY,d DOUBLE,KEY(d))")
            .unwrap();
        engine
            .execute("INSERT INTO zero_restart VALUES(1,-1e0*0e0)")
            .unwrap();
        engine
            .execute("UPDATE zero_restart SET d=0e0 WHERE id=1")
            .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(
            &engine,
            "SELECT FORMAT(d,2) FROM zero_restart FORCE INDEX(d) WHERE d=0"
        ),
        vec![vec!["0.00"]]
    );
}

#[test]
fn audit1108_numeric_prefix_identifiers_are_never_constants() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(
        rows(
            &engine,
            "SELECT 1e,1Ea,3e_,1e+1 FROM (SELECT 5 AS `1e`,6 AS `1Ea`,7 AS `3e_`) d ORDER BY 1e"
        ),
        vec![vec!["5", "6", "7", "10"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT 1e 'alias' FROM (SELECT 5 AS `1e`) d WHERE 1e + 2=7"
        ),
        vec![vec!["5"]]
    );
    for sql in ["SELECT 1ea", "SELECT 3e_"] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1054, "{sql}");
    }
    for sql in ["SELECT 1.5e", "SELECT 1.5ea", "SELECT 1.e"] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1064, "{sql}");
    }
    assert_eq!(
        rows(&engine, "SELECT 1e5x,2e-1,1 `e`,'1e'"),
        vec![vec!["100000", "0.2", "1", "1e"]]
    );
}

#[test]
fn audit1108_comment_boundaries_follow_sql_quote_modes() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(rows(&engine, "SELECT 1 --"), vec![vec!["1"]]);
    assert_eq!(
        rows(
            &engine,
            r#"SELECT /*! 'a*/b' */ AS x,/*! 'a\'*/b' */ AS y,/*!80000 '*/' */ AS z"#
        ),
        vec![vec!["a*/b", "a'*/b", "*/"]]
    );
    assert_eq!(
        engine
            .execute("SELECT 1 /*!999999 '*/' */")
            .unwrap_err()
            .code(),
        1064
    );
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'")
        .unwrap();
    let response = engine
        .execute_with_session(&mut session, r#"SELECT /*! 'a\*/b' */ AS x"#)
        .and_then(QueryResponse::materialize)
        .unwrap();
    let QueryResponse::ResultSet(result) = response else {
        panic!()
    };
    assert_eq!(result.rows, vec![vec![Value::String(r"a\*/b".into())]]);
}

#[test]
fn audit1108_charset_introducers_accept_bit_literals() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(rows(&engine,"SELECT HEX(_binary b'01000001'),HEX(_binary 0b01000001),HEX(_latin1 b'01000001'),CHARSET(_latin1 b'01000001'),CHARSET(_utf8mb4 b'01000001' COLLATE utf8mb4_bin)"),vec![vec!["41","41","41","latin1","utf8mb4"]]);
    assert!(engine.execute("SELECT _unknown b'01000001'").is_err());
}

#[test]
fn audit1108_json_integer_conversion_preserves_numeric_domains() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE json_integer_domains(id INT PRIMARY KEY,j JSON,s BIGINT AS(j->'$.a') STORED,v BIGINT AS(j->'$.a') VIRTUAL,KEY(s),KEY(v))",
        r#"INSERT INTO json_integer_domains(id,j) VALUES(1,'{"a":2.5}'),(2,'{"a":-2.5}'),(3,'{"a":3.5}'),(4,'{"a":2.50001}'),(5,'{"a":"3"}'),(6,'{"a":true}'),(7,'{"a":9007199254740993}'),(8,NULL)"#,
        "CREATE TABLE json_integer_copy(id INT PRIMARY KEY,n BIGINT)",
        "INSERT INTO json_integer_copy SELECT id,j->'$.a' FROM json_integer_domains",
        "CREATE TABLE json_integer_unsigned(n BIGINT UNSIGNED)",
        r#"INSERT INTO json_integer_unsigned VALUES(CAST('"18446744073709551615"' AS JSON))"#,
        "INSERT INTO json_integer_unsigned VALUES(CAST('18446744073709551615' AS JSON)),(CAST('-0.5' AS JSON))",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,s,v,CAST(j->'$.a' AS SIGNED) FROM json_integer_domains ORDER BY id"
                ),
                vec![
                    vec!["1", "2", "2", "2"],
                    vec!["2", "-2", "-2", "-2"],
                    vec!["3", "4", "4", "4"],
                    vec!["4", "3", "3", "3"],
                    vec!["5", "3", "3", "3"],
                    vec!["6", "1", "1", "1"],
                    vec![
                        "7",
                        "9007199254740993",
                        "9007199254740993",
                        "9007199254740993"
                    ],
                    vec!["8", "NULL", "NULL", "NULL"]
                ]
            );
            for access in ["IGNORE INDEX(s,v)", "FORCE INDEX(s)", "FORCE INDEX(v)"] {
                for column in ["s", "v"] {
                    assert_eq!(rows(&engine,&format!("SELECT id FROM json_integer_domains {access} WHERE {column}=2 ORDER BY id")),vec![vec!["1"]]);
                }
            }
            assert_eq!(
                rows(&engine, "SELECT id,n FROM json_integer_copy ORDER BY id"),
                vec![
                    vec!["1", "2"],
                    vec!["2", "-2"],
                    vec!["3", "4"],
                    vec!["4", "3"],
                    vec!["5", "3"],
                    vec!["6", "1"],
                    vec!["7", "9007199254740993"],
                    vec!["8", "NULL"]
                ]
            );
            assert_eq!(
                rows(&engine, "SELECT n FROM json_integer_unsigned ORDER BY n"),
                vec![
                    vec!["0"],
                    vec!["18446744073709551615"],
                    vec!["18446744073709551615"]
                ]
            );
            assert_eq!(rows(&engine,"SELECT CAST(JSON_EXTRACT(JSON_ARRAY(CAST(2.5 AS DECIMAL(5,1))),'$[0]') AS SIGNED),CAST(CAST('2.5' AS JSON) AS SIGNED),CAST(CAST('2.5000000000000004' AS JSON) AS SIGNED),CAST(CAST('-3.5' AS JSON) AS SIGNED),CAST(CAST('3.5' AS JSON) AS UNSIGNED)"),vec![vec!["3","2","3","-4","4"]]);
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT CAST(CAST('"18446744073709551615"' AS JSON) AS SIGNED),CAST(CAST('"18446744073709551615"' AS JSON) AS UNSIGNED),CAST(CAST('"-9223372036854775808"' AS JSON) AS SIGNED)"#
                ),
                vec![vec!["-1", "18446744073709551615", "-9223372036854775808"]]
            );
        });
    }
    engine
        .execute(r#"UPDATE json_integer_domains SET j='{"a":-3.5}' WHERE id=1"#)
        .unwrap();
    engine.execute(r#"INSERT INTO json_integer_domains(id,j) VALUES(2,'{"a":4.5}') ON DUPLICATE KEY UPDATE j=VALUES(j)"#).unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id,s,v FROM json_integer_domains WHERE id<=2 ORDER BY id"
        ),
        vec![vec!["1", "-4", "-4"], vec!["2", "4", "4"]]
    );
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            r#"UPDATE json_integer_domains SET j='{"a":2.5}' WHERE id=1"#,
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM json_integer_domains FORCE INDEX(s) WHERE s=-4"
        ),
        vec![vec!["1"]]
    );
}

#[test]
fn audit1108_invalid_json_integer_assignments_are_never_silent() {
    let (_dir, engine) = fixture(&["CREATE TABLE json_integer_invalid(i INT)"]);
    let mut session = SessionContext::default();
    for source in ["null", "[]", "{}", r#""2.5""#, r#""2x""#] {
        let sql = format!(
            "INSERT INTO json_integer_invalid VALUES(7),(CAST('{}' AS JSON))",
            source
        );
        assert_eq!(
            engine
                .execute_with_session(&mut session, &sql)
                .unwrap_err()
                .code(),
            3156,
            "{source}"
        );
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM json_integer_invalid"),
            vec![vec!["0"]]
        );
        let sql = format!("SELECT CAST(CAST('{}' AS JSON) AS SIGNED)", source);
        engine.execute_with_session(&mut session, &sql).unwrap();
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(warnings.rows.len(), 1, "{source}: {:?}", warnings.rows);
        assert_eq!(warnings.rows[0][1].to_display_string(), "3156");
    }
    engine
        .execute_with_session(&mut session, "SET sql_mode=''")
        .unwrap();
    engine.execute_with_session(&mut session,r#"INSERT INTO json_integer_invalid VALUES(CAST('null' AS JSON)),(CAST('"2.5"' AS JSON)),(CAST('[]' AS JSON)),(NULL)"#).unwrap();
    let QueryResponse::ResultSet(warnings) = engine
        .execute_with_session(&mut session, "SHOW WARNINGS")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(warnings.rows.len(), 3);
    assert!(warnings
        .rows
        .iter()
        .all(|row| row[1].to_display_string() == "3156"));
    assert_eq!(
        rows(&engine, "SELECT i FROM json_integer_invalid"),
        vec![vec!["0"], vec!["2"], vec!["0"], vec!["NULL"]]
    );
    engine
        .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT IGNORE INTO json_integer_invalid VALUES(CAST('{}' AS JSON))",
        )
        .unwrap();
    assert_eq!(session.warning_count(), 1);
}

#[test]
fn audit1108_json_integer_generated_indexes_survive_restart() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        engine.execute("CREATE TABLE j21_restart(id INT PRIMARY KEY,j JSON,s INT AS(j->'$.a') STORED,v INT AS(j->'$.a') VIRTUAL,KEY(s),KEY(v))").unwrap();
        engine
            .execute(r#"INSERT INTO j21_restart(id,j) VALUES(1,'{"a":2.5}'),(2,'{"a":3.5}')"#)
            .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    for column in ["s", "v"] {
        assert_eq!(
            rows(
                &engine,
                &format!(
                    "SELECT id,{column} FROM j21_restart FORCE INDEX({column}) WHERE {column}=2"
                )
            ),
            vec![vec!["1", "2"]]
        );
    }
}

#[test]
fn audit1108_json_sql_scalar_sources_and_nulls_are_preserved() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE json_source_types(id INT PRIMARY KEY,y YEAR,b BIT(8),bi BINARY(3),bl BLOB,vb VARBINARY(3),ts TIMESTAMP(2))",
        "INSERT INTO json_source_types VALUES(1,2024,b'101','ab','blob','ab','2020-01-02 03:04:05.12'),(2,NULL,NULL,NULL,NULL,NULL,NULL),(3,0,b'0',X'000102',X'00FF',X'61','0000-00-00 00:00:00')",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    "SELECT JSON_ARRAY(y,b,bi,bl,vb,ts) FROM json_source_types WHERE id=1"
                ),
                vec![vec![
                    r#"[2024, "base64:type16:BQ==", "base64:type254:YWIA", "base64:type252:YmxvYg==", "base64:type15:YWI=", "2020-01-02 03:04:05.120000"]"#
                ]]
            );
            assert_eq!(rows(&engine,"SELECT JSON_TYPE(CAST(y AS JSON)),JSON_TYPE(CAST(b AS JSON)),JSON_TYPE(CAST(ts AS JSON)),CAST(bi AS JSON),CAST(bl AS JSON) FROM json_source_types WHERE id=1"),vec![vec!["UNSIGNED INTEGER","BIT","TIMESTAMP",r#""base64:type254:YWIA""#,r#""base64:type252:YmxvYg==""#]]);
            assert_eq!(rows(&engine,"SELECT CAST(y AS JSON),CAST(b AS JSON),CAST(bi AS JSON),CAST(bl AS JSON),CAST(vb AS JSON),CAST(ts AS JSON),JSON_ARRAY(y,b,bi,bl,vb,ts) FROM json_source_types WHERE id=2"),vec![vec!["NULL","NULL","NULL","NULL","NULL","NULL","[null, null, null, null, null, null]"]]);
            assert_eq!(rows(&engine,"SELECT JSON_ARRAY(y,b),JSON_TYPE(CAST(y AS JSON)),JSON_TYPE(CAST(ts AS JSON)) FROM json_source_types WHERE id=3"),vec![vec![r#"[0, "base64:type16:AA=="]"#,"UNSIGNED INTEGER","TIMESTAMP"]]);
            assert_eq!(rows(&engine,"SELECT JSON_OBJECT('b',b),JSON_SET('{}','$.b',b),JSON_ARRAY(IFNULL(b,b)),JSON_ARRAY(IF(1,b,b)) FROM json_source_types WHERE id=1"),vec![vec![r#"{"b": "base64:type16:BQ=="}"#,r#"{"b": "base64:type16:BQ=="}"#,r#"["base64:type16:BQ=="]"#,r#"["base64:type16:BQ=="]"#]]);
            assert_eq!(rows(&engine,"SELECT JSON_ARRAY(MIN(b)),JSON_ARRAY(MIN(bi)),JSON_ARRAY(MAX(bl)),JSON_ARRAYAGG(b),JSON_ARRAYAGG(bi),JSON_ARRAYAGG(bl),JSON_ARRAYAGG(TRUE) FROM json_source_types WHERE id=1"),vec![vec![r#"["base64:type16:BQ=="]"#,r#"["base64:type254:YWIA"]"#,r#"["base64:type252:YmxvYg=="]"#,r#"["base64:type16:BQ=="]"#,r#"["base64:type254:YWIA"]"#,r#"["base64:type252:YmxvYg=="]"#,"[true]"]]);
            assert_eq!(rows(&engine,"SELECT id,JSON_ARRAYAGG(b) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW),JSON_ARRAYAGG(bi) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM json_source_types WHERE id<3 ORDER BY id"),vec![vec!["1",r#"["base64:type16:BQ=="]"#,r#"["base64:type254:YWIA"]"#],vec!["2","[null]","[null]"]]);
            assert_eq!(rows(&engine,"SELECT JSON_ARRAY(y,b,bi,bl) FROM (SELECT y,b,bi,bl FROM json_source_types WHERE id=1) d"),vec![vec![r#"[2024, "base64:type16:BQ==", "base64:type254:YWIA", "base64:type252:YmxvYg=="]"#]]);
        });
    }
    with_vec_mode(VecMode::On, || {
        engine.execute("CREATE TABLE json_source_copy AS SELECT id,JSON_ARRAY(y,b,bi,bl) j FROM json_source_types").unwrap();
    });
    assert_eq!(
        rows(
            &engine,
            "SELECT JSON_TYPE(j->'$[0]'),JSON_TYPE(j->'$[1]') FROM json_source_copy WHERE id=1"
        ),
        vec![vec!["UNSIGNED INTEGER", "BIT"]]
    );
    assert_eq!(rows(&engine,"SELECT JSON_ARRAY(X'61'),JSON_ARRAY(CAST('a' AS BINARY(3))),JSON_TYPE(CAST(YEAR(DATE'2024-01-01') AS JSON))"),vec![vec![r#"["base64:type15:YQ=="]"#,r#"["base64:type15:YQAA"]"#,"INTEGER"]]);
    engine
        .execute("CREATE TABLE json_text_binary_collation(t TEXT COLLATE utf8mb4_bin)")
        .unwrap();
    engine
        .execute("INSERT INTO json_text_binary_collation VALUES('123'),(NULL)")
        .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT CAST(t AS JSON),JSON_ARRAY(t) FROM json_text_binary_collation"
        ),
        vec![vec!["123", r#"["123"]"#], vec!["NULL", "[null]"]]
    );
}

#[test]
fn audit1108_json_bit_scalar_widths_are_lossless() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE json_bit_widths(a BIT(1),b BIT(9),c BIT(64))",
        "INSERT INTO json_bit_widths VALUES(b'1',b'100000001',18446744073709551615)",
    ]);
    assert_eq!(rows(&engine,"SELECT JSON_ARRAY(a,b,c),JSON_TYPE(CAST(a AS JSON)),JSON_TYPE(CAST(b AS JSON)),JSON_TYPE(CAST(c AS JSON)) FROM json_bit_widths"),vec![vec![r#"["base64:type16:AQ==", "base64:type16:AQE=", "base64:type16://////////8="]"#,"BIT","BIT","BIT"]]);
}

#[test]
fn audit1108_large_fractional_double_strings_keep_short_fixed_form() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE large_fractional_double(id INT PRIMARY KEY,d DOUBLE,KEY(d))",
        "INSERT INTO large_fractional_double VALUES(1,1234567890123456.7e0),(2,-1234567890123456.7e0),(3,4000000000000000.5e0)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for index in ["IGNORE INDEX(d)", "FORCE INDEX(d)"] {
                assert_eq!(rows(&engine,&format!("SELECT CONCAT(d),CAST(d AS CHAR),JSON_ARRAY(d) FROM large_fractional_double {index} ORDER BY id")),vec![vec!["1234567890123456.8","1234567890123456.8","[1234567890123456.8]"],vec!["-1234567890123456.8","-1234567890123456.8","[-1234567890123456.8]"],vec!["4000000000000000.5","4000000000000000.5","[4000000000000000.5]"]]);
            }
            assert_eq!(rows(&engine,"SELECT CONCAT(1e15),CONCAT(1.5e15),CONCAT(1000000000000001e0),CONCAT(1e16),CONCAT(1e-15),CONCAT(1e-16),CONCAT(-0.0e0)"),vec![vec!["1e15","1.5e15","1.000000000000001e15","1e16","0.000000000000001","1e-16","-0"]]);
            assert_eq!(
                rows(
                    &engine,
                    "SELECT CAST('4000000000000000.5' AS JSON),CAST('999999999999999.9' AS JSON)"
                ),
                vec![vec!["4000000000000000.5", "999999999999999.9"]]
            );
        });
    }
    for value in [
        1234567890123456.75_f64,
        -1234567890123456.75,
        4000000000000000.5,
        999999999999999.9,
    ] {
        let text = Value::Double(value).to_display_string();
        assert_eq!(text.parse::<f64>().unwrap().to_bits(), value.to_bits());
    }
}

#[test]
fn audit1108_json_path_consumers_choose_auto_wrap() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_KEYS('{"a":{"x":1}}','$[0].a'),JSON_KEYS('{"a":{"x":1}}','$.a[0]'),JSON_KEYS('{"a":1}','$[last]')"#
                ),
                vec![vec!["NULL", "NULL", "NULL"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_SEARCH('{"a":"x"}','one','x',NULL,'$[0]'),JSON_SEARCH('{"a":"x"}','all','x',NULL,'$.a[0]'),JSON_SEARCH('"x"','one','x',NULL,'$[0]')"#
                ),
                vec![vec!["NULL", "NULL", "NULL"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_ARRAY_INSERT('{"a":[1]}','$[0].a[0]',9),JSON_ARRAY_INSERT('{"a":[1]}','$.a[0]',9),JSON_KEYS('[{"x":1}]','$[0]')"#
                ),
                vec![vec![r#"{"a": [1]}"#, r#"{"a": [9, 1]}"#, r#"["x"]"#]]
            );
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_EXTRACT('{"a":1}','$[0].a'),JSON_LENGTH('{"a":1}','$[0]'),JSON_CONTAINS('{"a":1}','1','$[0].a'),JSON_CONTAINS_PATH('{"a":1}','one','$[0].a'),JSON_SET('{"a":1}','$[0].a',2),JSON_REMOVE('{"a":1}','$[0].a')"#
                ),
                vec![vec!["1", "1", "1", "1", r#"{"a": 2}"#, "{}"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_SEARCH('"x"','all','x',NULL,'$[0 to 0]'),JSON_SEARCH('["x"]','all','x',NULL,'$[0 to 0]')"#
                ),
                vec![vec!["NULL", r#""$[0]""#]]
            );
            assert_eq!(
                engine
                    .execute(r#"SELECT JSON_KEYS('{"a":1}','$[*]')"#)
                    .and_then(QueryResponse::materialize)
                    .unwrap_err()
                    .code(),
                3149
            );
        });
    }
}

#[test]
fn audit1108_json_search_respects_scope_priority_without_duplicates() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_SEARCH('{"a":"x","b":{"c":"x"}}','one','x',NULL,'$.b','$.a')"#
                ),
                vec![vec![r#""$.b.c""#]]
            );
            for scopes in [
                "'$[2]','$[0]'",
                "'$[2]','$','$[0]'",
                "'$[2]','$[2]','$','$'",
            ] {
                assert_eq!(
                    rows(
                        &engine,
                        &format!(
                            r#"SELECT JSON_SEARCH('["x","y",["x"]]','all','x',NULL,{scopes})"#
                        )
                    ),
                    vec![vec![r#"["$[2][0]", "$[0]"]"#]]
                );
            }
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_SEARCH('{"a":"x","b":{"c":"x"}}','one','x',NULL,'$.missing','$.b','$.a')"#
                ),
                vec![vec![r#""$.b.c""#]]
            );
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_SEARCH('{"x":{"k":"x"},"a":"x"}','all','x',NULL,'$.x.*','$**.k','$')"#
                ),
                vec![vec![r#"["$.x.k", "$.a"]"#]]
            );
            assert_eq!(
                rows(
                    &engine,
                    r#"SELECT JSON_SEARCH('{"a":"x"}','one','x',NULL,'$.a',NULL)"#
                ),
                vec![vec!["NULL"]]
            );
            assert_eq!(
                engine
                    .execute(r#"SELECT JSON_SEARCH('{"a":"x"}','one','x',NULL,'$.a','bad')"#)
                    .and_then(QueryResponse::materialize)
                    .unwrap_err()
                    .code(),
                3143
            );
        });
    }
}

#[test]
fn audit1108_json_text_integer_tags_preserve_sql_numeric_provenance() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    let engine = StorageQueryEngine::open_with_storage_config_without_background_workers(
        dir.path(),
        config.clone(),
    )
    .unwrap();
    for sql in [
        "CREATE TABLE json_integer_tags(id INT PRIMARY KEY,j JSON,u BIGINT UNSIGNED GENERATED ALWAYS AS(JSON_EXTRACT(j,'$.n')) STORED,KEY(u))",
        r#"INSERT INTO json_integer_tags(id,j) VALUES(1,'{"n":4294967295}'),(2,'{"n":4294967296}'),(3,'{"n":9223372036854775807}'),(4,'{"n":18446744073709551615}')"#,
    ] { engine.execute(sql).unwrap(); }
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT JSON_TYPE('4294967295'),JSON_TYPE('4294967296'),JSON_TYPE('9223372036854775807'),JSON_TYPE('18446744073709551615'),JSON_TYPE('-9223372036854775808')"),vec![vec!["INTEGER","UNSIGNED INTEGER","UNSIGNED INTEGER","UNSIGNED INTEGER","INTEGER"]]);
            assert_eq!(rows(&engine,"SELECT JSON_TYPE(CAST(5000000000 AS JSON)),JSON_TYPE(JSON_EXTRACT(JSON_ARRAY(5000000000),'$[0]')),JSON_TYPE(CAST(CAST(5000000000 AS UNSIGNED) AS JSON))"),vec![vec!["INTEGER","INTEGER","UNSIGNED INTEGER"]]);
            assert_eq!(rows(&engine,"SELECT JSON_TYPE(JSON_EXTRACT('[5000000000]','$[0]')),JSON_CONTAINS('[5000000000]',CAST(5000000000 AS JSON)),JSON_EXTRACT('[5000000000]','$[0]')=CAST(5000000000 AS JSON)"),vec![vec!["UNSIGNED INTEGER","1","1"]]);
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id,JSON_TYPE(j->'$.n') FROM json_integer_tags ORDER BY id"
                ),
                vec![
                    vec!["1", "INTEGER"],
                    vec!["2", "UNSIGNED INTEGER"],
                    vec!["3", "UNSIGNED INTEGER"],
                    vec!["4", "UNSIGNED INTEGER"]
                ]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT id FROM json_integer_tags FORCE INDEX(u) WHERE u=18446744073709551615"
                ),
                vec![vec!["4"]]
            );
        });
    }
    with_vec_mode(VecMode::On, || {
        engine.execute("CREATE TABLE native_unsigned_json AS SELECT id,JSON_ARRAY(u) a,JSON_OBJECT('n',u) o FROM json_integer_tags").unwrap();
    });
    assert_eq!(
        rows(
            &engine,
            "SELECT JSON_TYPE(a->'$[0]'),JSON_TYPE(o->'$.n') FROM native_unsigned_json ORDER BY id"
        ),
        vec![vec!["UNSIGNED INTEGER", "UNSIGNED INTEGER"]; 4]
    );
    drop(engine);
    let engine =
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap();
    assert_eq!(
        rows(
            &engine,
            "SELECT JSON_TYPE(j->'$.n') FROM json_integer_tags WHERE id=3"
        ),
        vec![vec!["UNSIGNED INTEGER"]]
    );
}

#[test]
fn audit1108_temporal_generated_strings_preserve_fsp() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir,engine)=fixture(&[
        "CREATE TABLE generated_temporal_text(id INT PRIMARY KEY,dt DATETIME(3),tm TIME(4),s VARCHAR(40) GENERATED ALWAYS AS(dt) STORED,v VARCHAR(40) GENERATED ALWAYS AS(tm) VIRTUAL,KEY(s),KEY(v))",
        "INSERT INTO generated_temporal_text(id,dt,tm) VALUES(1,'2024-01-01 00:00:00','-10:00:00.1000'),(2,NULL,NULL)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            for index in ["IGNORE INDEX(s,v)", "FORCE INDEX(s)", "FORCE INDEX(v)"] {
                assert_eq!(
                    rows(
                        &engine,
                        &format!("SELECT s,v FROM generated_temporal_text {index} WHERE id=1")
                    ),
                    vec![vec!["2024-01-01 00:00:00.000", "-10:00:00.1000"]]
                );
            }
            assert_eq!(
                rows(
                    &engine,
                    "SELECT s,v FROM generated_temporal_text WHERE id=2"
                ),
                vec![vec!["NULL", "NULL"]]
            );
        });
    }
    engine.execute("UPDATE generated_temporal_text SET dt='2024-01-01 00:00:00.120',tm='10:00:00' WHERE id=1").unwrap();
    assert_eq!(rows(&engine,"SELECT s,v FROM generated_temporal_text FORCE INDEX(s) WHERE s='2024-01-01 00:00:00.120'"),vec![vec!["2024-01-01 00:00:00.120","10:00:00.0000"]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT s,v FROM generated_temporal_text FORCE INDEX(v) WHERE v='10:00:00.0000'"
        ),
        vec![vec!["2024-01-01 00:00:00.120", "10:00:00.0000"]]
    );
}

#[test]
fn audit1108_temporal_string_context_preserves_declared_fsp() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[]);
    for fsp in 0..=6 {
        engine.execute(&format!("CREATE TABLE temporal_text_{fsp}(id INT PRIMARY KEY,dt DATETIME({fsp}),tm TIME({fsp}))")).unwrap();
        engine.execute(&format!("INSERT INTO temporal_text_{fsp} VALUES(1,'2024-01-01 00:00:00','10:00:00'),(2,NULL,NULL)")).unwrap();
        let fraction = if fsp == 0 {
            String::new()
        } else {
            format!(".{}", "0".repeat(fsp))
        };
        let dt = format!("2024-01-01 00:00:00{fraction}");
        let tm = format!("10:00:00{fraction}");
        for mode in [VecMode::Off, VecMode::On] {
            with_vec_mode(mode, || {
                let sql=format!("SELECT CONCAT(dt),CAST(dt AS CHAR),CONCAT(tm),CAST(tm AS CHAR),LENGTH(dt),SUBSTRING(dt,20),RIGHT(tm,{n}),dt LIKE '%{fraction}',CONCAT(IFNULL(dt,dt)) FROM temporal_text_{fsp} WHERE id=1",n=fraction.len());
                assert_eq!(
                    rows(&engine, &sql),
                    vec![vec![
                        dt.clone(),
                        dt.clone(),
                        tm.clone(),
                        tm.clone(),
                        dt.len().to_string(),
                        fraction.clone(),
                        fraction.clone(),
                        "1".into(),
                        dt.clone()
                    ]],
                    "{mode:?} fsp={fsp}"
                );
                assert_eq!(rows(&engine,&format!("SELECT GROUP_CONCAT(dt ORDER BY id),GROUP_CONCAT(tm),CONCAT(MIN(dt)),JSON_OBJECTAGG(dt,id) FROM temporal_text_{fsp} WHERE dt IS NOT NULL")),vec![vec![dt.clone(),tm.clone(),dt.clone(),format!(r#"{{"{dt}": 1}}"#)]],"{mode:?} fsp={fsp}");
                assert_eq!(rows(&engine,&format!("SELECT CONCAT(CAST('2024-01-01 00:00:00' AS DATETIME({fsp}))),GROUP_CONCAT(CAST('10:00:00' AS TIME({fsp})))")),vec![vec![dt.clone(),tm.clone()]]);
                assert_eq!(
                    rows(
                        &engine,
                        "SELECT SUBSTRING('abcd',TIME'00:00:02'),REPEAT('a',TIME'00:00:02')"
                    ),
                    vec![vec!["bcd", "aa"]]
                );
                assert_eq!(
                    rows(
                        &engine,
                        &format!(
                            "SELECT CONCAT(dt),CONCAT(tm) FROM temporal_text_{fsp} WHERE id=2"
                        )
                    ),
                    vec![vec!["NULL", "NULL"]]
                );
                assert_eq!(
                    rows(
                        &engine,
                        &format!(
                            "SELECT dt FROM temporal_text_{fsp} WHERE id=1 UNION ALL SELECT 'x'"
                        )
                    ),
                    vec![vec![dt.clone()], vec!["x".into()]]
                );
            });
        }
        engine
            .execute(&format!(
                "CREATE TABLE temporal_text_sink_{fsp}(d VARCHAR(40),t VARCHAR(30))"
            ))
            .unwrap();
        engine.execute(&format!("INSERT INTO temporal_text_sink_{fsp} SELECT dt,tm FROM temporal_text_{fsp} WHERE id=1")).unwrap();
        assert_eq!(
            rows(&engine, &format!("SELECT * FROM temporal_text_sink_{fsp}")),
            vec![vec![dt.clone(), tm.clone()]]
        );
        engine.execute(&format!("UPDATE temporal_text_sink_{fsp} SET d=CAST('2024-01-01 00:00:00' AS DATETIME({fsp})),t=CAST('10:00:00' AS TIME({fsp}))")).unwrap();
        assert_eq!(
            rows(&engine, &format!("SELECT * FROM temporal_text_sink_{fsp}")),
            vec![vec![dt.clone(), tm.clone()]]
        );
        engine.execute(&format!("UPDATE temporal_text_sink_{fsp} s JOIN temporal_text_{fsp} t ON t.id=1 SET s.d=t.dt,s.t=t.tm")).unwrap();
        assert_eq!(
            rows(&engine, &format!("SELECT * FROM temporal_text_sink_{fsp}")),
            vec![vec![dt.clone(), tm.clone()]]
        );
        engine.execute(&format!("CREATE TABLE temporal_text_ctas_{fsp} AS SELECT CONCAT(dt) d,CAST(tm AS CHAR) t FROM temporal_text_{fsp} WHERE id=1")).unwrap();
        engine.execute(&format!("CREATE TABLE temporal_union_ctas_{fsp} AS SELECT dt d FROM temporal_text_{fsp} WHERE id=1 UNION ALL SELECT 'x'")).unwrap();
        assert_eq!(
            rows(&engine, &format!("SELECT * FROM temporal_union_ctas_{fsp}")),
            vec![vec![dt.clone()], vec!["x".into()]]
        );
        assert_eq!(
            rows(&engine, &format!("SELECT * FROM temporal_text_ctas_{fsp}")),
            vec![vec![dt, tm]]
        );
    }
}

#[test]
fn audit1108_null_selection_uses_bound_domain_inside_functions() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE null_domain(id INT PRIMARY KEY, i INT, d DATE, tm TIME)",
        "INSERT INTO null_domain VALUES(1,5,'2020-01-01','10:00:00'),(2,NULL,NULL,NULL)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT LENGTH(IFNULL(5,1.25)),CONCAT(IFNULL(5,1.5)),CONCAT(COALESCE(NULL,5,1.5)),IFNULL(DATE'2020-01-01',0)+0,IFNULL(TIME'10:00:00',0)+0"), vec![vec!["4","5.0","5.0","2020","10"]]);
            assert_eq!(rows(&engine,"SELECT id,LENGTH(IFNULL(i,1.25)),CONCAT(COALESCE(i,1.5)),IFNULL(d,0)+0,IFNULL(tm,0)+0 FROM null_domain ORDER BY id"), vec![vec!["1","4","5.0","2020","10"],vec!["2","4","1.5","0","0"]]);
            assert_eq!(rows(&engine,"SELECT id,LENGTH(IFNULL(i,1.25)),IFNULL(d,0)+0,COUNT(*) FROM null_domain GROUP BY id ORDER BY id"), vec![vec!["1","4","2020","1"],vec!["2","4","0","1"]]);
            assert_eq!(
                rows(
                    &engine,
                    "SELECT IFNULL(1,1/0),COALESCE(NULL,1,1/0),(IFNULL(2/3,0)*100)"
                ),
                vec![vec!["1.0000", "1.0000", "66.6667"]]
            );
        });
    }
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET @v=5")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT CONCAT(IFNULL(@v,1.5)),LENGTH(COALESCE(NULL,@v,1.25))",
        )
        .and_then(QueryResponse::materialize)
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        result.rows[0]
            .iter()
            .map(Value::to_display_string)
            .collect::<Vec<_>>(),
        vec!["5.0", "4"]
    );
    assert_eq!(session.warning_count(), 0);
}

#[test]
fn audit1108_json_objectagg_string_keys_and_empty_frames() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE object_keys(id INT PRIMARY KEY,j JSON)",
        "INSERT INTO object_keys VALUES(1,'\"1\"'),(2,'10'),(3,'true')",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(
                rows(
                    &engine,
                    "SELECT JSON_OBJECTAGG(TRUE,1),JSON_OBJECTAGG(FALSE,2)"
                ),
                vec![vec![r#"{"1": 1}"#, r#"{"0": 2}"#]]
            );
            assert_eq!(
                rows(&engine, "SELECT JSON_OBJECTAGG(j,id) FROM object_keys"),
                vec![vec![r#"{"10": 2, "\"1\"": 1, "true": 3}"#]]
            );
            assert_eq!(rows(&engine,"SELECT id,JSON_OBJECTAGG(j,id) OVER(ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM object_keys ORDER BY id"),vec![vec!["1",r#"{"\"1\"": 1}"#],vec!["2",r#"{"10": 2}"#],vec!["3",r#"{"true": 3}"#]]);
            assert_eq!(rows(&engine,"SELECT id,JSON_OBJECTAGG(id,j) OVER(ORDER BY id ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) FROM object_keys ORDER BY id"),vec![vec!["1",r#"{"3": true}"#],vec!["2","NULL"],vec!["3","NULL"]]);
            assert_eq!(
                engine
                    .execute("SELECT JSON_OBJECTAGG(NULL,1)")
                    .and_then(QueryResponse::materialize)
                    .unwrap_err()
                    .code(),
                3158
            );
            assert_eq!(engine.execute("SELECT JSON_OBJECTAGG(NULL,1) OVER(ROWS BETWEEN CURRENT ROW AND CURRENT ROW)").and_then(QueryResponse::materialize).unwrap_err().code(),3158);
            assert_eq!(
                rows(
                    &engine,
                    "SELECT JSON_OBJECTAGG(NULL,1) OVER(ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)"
                ),
                vec![vec!["NULL"]]
            );
        });
    }
}

#[test]
fn audit1108_recursive_cte_enforces_anchor_character_width() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            let mut session = SessionContext::default();
            for anchor in ["'a'", "'中'", "_latin1'a'"] {
                let sql = format!("WITH RECURSIVE c(n,s) AS (SELECT 1,{anchor} UNION ALL SELECT n+1,CONCAT(s,'b') FROM c WHERE n<3) SELECT n,s FROM c");
                engine
                    .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
                    .unwrap();
                assert_eq!(
                    engine
                        .execute_with_session(&mut session, &sql)
                        .and_then(QueryResponse::materialize)
                        .unwrap_err()
                        .code(),
                    1406,
                    "{mode:?}: {sql}"
                );
                engine
                    .execute_with_session(&mut session, "SET sql_mode=''")
                    .unwrap();
                let QueryResponse::ResultSet(result) = engine
                    .execute_with_session(&mut session, &sql)
                    .and_then(QueryResponse::materialize)
                    .unwrap()
                else {
                    panic!()
                };
                assert_eq!(
                    result
                        .rows
                        .iter()
                        .map(|r| r[1].to_display_string())
                        .collect::<Vec<_>>(),
                    vec![if anchor == "'中'" { "中" } else { "a" }; 3]
                );
                assert!(session.warning_count() > 0);
                assert!(session.warnings.iter().all(|w| w.code() == 1265));
            }
            engine
                .execute_with_session(&mut session, "SET sql_mode=''")
                .unwrap();
            engine.execute_with_session(&mut session,"WITH RECURSIVE c(n,d) AS (SELECT 1,CAST(1 AS DECIMAL(4,1)) UNION ALL SELECT n+1,10000.04e0 FROM c WHERE n<3) SELECT * FROM c").and_then(QueryResponse::materialize).unwrap();
            assert_eq!(session.warning_count(), 2);
            assert!(session.warnings.iter().all(|w| w.code() == 1264));
            assert_eq!(rows(&engine,"WITH RECURSIVE c(n,s) AS (SELECT 1,CAST('a' AS CHAR(8)) UNION ALL SELECT n+1,CONCAT(s,'b') FROM c WHERE n<3) SELECT n,s FROM c"),vec![vec!["1","a"],vec!["2","ab"],vec!["3","abb"]]);
            engine
                .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
                .unwrap();
            for anchor in ["X'61'", "_binary'a'", "_binary'中'"] {
                let binary=format!("WITH RECURSIVE c(n,s) AS (SELECT 1,{anchor} UNION ALL SELECT n+1,CONCAT(s,X'62') FROM c WHERE n<3) SELECT HEX(s) FROM c");
                assert_eq!(
                    engine
                        .execute_with_session(&mut session, &binary)
                        .and_then(QueryResponse::materialize)
                        .unwrap_err()
                        .code(),
                    1406,
                    "{anchor}"
                );
            }
            for value in ["'a'", "1"] {
                let sql=format!("WITH RECURSIVE c(n,s) AS (SELECT 1,NULL UNION ALL SELECT n+1,{value} FROM c WHERE n<3) SELECT * FROM c");
                assert_eq!(
                    engine
                        .execute_with_session(&mut session, &sql)
                        .and_then(QueryResponse::materialize)
                        .unwrap_err()
                        .code(),
                    1406,
                    "NULL anchor with {value}"
                );
            }
            assert_eq!(rows(&engine,"WITH RECURSIVE c(n,s) AS (SELECT 1,NULL UNION ALL SELECT n+1,NULL FROM c WHERE n<3) SELECT * FROM c"),vec![vec!["1","NULL"],vec!["2","NULL"],vec!["3","NULL"]]);
            assert_eq!(engine.execute_with_session(&mut session,"CREATE TABLE recursive_width_ctas AS WITH RECURSIVE c(n,s) AS (SELECT 1,'a' UNION ALL SELECT n+1,CONCAT(s,'b') FROM c WHERE n<3) SELECT * FROM c").unwrap_err().code(),1406);
            engine
                .execute_with_session(&mut session, "DROP TABLE IF EXISTS recursive_width_ctas")
                .unwrap();
            for (input, warnings) in [("d+0.04", 2), ("'1.04'", 2), ("1.04e0", 2), ("d+0.00", 0)] {
                let sql=format!("WITH RECURSIVE c(n,d) AS (SELECT 1,CAST(1 AS DECIMAL(4,1)) UNION ALL SELECT n+1,{input} FROM c WHERE n<3) SELECT * FROM c");
                let QueryResponse::ResultSet(result) = engine
                    .execute_with_session(&mut session, &sql)
                    .and_then(QueryResponse::materialize)
                    .unwrap()
                else {
                    panic!()
                };
                assert!(result
                    .rows
                    .iter()
                    .all(|row| row[1].to_display_string() == "1.0"));
                assert_eq!(session.warning_count(), warnings, "{input}");
                assert!(session.warnings.iter().all(|w| w.code() == 1265));
            }
        });
    }
}

#[test]
fn audit1108_recursive_cte_rejects_null_producing_outer_join_side() {
    let (_dir, engine) = fixture(&[]);
    for recursive in [
        "SELECT t.x FROM (SELECT 2 x UNION ALL SELECT 3) t LEFT JOIN c ON c.n=t.x WHERE c.n IS NULL",
        "SELECT t.x FROM c RIGHT JOIN (SELECT 2 x UNION ALL SELECT 3) t ON c.n=t.x WHERE c.n IS NULL",
        "SELECT t.x FROM c JOIN (SELECT 2 x) a ON c.n=a.x RIGHT JOIN (SELECT 3 x) t ON a.x=t.x",
        "SELECT t.x FROM c FULL JOIN (SELECT 2 x) t ON c.n=t.x",
    ] {
        let sql=format!("WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL {recursive}) SELECT * FROM c");
        assert_eq!(engine.execute(&sql).unwrap_err().code(),3576,"{sql}");
    }
    for recursive in [
        "SELECT c.n+1 FROM c LEFT JOIN (SELECT 1 x) t ON c.n=t.x WHERE c.n<3",
        "SELECT c.n+1 FROM (SELECT 1 x) t RIGHT JOIN c ON c.n=t.x WHERE c.n<3",
    ] {
        let sql = format!(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL {recursive}) SELECT * FROM c ORDER BY n"
        );
        assert_eq!(rows(&engine, &sql), vec![vec!["1"], vec!["2"], vec!["3"]]);
    }
}

#[test]
fn audit1108_unsigned_assignment_checks_exact_sign_before_rounding() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    for kind in [
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "INT",
        "BIGINT",
        "DECIMAL(5,2)",
        "DECIMAL(35,2)",
        "DECIMAL(65,2)",
    ] {
        engine
            .execute_with_session(&mut session, "DROP TABLE IF EXISTS unsigned_value")
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                &format!("CREATE TABLE unsigned_value(v {kind} UNSIGNED)"),
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "SET sql_mode='STRICT_TRANS_TABLES'")
            .unwrap();
        let mut negative_values = vec![
            "-0.001",
            "CAST(-0.001 AS DECIMAL(35,30))",
            "CAST(-0.001 AS DECIMAL(65,30))",
        ];
        if kind.starts_with("DECIMAL") {
            negative_values.push("'-0.001'");
        }
        for value in negative_values {
            let sql = format!("INSERT INTO unsigned_value VALUES({value})");
            assert_eq!(
                engine
                    .execute_with_session(&mut session, &sql)
                    .unwrap_err()
                    .code(),
                1264,
                "{kind}: {sql}"
            );
        }
        for value in ["-0.0", "'-0.000'", "-0", "0.001"] {
            engine
                .execute_with_session(
                    &mut session,
                    &format!("INSERT INTO unsigned_value VALUES({value})"),
                )
                .unwrap();
        }
        if !kind.starts_with("DECIMAL") {
            engine
                .execute_with_session(
                    &mut session,
                    "INSERT INTO unsigned_value VALUES(-0.4e0),('-0.001'),(CAST('-0.4' AS JSON))",
                )
                .unwrap();
        }
        engine
            .execute_with_session(&mut session, "SET sql_mode=''")
            .unwrap();
        engine
            .execute_with_session(&mut session, "INSERT INTO unsigned_value VALUES(-0.001)")
            .unwrap();
        assert_eq!(session.warning_count(), 1, "{kind}");
        assert_eq!(session.warnings[0].code(), 1264, "{kind}");
        let QueryResponse::ResultSet(result) = engine
            .execute_with_session(&mut session, "SELECT v FROM unsigned_value")
            .and_then(QueryResponse::materialize)
            .unwrap()
        else {
            panic!()
        };
        assert!(
            result
                .rows
                .iter()
                .all(|row| crate::engine::expr::numeric(&row[0]).unwrap() == 0.0),
            "{kind}: {:?}",
            result.rows
        );
    }
}

#[test]
fn audit1108_time_component_functions_preserve_extract_signed_semantics() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE components(id INT,t TIME(6),s VARCHAR(30))",
        "INSERT INTO components VALUES(1,'-12:34:56.123456','-12:34:56.123456'),(2,'-838:59:59.000000','-838:59:59.000000'),(3,NULL,NULL)",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT HOUR(TIME'-12:34:56.123456'),MINUTE(TIME'-12:34:56.123456'),SECOND(TIME'-12:34:56.123456'),MICROSECOND(TIME'-12:34:56.123456'),EXTRACT(HOUR FROM TIME'-12:34:56.123456')"),vec![vec!["12","34","56","123456","-12"]]);
            for column in ["t", "s"] {
                assert_eq!(rows(&engine,&format!("SELECT HOUR({column}),MINUTE({column}),SECOND({column}),EXTRACT(HOUR FROM {column}),EXTRACT(MINUTE FROM {column}),EXTRACT(SECOND FROM {column}) FROM components ORDER BY id")),vec![vec!["12","34","56","-12","-34","-56"],vec!["838","59","59","-838","-59","-59"],vec!["NULL","NULL","NULL","NULL","NULL","NULL"]]);
            }
        });
    }
}

#[test]
fn audit1108_temporal_sum_avg_keep_exact_numbers_and_declared_scale() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    let (_dir, engine) = fixture(&[
        "CREATE TABLE temporal_aggs(id INT,d DATE,dt DATETIME(6),tm TIME(4),y YEAR)",
        "INSERT INTO temporal_aggs VALUES(1,'2020-01-01','2020-01-01 00:00:00.123456','10:00:00.1200',2009),(2,'2020-01-02','2020-01-02 00:00:00.654321','-12:59:59.9900',2010),(3,NULL,NULL,NULL,NULL)",
        "CREATE TABLE fractional_guard(id INT,t TIME(6))",
        "INSERT INTO fractional_guard VALUES(1,'00:00:01'),(2,'00:00:00'),(3,'00:00:00')",
    ]);
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            let sql = "SELECT SUM(d),AVG(d),SUM(dt),AVG(dt),SUM(tm),AVG(tm),SUM(y),AVG(y) FROM temporal_aggs";
            assert_eq!(
                rows(&engine, sql),
                vec![vec![
                    "40400203",
                    "20200101.5000",
                    "40400203000000.777777",
                    "20200101500000.3888885000",
                    "-25959.8700",
                    "-12979.93500000",
                    "4019",
                    "2009.5000"
                ]],
                "{mode:?}"
            );
            let QueryResponse::ResultSet(result) = engine
                .execute(sql)
                .and_then(QueryResponse::materialize)
                .unwrap()
            else {
                panic!()
            };
            assert!(
                result
                    .columns
                    .iter()
                    .all(|c| matches!(c.column_type, ColumnType::Decimal { .. })),
                "{:?}",
                result.columns
            );
            assert_eq!(rows(&engine,"SELECT CONCAT(SUM(tm)),CONCAT(AVG(tm)),SUM(DISTINCT dt),AVG(DISTINCT y),AVG(NULLIF(y,10)) FROM temporal_aggs"),vec![vec!["-25959.8700","-12979.93500000","40400203000000.777777","2009.5000","2009.5000"]],"{mode:?}");
            assert_eq!(
                rows(
                    &engine,
                    "SELECT s,a FROM (SELECT SUM(tm) s,AVG(dt) a FROM temporal_aggs) q"
                ),
                vec![vec!["-25959.8700", "20200101500000.3888885000"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT SUM(tm),AVG(dt),AVG(y) FROM temporal_aggs WHERE id<0"
                ),
                vec![vec!["NULL", "NULL", "NULL"]]
            );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT SUM(TIME'10:00:00.1200'),AVG(TIME'10:00:00')"
                ),
                vec![vec!["100000.1200", "100000.0000"]]
            );
            assert_eq!(rows(&engine,"SELECT id,SUM(tm) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING),AVG(dt) OVER(),AVG(y) OVER() FROM temporal_aggs ORDER BY id"),vec![vec!["1","100000.1200","20200101500000.3888885000","2009.5000"],vec!["2","-25959.8700","20200101500000.3888885000","2009.5000"],vec!["3","-25959.8700","20200101500000.3888885000","2009.5000"]],"{mode:?}");
            assert_eq!(rows(&engine,"SELECT id,SUM(dt) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),AVG(tm) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM temporal_aggs ORDER BY id"),vec![vec!["1","20200101000000.123456","100000.12000000"],vec!["2","40400203000000.777777","-12979.93500000"],vec!["3","20200102000000.654321","-125959.99000000"]],"{mode:?}");
            assert_eq!(
                rows(&engine, "SELECT SUM(t),AVG(t) FROM fractional_guard"),
                vec![vec!["1.000000", "0.3333333333"]]
            );
            assert_eq!(rows(&engine,"SELECT AVG(t) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) FROM fractional_guard ORDER BY id"),vec![vec!["1.0000000000"],vec!["0.5000000000"],vec!["0.3333333333"]]);
            assert_eq!(rows(&engine,"SELECT id%2,SUM(tm),AVG(dt),GROUP_CONCAT(id ORDER BY id) FROM temporal_aggs GROUP BY id%2 ORDER BY 1"),vec![vec!["0","-125959.9900","20200102000000.6543210000","2"],vec!["1","100000.1200","20200101000000.1234560000","1,3"]]);
        });
    }
}

#[test]
fn audit1108_call_reports_only_final_statement_status() {
    use super::support::execute_materialized as exec;
    let (_dir, engine) = fixture(&[
        "CREATE TABLE fr(id INT PRIMARY KEY,v INT)",
        "INSERT INTO fr VALUES(1,1),(2,2),(3,3)",
        "CREATE PROCEDURE fr_delete() BEGIN UPDATE fr SET v=v WHERE id<=2; DELETE FROM fr WHERE id=999; END",
        "CREATE PROCEDURE fr_into() BEGIN UPDATE fr SET v=v; SELECT 1 INTO @x; END",
        "CREATE PROCEDURE fr_update() BEGIN UPDATE fr SET v=v; END",
        "CREATE PROCEDURE fr_set() BEGIN UPDATE fr SET v=v; SET @x=1; END",
        "CREATE PROCEDURE fr_select() SELECT 1",
        "CREATE PROCEDURE fr_empty() BEGIN END",
        "CREATE PROCEDURE fr_insert() INSERT INTO fr VALUES(4,4),(5,5) ON DUPLICATE KEY UPDATE v=VALUES(v)+1",
        "CREATE PROCEDURE fr_nested() CALL fr_into()",
    ]);
    for found_rows in [false, true] {
        let mut session = SessionContext::default();
        session.client_found_rows = found_rows;
        for (name, affected) in [
            ("fr_delete", 0),
            ("fr_into", 1),
            ("fr_update", if found_rows { 3 } else { 0 }),
            ("fr_set", 0),
            ("fr_select", 0),
            ("fr_empty", 0),
            ("fr_nested", 1),
            ("fr_insert", 2),
            ("fr_insert", 4),
        ] {
            let response = exec(&engine, &mut session, &format!("CALL {name}()")).unwrap();
            let QueryResponse::MultiResult(results) = response else {
                panic!("expected CALL response");
            };
            assert!(
                matches!(results.last(), Some(QueryResponse::OkAffected { affected_rows }) if *affected_rows==affected),
                "{name}: {results:?}"
            );
            assert_eq!(session.last_row_count, affected as i64, "{name}");
            assert_eq!(session.pending_statement_matched_rows, None, "{name}");
            assert_eq!(session.pending_statement_info, None, "{name}");
        }
        exec(&engine, &mut session, "DELETE FROM fr WHERE id>=4").unwrap();
    }
}

#[test]
fn audit1108_odku_binds_bare_alias_columns_and_rejects_ambiguity() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE odku(id INT PRIMARY KEY,u INT UNIQUE,c INT DEFAULT 0)",
        "INSERT INTO odku VALUES(1,10,0)",
    ]);
    for statement in [
        "INSERT INTO odku(id,u,c) VALUES(1,10,9) AS nw(i,uu,cc) ON DUPLICATE KEY UPDATE c=cc+2",
        "INSERT INTO odku SET id=1,u=10,c=9 AS nw(i,uu,cc) ON DUPLICATE KEY UPDATE c=cc+2",
        "INSERT INTO odku(c,id,u) VALUES(9,1,10) AS nw(cc,i,uu) ON DUPLICATE KEY UPDATE c=nw.cc+2",
        "INSERT INTO odku VALUES(1,10,9) AS nw(i,uu,cc) ON DUPLICATE KEY UPDATE c=odku.c+cc-9",
    ] {
        engine.execute(statement).unwrap();
        assert_eq!(rows(&engine, "SELECT c FROM odku"), vec![vec!["11"]]);
    }
    for statement in [
        "INSERT INTO odku VALUES(1,10,9) AS nw(id,u,c) ON DUPLICATE KEY UPDATE c=c+1",
        "INSERT INTO odku VALUES(1,10,9) AS nw ON DUPLICATE KEY UPDATE c=c+1",
        "INSERT INTO odku VALUES(1,10,9) AS nw(id,uu,cc) ON DUPLICATE KEY UPDATE c=id+1",
    ] {
        assert_eq!(
            engine.execute(statement).unwrap_err().code(),
            1052,
            "{statement}"
        );
        assert_eq!(rows(&engine, "SELECT c FROM odku"), vec![vec!["11"]]);
    }
    engine
        .execute("INSERT INTO odku VALUES(1,10,9) AS nw ON DUPLICATE KEY UPDATE c=nw.c+odku.c")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT c FROM odku"), vec![vec!["20"]]);
}

fn fixture(sql: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    for statement in sql {
        engine
            .execute(statement)
            .unwrap_or_else(|e| panic!("{statement}: {e:?}"));
    }
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .and_then(QueryResponse::materialize)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    else {
        panic!("{sql}")
    };
    result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.to_display_string()).collect())
        .collect()
}

#[test]
fn audit1108_comments_cannot_rewrite_literal_contents() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(id INT PRIMARY KEY,c VARCHAR(200),d VARCHAR(200))",
        "INSERT INTO t VALUES(1,'old','old')",
    ]);
    for comment in [
        "-- customer's address",
        "# customer's address",
        "/* customer's address */",
    ] {
        let sql = format!("UPDATE t {comment}\nSET c='No. 5, Main St' WHERE id=1 AND d != 'x'");
        engine.execute(&sql).unwrap();
        assert_eq!(
            rows(&engine, "SELECT c FROM t"),
            vec![vec!["No. 5, Main St"]]
        );
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT CONCAT('a') {comment}\n, 'tagged AS 1st'")
            ),
            vec![vec!["a", "tagged AS 1st"]]
        );
    }
    engine
        .execute("INSERT INTO t (id,c) -- don't\nVALUES(2,'x AS 3rd, (), ROW(1)')")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT c FROM t WHERE id=2"),
        vec![vec!["x AS 3rd, (), ROW(1)"]]
    );
}

#[test]
fn audit1108_cte_multi_dml_shadows_catalog_and_is_read_only() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(a INT PRIMARY KEY,b INT)",
        "INSERT INTO t VALUES(1,1),(2,2),(3,3)",
        "CREATE TABLE c(a INT)",
        "INSERT INTO c VALUES(2)",
    ]);
    engine
        .execute("WITH c AS (SELECT 1 a) UPDATE t JOIN c ON t.a=c.a SET t.b=999")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t ORDER BY a"),
        vec![vec!["1", "999"], vec!["2", "2"], vec!["3", "3"]]
    );
    engine
        .execute("WITH c AS (SELECT 3 a) DELETE t FROM t JOIN c ON t.a=c.a")
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t ORDER BY a"),
        vec![vec!["1", "999"], vec!["2", "2"]]
    );
    assert!(engine
        .execute("WITH c AS (SELECT 1 a) UPDATE t JOIN c ON t.a=c.a SET c.a=4")
        .is_err());
    assert!(engine
        .execute("WITH c AS (SELECT 1 a) DELETE c FROM t JOIN c ON t.a=c.a")
        .is_err());
    assert_eq!(rows(&engine, "SELECT * FROM c"), vec![vec!["2"]]);
}

#[test]
fn audit1108_geometry_concat_preserves_internal_binary() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(g GEOMETRY)",
        "INSERT INTO t VALUES(POINT(1,1))",
    ]);
    for sql in ["SELECT HEX(CONCAT(g)),HEX(CONCAT_WS('',g)),HEX(g) FROM t", "SELECT HEX(CONCAT(POINT(1,1))),HEX(CONCAT_WS('',POINT(1,1))),HEX(GROUP_CONCAT(POINT(1,1)))"] {
        assert_eq!(rows(&engine,sql), vec![vec!["000000000101000000000000000000F03F000000000000F03F";3]]);
    }
}

#[test]
fn audit1108_overlapping_collection_topology_does_not_panic() {
    let (_dir, engine) = fixture(&[]);
    let gc="ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,10 0,10 10,0 10,0 0)),POLYGON((5 5,15 5,15 15,5 15,5 5)))')";
    assert_eq!(rows(&engine,&format!("SELECT ST_Overlaps({gc},ST_GeomFromText('POLYGON((8 8,20 8,20 20,8 20,8 8))')),ST_Crosses(ST_GeomFromText('LINESTRING(-1 7,20 7)'),{gc})")),vec![vec!["1","1"]]);
}

#[test]
fn audit1108_alias_instances_in_decorrelation() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE s(id INT PRIMARY KEY,k INT,v INT)",
        "INSERT INTO s VALUES(1,10,1),(2,20,100)",
        "CREATE TABLE t(k INT,x INT)",
        "INSERT INTO t VALUES(10,1),(10,2),(20,3)",
        "CREATE TABLE nn(id INT PRIMARY KEY,k INT NOT NULL)",
        "INSERT INTO nn VALUES(1,1),(2,2)",
    ]);
    assert_eq!(rows(&engine,"SELECT x.id,(SELECT COUNT(*) FROM t WHERE t.k=x.k) FROM s x JOIN s ON s.id=2 WHERE s.v>5 ORDER BY x.id"),vec![vec!["1","2"],vec!["2","1"]]);
    assert!(rows(
        &engine,
        "SELECT x.id FROM nn x LEFT JOIN nn ON nn.id=x.id+100 WHERE nn.k NOT IN(SELECT k FROM nn)"
    )
    .is_empty());
}

#[test]
fn audit1108_union_column_alias_pushdown_and_implicit_group() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE n(a INT,b INT)",
        "INSERT INTO n VALUES(1,10),(10,1)",
        "CREATE TABLE d(id INT PRIMARY KEY,tag VARCHAR(10))",
        "INSERT INTO d VALUES(10,'ten')",
        "CREATE TABLE e(k INT,x INT)",
    ]);
    assert_eq!(rows(&engine,"SELECT u.a,u.b FROM (SELECT a,b FROM n UNION ALL SELECT a,b FROM n) u(b,a) JOIN d ON u.a=d.id WHERE d.tag='ten'"),vec![vec!["10","1"],vec!["10","1"]]);
    assert!(rows(
        &engine,
        "SELECT * FROM (SELECT k,COUNT(*) c FROM e WHERE k=5) d WHERE d.k=5"
    )
    .is_empty());
}

#[test]
fn audit1108_float_zero_index_equality_and_uniqueness() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(id INT PRIMARY KEY,d DOUBLE,KEY(d))",
        "INSERT INTO t VALUES(1,0e0),(2,-1e0*0e0),(3,1e0)",
        "CREATE TABLE u(d DOUBLE UNIQUE)",
        "INSERT INTO u VALUES(0e0)",
    ]);
    for predicate in ["d=0", "d<=0", "d>=0 AND d<1"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM t WHERE {predicate} ORDER BY id")
            ),
            vec![vec!["1"], vec!["2"]]
        );
    }
    assert!(engine.execute("INSERT INTO u VALUES(-1e0*0e0)").is_err());
}

#[test]
fn audit1108_short_string_json_cast_and_min_distinct() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(v VARCHAR(100))",
        "INSERT INTO t VALUES('true'),('{\"x\":1}'),('[1]')",
    ]);
    assert_eq!(
        rows(&engine, "SELECT JSON_TYPE(CAST(v AS JSON)) FROM t"),
        vec![vec!["BOOLEAN"], vec!["OBJECT"], vec!["ARRAY"]]
    );
    engine.execute("INSERT INTO t VALUES('invalid')").unwrap();
    assert!(engine
        .execute("SELECT CAST(v AS JSON) FROM t")
        .and_then(QueryResponse::materialize)
        .is_err());
    assert_eq!(rows(&engine,"SELECT MIN(DISTINCT x),MAX(DISTINCT x) FROM (SELECT 2 x UNION ALL SELECT 1 UNION ALL SELECT 2) t"),vec![vec!["1","2"]]);
}

#[test]
fn audit1108_float_integer_rounding_and_range_checks() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(i INT,u BIGINT UNSIGNED)",
        "INSERT INTO t VALUES(2.7e0,0),(-2.7e0,0),(2.5e0,0),(3.5e0,0),(0,-0.4e0)",
    ]);
    assert_eq!(
        rows(&engine, "SELECT i FROM t ORDER BY i"),
        vec![vec!["-3"], vec!["0"], vec!["2"], vec!["3"], vec!["4"]]
    );
    for value in ["2147483647.5e0", "-2147483648.6e0", "1e100"] {
        let error = engine
            .execute(&format!("INSERT INTO t(i) VALUES({value})"))
            .unwrap_err();
        assert_eq!(error.code(), 1264, "{value}: {error:?}");
    }
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM t"), vec![vec!["5"]]);
}

#[test]
fn audit1108_group_concat_distinct_uses_tuple_boundaries() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(a VARCHAR(10),b VARCHAR(10))",
        "INSERT INTO t VALUES('ab','c'),('a','bc'),('ab','c'),('AB','C'),(NULL,'x')",
    ]);
    for sql in [
        "SELECT GROUP_CONCAT(DISTINCT a,b) FROM t",
        "SELECT GROUP_CONCAT(DISTINCT a,b ORDER BY a,b) FROM t",
    ] {
        assert_eq!(rows(&engine, sql), vec![vec!["abc,abc"]]);
    }
    assert_eq!(
        rows(&engine, "SELECT GROUP_CONCAT(DISTINCT CONCAT(a,b)) FROM t"),
        vec![vec!["abc"]]
    );
}

#[test]
fn audit1108_window_expression_metadata() {
    let (_dir, engine) = fixture(&["CREATE TABLE t(id INT)", "INSERT INTO t VALUES(1),(2)"]);
    let QueryResponse::ResultSet(result) = engine.execute("SELECT CONCAT('r',ROW_NUMBER() OVER(ORDER BY id)),ROW_NUMBER() OVER(ORDER BY id)+1 FROM t").unwrap().materialize().unwrap() else { panic!() };
    assert_eq!(result.columns[0].column_type, ColumnType::VarString);
    assert_eq!(result.columns[1].column_type, ColumnType::LongLong);
    assert_eq!(result.rows[0][0].to_display_string(), "r1");
}

#[test]
fn audit1108_cte_anchor_derived_and_forward_reference_extension() {
    let (_dir, engine) = fixture(&["CREATE TABLE b(x INT)", "INSERT INTO b VALUES(42)"]);
    for union in ["UNION ALL", "UNION"] {
        assert_eq!(rows(&engine,&format!("WITH RECURSIVE c(n) AS(SELECT n FROM (SELECT 1 n) d {union} SELECT n+1 FROM c WHERE n<3) SELECT * FROM c ORDER BY n")),vec![vec!["1"],vec!["2"],vec!["3"]]);
    }
    assert_eq!(
        rows(
            &engine,
            "WITH a AS(SELECT * FROM b),b AS(SELECT 1 x) SELECT * FROM a"
        ),
        vec![vec!["1"]]
    );
    engine.execute("DROP TABLE b").unwrap();
    assert_eq!(
        rows(
            &engine,
            "WITH a AS(SELECT * FROM b),b AS(SELECT 1 x) SELECT * FROM a"
        ),
        vec![vec!["1"]]
    );
}

#[test]
fn audit1108_time_calendar_anchor_and_timestampadd_fraction() {
    let (_dir, engine) = fixture(&[
        "SET timestamp=1577880000",
        "CREATE TABLE t(tm TIME(6))",
        "INSERT INTO t VALUES('12:34:56.123456'),('-25:00:00')",
    ]);
    assert_eq!(rows(&engine,"SELECT CAST(TIME'12:34:56' AS DATE)=CURRENT_DATE(),DATEDIFF(TIME'12:34:56',CURRENT_DATE())"),vec![vec!["1","0"]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT EXTRACT(DAY_HOUR FROM tm),EXTRACT(DAY_MICROSECOND FROM tm) FROM t"
        ),
        vec![vec!["12", "123456123456"], vec!["-25", "-250000000000"]]
    );
    assert_eq!(rows(&engine,"SELECT TIMESTAMPADD(SECOND,1.5,CAST('2020-01-01 00:00:00' AS DATETIME(6)))=CAST('2020-01-01 00:00:01.5' AS DATETIME(6))"),vec![vec!["1"]]);
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT TIMESTAMPADD(HOUR,1,TIME'10:00:00')")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.columns[0].column_type, ColumnType::Time);
    assert_eq!(result.rows[0][0].to_display_string(), "11:00:00");
}

#[test]
fn audit1108_json_boolean_comparison_and_constructors() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(id INT,j JSON)",
        "INSERT INTO t VALUES(1,'true'),(2,'1'),(3,'false'),(4,'0')",
    ]);
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE j=TRUE ORDER BY id"),
        vec![vec!["1"]]
    );
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE FALSE=j ORDER BY id"),
        vec![vec!["3"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT JSON_ARRAY(ISNULL(NULL)),JSON_ARRAY(IS_UUID('x')),JSON_ARRAY((SELECT TRUE))"
        ),
        vec![vec!["[true]", "[false]", "[true]"]]
    );
    assert_eq!(rows(&engine,"SELECT JSON_CONTAINS_PATH('{\"a\":1}','one','$.a',NULL),JSON_CONTAINS_PATH('{\"a\":1}','all','$.x',NULL),JSON_UNQUOTE('\"'),JSON_VALUE('{\"a\":[1]}','$.a' RETURNING JSON)"),vec![vec!["1","0","\"","[1]"]]);
}

#[test]
fn audit1108_numeric_cast_bit_aggregate_and_like_escape() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(rows(&engine,"SELECT CAST(1.005e0 AS DECIMAL(10,2)),CAST(2.5e0 AS SIGNED),CAST(3.5e0 AS SIGNED),LEFT('abc',2.5e0),'a' LIKE 'a%%' ESCAPE '%'"),vec![vec!["1.01","2","4","ab","0"]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT BIT_OR(v),BIT_XOR(v),BIT_AND(v) FROM (SELECT 1.5 v UNION ALL SELECT 2.5) t"
        ),
        vec![vec!["3", "1", "2"]]
    );
}

#[test]
fn audit1108_spatial_scale_dimensions_and_binary_index_probe() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(g GEOMETRY NOT NULL SRID 0,SPATIAL INDEX(g))",
        "INSERT INTO t VALUES(POINT(1,1)),(POINT(5,5))",
    ]);
    assert_eq!(rows(&engine,"SELECT ST_Intersects(POINT(120.0005,30.0006),ST_GeomFromText('LINESTRING(120 30,120.001 30.001)')),ST_Dimension(ST_ConvexHull(ST_GeomFromText('MULTIPOINT(120 30,120.00001 30,120 30.00001)'))),ST_Dimension(ST_Envelope(ST_GeomFromText('LINESTRING(0 0,0.0000000001 1)')))"),vec![vec!["0","2","2"]]);
    assert_eq!(rows(&engine,"SELECT ABS(ST_X(ST_Centroid(ST_GeomFromText('POLYGON((120 30,120.00001 30,120.00001 30.00001,120 30.00001,120 30))')))-120.000005)<0.0000000001"),vec![vec!["1"]]);
    assert_eq!(rows(&engine,"SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0,1 0,0.0000000001 0)')),ST_IsSimple(ST_GeomFromText('LINESTRING(0 0,2 0,1 0)')),ST_IsValid(ST_GeomFromText('POLYGON((0 0,1 0,2 0,0 0))')),ST_Overlaps(POINT(0,0),ST_GeomFromText('LINESTRING(0 0,1 1)')) IS NULL,MBRTouches(POINT(1,1),POINT(1,1)),MBROverlaps(ST_GeomFromText('LINESTRING(0 0,2 0)'),ST_GeomFromText('LINESTRING(1 0,3 0)'))"),vec![vec!["0","0","0","1","0","1"]]);
    for hint in ["", "IGNORE INDEX(g)"] {
        assert_eq!(rows(&engine,&format!("SELECT COUNT(*) FROM t {hint} WHERE MBRIntersects(g,0x000000000101000000000000000000F03F000000000000F03F)")),vec![vec!["1"]]);
    }
    assert_eq!(rows(&engine,"SELECT ST_AsGeoJSON(POINT(1,2)),ST_AsText(ST_GeomFromText('POINT(10 20)',4326),'axis-order=long-lat'),ST_SRID(ST_GeomFromText('POINT(1 2)',4325.7))"),vec![vec!["{\"type\": \"Point\", \"coordinates\": [1.0, 2.0]}","POINT(20 10)","4326"]]);
}

#[test]
fn audit1108_rollup_subquery_uses_emitted_group_key() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE o(k INT)",
        "INSERT INTO o VALUES(10),(20)",
        "CREATE TABLE i(k INT,v INT)",
        "INSERT INTO i VALUES(10,1),(10,2),(20,3)",
    ]);
    assert_eq!(
        rows(
            &engine,
            "SELECT o.k,(SELECT MAX(v) FROM i WHERE i.k=o.k) FROM o GROUP BY o.k WITH ROLLUP"
        ),
        vec![vec!["10", "2"], vec!["20", "3"], vec!["NULL", "NULL"]]
    );
    assert_eq!(rows(&engine,"SELECT o.k,(SELECT COUNT(*) FROM i WHERE i.k=o.k LIMIT 1) FROM o GROUP BY o.k WITH ROLLUP"),vec![vec!["10","2"],vec!["20","1"],vec!["NULL","0"]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT o.k,(SELECT MAX(k) FROM i) FROM o GROUP BY o.k WITH ROLLUP"
        ),
        vec![vec!["10", "20"], vec!["20", "20"], vec!["NULL", "20"]]
    );
}

#[test]
fn audit1108_functional_dependencies_are_scoped_to_table_instances() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE a(id INT PRIMARY KEY,k INT,v INT)",
        "INSERT INTO a VALUES(1,10,1),(2,20,100)",
        "CREATE TABLE b(id INT PRIMARY KEY,k INT,v INT)",
        "INSERT INTO b VALUES(1,10,5),(2,10,6)",
    ]);
    for sql in [
        "SELECT a.k,COUNT(*) FROM a JOIN b ON a.id=b.id GROUP BY b.k",
        "SELECT a.v,COUNT(*) FROM a JOIN b ON a.k=b.k GROUP BY b.id",
    ] {
        let error = engine.execute(sql).unwrap_err();
        assert_eq!(error.code(), 1055, "{sql}: {error:?}");
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT a.v,b.v,COUNT(*) FROM a JOIN b ON a.id=b.id GROUP BY a.id ORDER BY a.id"
        ),
        vec![vec!["1", "5", "1"], vec!["100", "6", "1"]]
    );
    assert_eq!(
        rows(&engine, "SELECT ANY_VALUE(k),COUNT(*) FROM a"),
        vec![vec!["10", "2"]]
    );
}

#[test]
fn audit1108_geographic_planar_algorithms_fail_explicitly() {
    let (_dir, engine) = fixture(&[]);
    for sql in ["SELECT ST_FrechetDistance(ST_GeomFromText('LINESTRING(0 0,0 1)',4326),ST_GeomFromText('LINESTRING(0 0,1 0)',4326))", "SELECT ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0,1 1)',4326),0.5)", "SELECT ST_Centroid(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4326))", "SELECT ST_Intersects(ST_GeomFromText('LINESTRING(0 0,1 1)',4326),ST_GeomFromText('POINT(0 0)',4326))"] {
        let error=engine.execute(sql).unwrap_err();
        assert_eq!(error.code(),3618,"{sql}: {error:?}");
    }
}

#[test]
fn audit1108_geometry_binary_strings_and_structured_json() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(g GEOMETRY)",
        "INSERT INTO t VALUES(POINT(1,1))",
    ]);
    for from in ["", " FROM t"] {
        let g = if from.is_empty() { "POINT(1,1)" } else { "g" };
        let sql = format!("SELECT LENGTH({g}),CHAR_LENGTH({g}),HEX(SUBSTRING({g},1,4)),HEX(LOWER({g})),INSTR({g},'POINT'),STRCMP({g},'POINT(1 1)'),FIELD({g},'POINT(1 1)',{g}),ASCII({g}),JSON_TYPE(CAST({g} AS JSON)),JSON_EXTRACT(JSON_OBJECT('g',{g}),'$.g.type'){from}");
        assert_eq!(
            rows(&engine, &sql),
            vec![vec![
                "25",
                "25",
                "00000000",
                "000000000101000000000000000000F03F000000000000F03F",
                "0",
                "-1",
                "2",
                "0",
                "OBJECT",
                "\"Point\""
            ]]
        );
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT HEX(x) FROM (SELECT POINT(1,1) x UNION ALL SELECT 'abc')t"
        ),
        vec![
            vec!["000000000101000000000000000000F03F000000000000F03F"],
            vec!["616263"]
        ]
    );
    assert_eq!(rows(&engine,"SELECT JSON_EXTRACT(CAST(ST_GeomFromText('POINT(10 20)',4326) AS JSON),'$.coordinates')"),vec![vec!["[20.0, 10.0]"]]);
}

#[test]
fn audit1108_decimal_division_keeps_scratch_precision_until_output() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(a INT,b DECIMAL(12,2))",
        "INSERT INTO t VALUES(10,3.00),(5,7.00)",
    ]);
    assert_eq!(
        rows(
            &engine,
            "SELECT 2/3*100,1/3*3,10/3*3,1/6*6,5/7*7,CAST(2/3 AS DECIMAL(30,20)),1/3*3=1"
        ),
        vec![vec![
            "66.6667",
            "1.0000",
            "10.0000",
            "1.0000",
            "5.0000",
            "0.66666666600000000000",
            "0"
        ]]
    );
    assert_eq!(
        rows(&engine, "SELECT a/7*100 FROM t ORDER BY a"),
        vec![vec!["71.4286"], vec!["142.8571"]]
    );

    assert_eq!(
        rows(&engine, "SELECT COUNT(*)/7*100 FROM t"),
        vec![vec!["28.5714"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT (SELECT 1/3)*3,IF(1,1/3,0)*3,COALESCE(1/3,0)*3,(1/3+0)*3"
        ),
        vec![vec!["1.0000"; 4]]
    );
    assert_eq!(
        rows(&engine, "SELECT x*3 FROM (SELECT 1/3 x)t"),
        vec![vec!["0.9999"]]
    );
    assert_eq!(
        rows(&engine, "SELECT b*100/SUM(b) OVER() FROM t ORDER BY a DESC"),
        vec![vec!["30.000000"], vec!["70.000000"]]
    );
}

#[test]
fn audit1108_between_uses_one_comparison_domain() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(s VARCHAR(20))",
        "INSERT INTO t VALUES('abc'),('10'),('010'),('12abc'),(NULL)",
    ]);
    assert_eq!(
        rows(
            &engine,
            "SELECT '10' BETWEEN '9' AND 11,'10' NOT BETWEEN '9' AND 11,'010' BETWEEN '10' AND 10"
        ),
        vec![vec!["1", "0", "1"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT s FROM t WHERE s BETWEEN '10' AND 13 ORDER BY s"
        ),
        vec![vec!["010"], vec!["10"], vec!["12abc"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT 5 BETWEEN NULL AND 3,5 BETWEEN NULL AND 8,5 NOT BETWEEN NULL AND 3"
        ),
        vec![vec!["0", "NULL", "1"]]
    );
}

#[test]
fn audit1108_json_remove_tracks_auto_wrapped_parent() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(
        rows(
            &engine,
            r#"SELECT JSON_REMOVE('{"a":1}','$.a[0]'),JSON_REMOVE('[1,2]','$[0][0]'),JSON_REMOVE('{"a":{"b":1}}','$.a[last]'),JSON_REMOVE('1','$[0]')"#
        ),
        vec![vec!["{}", "[2]", "{}", "1"]]
    );
    assert!(engine
        .execute(r#"SELECT JSON_REMOVE('{"a":1}','$ ')"#)
        .is_err());
}

#[test]
fn audit1108_geometry_producers_reject_invalid_coordinates_and_drop_collapsed_rings() {
    let (_dir, engine) = fixture(&["CREATE TABLE t(g GEOMETRY)"]);
    for value in [
        "ST_GeomFromText('POINT(100 2)',4326)",
        "ST_SRID(POINT(1,200),4326)",
        "ST_X(ST_GeomFromText('POINT(1 2)',4326),100)",
    ] {
        assert!(engine
            .execute(&format!("INSERT INTO t VALUES({value})"))
            .is_err());
    }
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM t"), vec![vec!["0"]]);
    assert_eq!(rows(&engine,"SELECT ST_AsText(ST_GeomFromText('POINT(90 180)',4326)),ST_AsText(ST_Simplify(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'),100))"),vec![vec!["POINT(90 180)","NULL"]]);
}

#[test]
fn audit1108_json_string_assignment_retains_json_quotes() {
    let (_dir, engine) = fixture(&[
        r#"CREATE TABLE t(c CHAR(20),v VARCHAR(50),b VARBINARY(50),e ENUM('abc','"abc"'))"#,
        r#"INSERT INTO t VALUES(CAST('"abc"' AS JSON),CAST('"abc"' AS JSON),CAST('"abc"' AS JSON),CAST('"abc"' AS JSON))"#,
    ]);
    assert_eq!(
        rows(&engine, "SELECT c,v,HEX(b),e FROM t"),
        vec![vec!["\"abc\"", "\"abc\"", "2261626322", "\"abc\""]]
    );
}

#[test]
fn audit1108_json_value_strict_conversion_obeys_on_error() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(
        rows(
            &engine,
            r#"SELECT JSON_VALUE('{"a":2.5}','$.a' RETURNING SIGNED),JSON_VALUE('{"a":-2.5}','$.a' RETURNING SIGNED),JSON_VALUE('{"a":"42 "}','$.a' RETURNING SIGNED DEFAULT 999 ON ERROR),JSON_VALUE('{"a":"12abc"}','$.a' RETURNING DOUBLE DEFAULT 999 ON ERROR),JSON_VALUE('{"a":12345}','$.a' RETURNING DECIMAL(4,1) DEFAULT 999 ON ERROR),JSON_VALUE('{"a":20200102}','$.a' RETURNING DATE),JSON_VALUE('{"a":" 42"}','$.a' RETURNING SIGNED)"#
        ),
        vec![vec!["2", "-2", "999", "999", "999.0", "NULL", "42"]]
    );
    assert!(engine
        .execute(r#"SELECT JSON_VALUE('{"a":"4.0"}','$.a' RETURNING SIGNED ERROR ON ERROR)"#)
        .is_err());
    assert!(engine
        .execute(r#"SELECT JSON_VALUE('{"a":"1e400"}','$.a' RETURNING DOUBLE ERROR ON ERROR)"#)
        .is_err());
    assert_eq!(
        rows(
            &engine,
            r#"SELECT JSON_VALUE('{"a":1e100}','$.a' RETURNING DECIMAL(65,0) DEFAULT 42 ON ERROR)"#
        ),
        vec![vec!["42"]]
    );
}

#[test]
fn audit1108_geometry_collection_column_accepts_multi_subtypes() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(g GEOMETRYCOLLECTION)",
        "INSERT INTO t VALUES(ST_GeomFromText('MULTIPOINT(1 1,2 2)'))",
    ]);
    assert_eq!(
        rows(&engine, "SELECT ST_GeometryType(g) FROM t"),
        vec![vec!["MULTIPOINT"]]
    );
    assert!(engine.execute("INSERT INTO t VALUES(POINT(1,1))").is_err());
}

#[test]
fn audit1108_lag_default_participates_in_result_domain() {
    let (_dir, engine) = fixture(&["CREATE TABLE t(id INT)", "INSERT INTO t VALUES(1),(2)"]);
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT LAG(id,1,'x') OVER(ORDER BY id) v FROM t ORDER BY id")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(result.columns[0].column_type, ColumnType::VarString);
    assert_eq!(
        result
            .rows
            .iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>(),
        vec!["x", "1"]
    );
}

#[test]
fn audit1108_json_string_integer_cast_uses_string_contents() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(
        rows(
            &engine,
            r#"SELECT CAST(CAST('"3.5"' AS JSON) AS SIGNED),CAST(CAST('"1e2"' AS JSON) AS SIGNED),CAST(CAST('"12abc"' AS JSON) AS SIGNED),CAST(CAST('"9223372036854775807"' AS JSON) AS SIGNED)"#
        ),
        vec![vec!["3", "1", "12", "9223372036854775807"]]
    );
}

#[test]
fn audit1108_real_format_rounds_ties_even_and_preserves_negative_zero() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(rows(&engine,"SELECT FORMAT(2.5e0,0),FORMAT('0.125',2),FORMAT(-0.001e0,2),FORMAT(-0.0e0,2),FORMAT(2.5,0)"),vec![vec!["2","0.12","-0.00","-0.00","3"]]);
}

#[test]
fn audit1108_geometry_char_and_json_key_use_binary_charset() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(
        rows(&engine, "SELECT CAST(POINT(1,1) AS CHAR)"),
        vec![vec!["NULL"]]
    );
    assert!(engine.execute("SELECT JSON_OBJECT(POINT(1,1),1)").is_err());
    assert!(engine
        .execute("SELECT JSON_OBJECT(CAST('abc' AS BINARY),1)")
        .is_err());
}

#[test]
fn audit1108_temporal_unary_and_abs_return_numeric_domains() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(d DATE,tm TIME(6),dt DATETIME(6),y YEAR)",
        "INSERT INTO t VALUES('2020-01-01','10:00:00.123456','2020-01-01 10:00:00.123456',2020)",
    ]);
    assert_eq!(
        rows(&engine, "SELECT -d,-tm,ABS(d),-y,ABS(y) FROM t"),
        vec![vec![
            "-20200101",
            "-100000.123456",
            "20200101",
            "-2020",
            "2020"
        ]]
    );
    let QueryResponse::ResultSet(result) = engine.execute("SELECT -d,ABS(d) FROM t").unwrap()
    else {
        panic!()
    };
    assert!(result
        .columns
        .iter()
        .all(|column| column.column_type == ColumnType::Double));
}

#[test]
fn audit1108_geohash_respects_axis_order_and_cell_precision() {
    let (_dir, engine) = fixture(&[]);
    assert_eq!(rows(&engine,"SELECT ST_GeoHash(ST_GeomFromText('POINT(10 20)',4326),8),ST_AsText(ST_PointFromGeoHash('s3y0zh7w',4326)),ST_AsText(ST_PointFromGeoHash('s3y0zh7w',0)),ST_LatFromGeoHash('s'),ST_LongFromGeoHash('0'),ST_AsText(ST_PointFromGeoHash('zzzzzzzz',4326)),ST_AsText(ST_PointFromGeoHash('00000000',4326))"),vec![vec!["s3y0zh7w","POINT(10 20)","POINT(20 10)","22","-158","POINT(90 180)","POINT(-90 -180)"]]);
}

#[test]
fn audit1108_timestampadd_string_input_retains_string_domain() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(v VARCHAR(32))",
        "INSERT INTO t VALUES('2020-01-01')",
    ]);
    for from in ["", " FROM t"] {
        let arg = if from.is_empty() { "'2020-01-01'" } else { "v" };
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!(
                "SELECT TIMESTAMPADD(DAY,1,{arg}),TIMESTAMPADD(SECOND,1.5,{arg}){from}"
            ))
            .unwrap()
        else {
            panic!()
        };
        assert!(result
            .columns
            .iter()
            .all(|column| column.column_type == ColumnType::VarString));
        assert_eq!(result.rows[0][0].to_display_string(), "2020-01-02");
        assert_eq!(
            result.rows[0][1].to_display_string(),
            "2020-01-01 00:00:01.500000"
        );
    }
}

#[test]
fn audit1108_group_concat_bit_formats_fields_as_numbers() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(id INT,b BIT(8))",
        "INSERT INTO t VALUES(1,255),(2,1),(3,NULL)",
    ]);
    assert_eq!(rows(&engine,"SELECT GROUP_CONCAT(b ORDER BY id),GROUP_CONCAT(b,b ORDER BY id),HEX(GROUP_CONCAT(CONCAT(b) ORDER BY id)) FROM t"),vec![vec!["255,1","255255,11","FF2C01"]]);
    assert_eq!(
        rows(
            &engine,
            "SELECT GROUP_CONCAT(DISTINCT b,b ORDER BY id) FROM t"
        ),
        vec![vec!["255255,11"]]
    );
    assert!(engine
        .execute("SELECT GROUP_CONCAT(b) OVER() FROM t")
        .is_err());
}

#[test]
fn audit1108_all_zero_date_comparison_matches_persisted_sentinel() {
    let (_dir, engine) = fixture(&[]);
    let mut session = SessionContext::default();
    for sql in ["SET sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO'","CREATE TABLE t(id INT PRIMARY KEY,d DATE,dt DATETIME(6),KEY(d))","INSERT INTO t VALUES(0,'0000-00-00','0000-00-00 00:00:00'),(1,'2020-00-10','2020-01-00 10:00:00'),(2,'2020-01-01','2020-01-01 00:00:00')"] { engine.execute_with_session(&mut session,sql).unwrap(); }
    for (sql, expected) in [
        (
            "SELECT id FROM t WHERE d='0000-00-00' ORDER BY id",
            vec!["0"],
        ),
        (
            "SELECT id FROM t IGNORE INDEX(d) WHERE d='0000-00-00' ORDER BY id",
            vec!["0"],
        ),
        (
            "SELECT id FROM t WHERE d<='0000-00-00' ORDER BY id",
            vec!["0"],
        ),
        (
            "SELECT id FROM t WHERE '0000-00-00 00:00:00'=dt ORDER BY id",
            vec!["0"],
        ),
        (
            "SELECT id FROM t WHERE d IN('0000-00-00','2020-01-01') ORDER BY id",
            vec!["0", "2"],
        ),
    ] {
        let QueryResponse::ResultSet(result) =
            engine.execute_with_session(&mut session, sql).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row[0].to_display_string())
                .collect::<Vec<_>>(),
            expected,
            "{sql}"
        );
    }
    assert!(engine
        .execute_with_session(&mut session, "SELECT id FROM t WHERE d='2020-13-01'")
        .is_err());
}

#[test]
fn audit1108_string_extrema_preserve_original_numeric_spelling() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(i INT,d DECIMAL(12,4),s VARCHAR(20))",
        "INSERT INTO t VALUES(10,10.5000,'10'),(-10,-10.5000,'-10.5x')",
    ]);
    assert_eq!(
        rows(
            &engine,
            "SELECT LEAST(i,d,s),GREATEST(i,d,s) FROM t ORDER BY i"
        ),
        vec![vec!["-10", "-10.5x"], vec!["10", "10.5000"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT LEAST(2,1.25,'1'),GREATEST(2,1.25,'1'),BIN(''),OCT(''),BIN(0),OCT(0)"
        ),
        vec![vec!["1", "2", "NULL", "NULL", "0", "0"]]
    );
}

#[test]
fn audit1108_geojson_features_crs_and_geometry_contexts() {
    let (_dir, engine) = fixture(&[]);
    let point = r#"{"type":"Point","coordinates":[1,2]}"#;
    let feature = format!(r#"{{"type":"Feature","geometry":{point},"properties":{{}}}}"#);
    let collection = format!(
        r#"{{"type":"FeatureCollection","features":[{feature},{{"type":"Feature","geometry":null,"properties":null}}]}}"#
    );
    assert_eq!(rows(&engine,&format!("SELECT ST_AsText(ST_GeomFromGeoJSON('{feature}')),ST_AsText(ST_GeomFromGeoJSON('{collection}'))")),
        vec![vec!["POINT(2 1)","GEOMETRYCOLLECTION(POINT(2 1))"]]);
    for crs in ["EPSG:3857", "urn:ogc:def:crs:EPSG::3857"] {
        let doc = format!(
            r#"{{"type":"Point","coordinates":[1,2],"crs":{{"type":"name","properties":{{"name":"{crs}"}}}}}}"#
        );
        assert_eq!(rows(&engine,&format!("SELECT ST_SRID(ST_GeomFromGeoJSON('{doc}')),ST_AsText(ST_GeomFromGeoJSON('{doc}')),ST_AsText(ST_GeomFromGeoJSON('{doc}',1,4326))")),
            vec![vec!["3857","POINT(1 2)","POINT(2 1)"]]);
        let nested = format!(r#"{{"type":"GeometryCollection","geometries":[{doc}]}}"#);
        assert_eq!(
            engine
                .execute(&format!("SELECT ST_GeomFromGeoJSON('{nested}')"))
                .unwrap_err()
                .code(),
            3672
        );
    }
    assert_eq!(
        rows(
            &engine,
            r#"SELECT ST_GeomFromGeoJSON('{"type":"Feature","geometry":null,"properties":{}}')"#
        ),
        vec![vec!["NULL"]]
    );
    for doc in [format!(r#"{{"type":"GeometryCollection","geometries":[{feature}]}}"#), r#"{"type":"Point","coordinates":[1,2],"crs":{"type":"name","properties":{"name":"bogus"}}}"#.to_owned()] {
        assert_eq!(engine.execute(&format!("SELECT ST_GeomFromGeoJSON('{doc}')")).unwrap_err().code(),3072);
    }
}

#[test]
fn audit1108_mercator_transform_preserves_high_latitudes_and_wraps_longitude() {
    let (_dir, engine) = fixture(&[]);
    let QueryResponse::ResultSet(result)=engine.execute("SELECT ST_Y(ST_Transform(ST_GeomFromText('POINT(89 20)',4326),3857)),ST_Y(ST_Transform(ST_GeomFromText('POINT(-89.9 0)',4326),3857)),ST_Longitude(ST_Transform(ST_GeomFromText('POINT(1e8 0)',3857),4326))").unwrap() else {panic!()};
    for (value, expected) in
        result.rows[0]
            .iter()
            .zip([30240971.95838556, -44927335.42709139, 178.31528411952152])
    {
        assert!(
            (value.to_display_string().parse::<f64>().unwrap() - expected).abs() < 1e-5,
            "{value:?}, expected {expected}"
        );
    }
    assert_eq!(rows(&engine,"SELECT ST_Transform(ST_GeomFromText('POINT(90 0)',4326),3857),ST_Transform(ST_GeomFromText('POINT(-90 0)',4326),3857)"),vec![vec!["NULL","NULL"]]);
    assert!(engine
        .execute("SELECT ST_GeomFromText('MULTIPOINT(1 1,(2 2))')")
        .is_err());
    assert_eq!(rows(&engine,"SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOINT((1 1),(2 2))')),ST_NumGeometries(ST_GeomFromText('MULTIPOINT(1 1,2 2)'))"),vec![vec!["2","2"]]);
}

#[test]
fn audit1108_integer_column_in_constants_keep_precision() {
    let (_dir,engine)=fixture(&["CREATE TABLE t(id INT PRIMARY KEY,n BIGINT,u BIGINT UNSIGNED,KEY(n),KEY(u))","INSERT INTO t VALUES(1,9007199254740993,18446744073709551615),(2,9007199254740992,9007199254740993),(3,2,2),(4,NULL,NULL)"]);
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        crate::engine::executor::vec::config::with_vec_mode(mode, || {
            for hint in ["", "IGNORE INDEX(n,u)"] {
                for (condition, expected) in [
                    ("n IN(9007199254740992e0)", vec!["2"]),
                    ("n IN('9007199254740992')", vec!["2"]),
                    ("n IN('9007199254740992',1e0,2,3,NULL)", vec!["2", "3"]),
                    ("n NOT IN('9007199254740992',2)", vec!["1"]),
                    ("u IN('18446744073709551615')", vec!["1"]),
                    ("u IN(18446744073709551616e0)", vec![]),
                    ("n IN(2.5e0)", vec![]),
                ] {
                    let actual = rows(
                        &engine,
                        &format!("SELECT id FROM t {hint} WHERE {condition} ORDER BY id"),
                    );
                    assert_eq!(
                        actual,
                        expected.into_iter().map(|id| vec![id]).collect::<Vec<_>>(),
                        "{condition} {hint}"
                    );
                }
            }
        });
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT n IN('9007199254740992'),n IN(9007199254740992e0) FROM t WHERE id=1"
        ),
        vec![vec!["0", "0"]]
    );
}

#[test]
fn audit1108_sum_avg_distinct_deduplicate_converted_string_values() {
    let (_dir,engine)=fixture(&["CREATE TABLE t(g INT,s VARCHAR(10),n DECIMAL(12,2))","INSERT INTO t VALUES(1,'1',1.25),(1,'01',1.25),(1,'1.0',2.50),(1,'a',NULL),(2,'2',2.50),(2,'2.0',2.50),(2,'02',3.75)"]);
    for _ in 0..7 {
        engine.execute("INSERT INTO t SELECT * FROM t").unwrap();
    }
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        crate::engine::executor::vec::config::with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT /*+ PARALLEL(4) */ SUM(DISTINCT s),AVG(DISTINCT s),COUNT(DISTINCT s),SUM(DISTINCT n),AVG(DISTINCT n) FROM t"),
        vec![vec!["3","1","7","7.50","2.500000"]]);
            assert_eq!(
        rows(
            &engine,
            "SELECT g,SUM(DISTINCT s),AVG(DISTINCT s) FROM t GROUP BY g WITH ROLLUP ORDER BY g IS NULL,g"
        ),
        vec![
            vec!["1", "1", "0.5"],
            vec!["2", "2", "2"],
            vec!["NULL", "3", "1"]
        ]
    );
            assert_eq!(
                rows(
                    &engine,
                    "SELECT SUM(DISTINCT s),AVG(DISTINCT s) FROM t WHERE g=3"
                ),
                vec![vec!["NULL", "NULL"]]
            );
        });
    }
}

#[test]
fn audit1108_load_enclosure_preserves_bytes_and_fixed_rows_fail_explicitly() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(id INT,s VARCHAR(100))",
        "CREATE TABLE fixed(a CHAR(3),b CHAR(4))",
    ]);
    let mut session = SessionContext::default();
    let statement=crate::parser::parse_statement(r#"LOAD DATA LOCAL INFILE 'client-input' INTO TABLE t FIELDS TERMINATED BY ',' ENCLOSED BY '"'"#).unwrap();
    engine
        .execute_load_data_statement_from_reader(
            &mut session,
            statement,
            Box::new(std::io::Cursor::new(b"1,\"b\"x,zz\n2,\"c\"\n".to_vec())),
        )
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT id,s FROM t"),
        vec![vec!["1", "b\"x,zz\n2,\"c"]]
    );
    let statement=crate::parser::parse_statement("LOAD DATA LOCAL INFILE 'client-input' INTO TABLE fixed FIELDS TERMINATED BY '' ENCLOSED BY ''").unwrap();
    let err = engine
        .execute_load_data_statement_from_reader(
            &mut session,
            statement,
            Box::new(std::io::Cursor::new(b"  1abc  \n".to_vec())),
        )
        .unwrap_err();
    assert_eq!(err.code(), 1235);
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM fixed"), vec![vec!["0"]]);
}

#[test]
fn audit1108_load_strict_field_validation_and_ignore_diagnostics() {
    let (dir, engine) = fixture(&["CREATE TABLE t(id INT,s VARCHAR(20))"]);
    let mut session = SessionContext::default();
    for (name, input, code) in [
        ("many", "1,a\n2,b,x\n", 1262),
        ("few", "1,a\n2\n", 1261),
        ("empty", "1,a\n,b\n", 1366),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, input).unwrap();
        for suffix in ["", " SET s=CONCAT(s,'x')"] {
            let sql = format!(
                "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ','{suffix}",
                crate::util::file::sql_path(&path)
            );
            let err = engine.execute_with_session(&mut session, &sql).unwrap_err();
            assert_eq!(err.code(), code, "{name} {suffix}: {err:?}");
            assert_eq!(rows(&engine, "SELECT COUNT(*) FROM t"), vec![vec!["0"]]);
        }
        let sql = format!(
            "LOAD DATA INFILE '{}' IGNORE INTO TABLE t FIELDS TERMINATED BY ','",
            crate::util::file::sql_path(&path)
        );
        engine.execute_with_session(&mut session, &sql).unwrap();
        assert_eq!(session.warning_count(), 1);
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert!(
            warnings.rows[0][2].to_display_string().contains("2"),
            "{:?}",
            warnings.rows
        );
        engine.execute("DELETE FROM t").unwrap();
    }
    for separator in [",", "||"] {
        let input = format!("1{separator}a{separator}\n2{separator}b{separator}\n");
        let statement = crate::parser::parse_statement(&format!(
            "LOAD DATA LOCAL INFILE 'client-input' INTO TABLE t FIELDS TERMINATED BY '{separator}'"
        ))
        .unwrap();
        engine
            .execute_load_data_statement_from_reader(
                &mut session,
                statement,
                Box::new(std::io::Cursor::new(input.into_bytes())),
            )
            .unwrap();
        assert_eq!(session.warning_count(), 0);
        assert_eq!(
            rows(&engine, "SELECT id,s FROM t ORDER BY id"),
            vec![vec!["1", "a"], vec!["2", "b"]]
        );
        engine.execute("DELETE FROM t").unwrap();
    }
    let statement = crate::parser::parse_statement(
        "LOAD DATA LOCAL INFILE 'client-input' INTO TABLE t FIELDS TERMINATED BY ',' (id,@a,@b,@c)",
    )
    .unwrap();
    engine
        .execute_load_data_statement_from_reader(
            &mut session,
            statement,
            Box::new(std::io::Cursor::new(b"1,x,y\n2,x,y\n".to_vec())),
        )
        .unwrap();
    assert_eq!(session.warning_count(), 0);
    for kind in ["INT", "DOUBLE", "FLOAT", "DECIMAL(8,2)", "DECIMAL(50,8)"] {
        engine.execute("DROP TABLE IF EXISTS numeric_load").unwrap();
        engine
            .execute(&format!("CREATE TABLE numeric_load(id INT,n {kind})"))
            .unwrap();
        let statement=crate::parser::parse_statement("LOAD DATA LOCAL INFILE 'client-input' INTO TABLE numeric_load FIELDS TERMINATED BY ','").unwrap();
        engine
            .execute_load_data_statement_from_reader(
                &mut session,
                statement,
                Box::new(std::io::Cursor::new(b"1,\n2,\n".to_vec())),
            )
            .unwrap();
        let QueryResponse::ResultSet(warnings) = engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(warnings.rows.len(), 2, "{kind}: {:?}", warnings.rows);
        for (index, row) in warnings.rows.iter().enumerate() {
            let text = row[2].to_display_string();
            assert!(
                text.contains("column 'n'") && text.contains(&format!("row {}", index + 1)),
                "{kind}: {text}"
            );
        }
    }
}

#[test]
fn audit1108_integer_column_comparison_projection_matches_filter() {
    let (_dir, engine) = fixture(&[
        "CREATE TABLE t(id INT PRIMARY KEY,n BIGINT,KEY(n))",
        "INSERT INTO t VALUES(1,9007199254740993),(2,9007199254740992),(3,2)",
    ]);
    for mode in [
        crate::engine::executor::vec::config::VecMode::Off,
        crate::engine::executor::vec::config::VecMode::On,
    ] {
        crate::engine::executor::vec::config::with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT n='9007199254740992' OR n=2,n=9007199254740992e0,n>9007199254740992e0,9007199254740992e0<n FROM t ORDER BY id"),vec![vec!["0","0","1","1"],vec!["1","1","0","0"],vec!["1","0","0","0"]]);
        });
    }
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "PREPARE p FROM 'SELECT n=? FROM t ORDER BY id'",
        )
        .unwrap();
    for (parameter, expected) in [
        ("9007199254740992e0", vec!["0", "1", "0"]),
        ("'9007199254740993'", vec!["1", "0", "0"]),
        ("2", vec!["0", "0", "1"]),
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET @v={parameter}"))
            .unwrap();
        let response = engine
            .execute_with_session(&mut session, "EXECUTE p USING @v")
            .and_then(QueryResponse::materialize)
            .unwrap();
        let QueryResponse::ResultSet(result) = response else {
            panic!()
        };
        assert_eq!(
            result
                .rows
                .iter()
                .map(|row| row[0].to_display_string())
                .collect::<Vec<_>>(),
            expected,
            "{parameter}"
        );
    }
}

#[test]
fn audit1108_signed_zero_covering_projection_is_lossless() {
    use crate::engine::executor::vec::config::{with_vec_mode, VecMode};
    for kind in ["FLOAT", "DOUBLE"] {
        let (_dir, engine) = fixture(&[
            &format!("CREATE TABLE t(id INT PRIMARY KEY,d {kind},a INT,KEY kd(d))"),
            "INSERT INTO t VALUES(1,-1e0*0e0,1),(2,0e0,1)",
            "CREATE INDEX ka ON t(a) INCLUDE(d)",
            &format!("CREATE TABLE u(d {kind} PRIMARY KEY)"),
            "INSERT INTO u VALUES(-1e0*0e0)",
            "CREATE TABLE p(k INT)",
            "INSERT INTO p VALUES(0)",
        ]);
        for mode in [VecMode::Off, VecMode::On] {
            with_vec_mode(mode, || {
                for hint in ["FORCE INDEX(kd)", "IGNORE INDEX(kd,ka)"] {
                    assert_eq!(rows(&engine,&format!("SELECT FORMAT(d,2),CAST(d AS CHAR) FROM t {hint} WHERE d=0 ORDER BY 1")),vec![vec!["-0.00","-0"],vec!["0.00","0"]],"{kind} {mode:?} {hint}");
                    assert_eq!(rows(&engine,&format!("SELECT FORMAT(d,2) FROM t {hint} WHERE d=0 AND FORMAT(d,2)='-0.00'")),vec![vec!["-0.00"]]);
                }
                assert_eq!(
                    rows(
                        &engine,
                        "SELECT FORMAT(d,2) FROM t FORCE INDEX(ka) WHERE a=1 ORDER BY 1 LIMIT 2"
                    ),
                    vec![vec!["-0.00"], vec!["0.00"]]
                );
                let mut random_rows = rows(
                    &engine,
                    "SELECT FORMAT(d,2) FROM t FORCE INDEX(ka) WHERE a=1 ORDER BY RAND(7) LIMIT 2",
                );
                random_rows.sort();
                assert_eq!(random_rows, vec![vec!["-0.00"], vec!["0.00"]]);
                assert_eq!(
                    rows(&engine, "SELECT FORMAT(MIN(d),2),FORMAT(MAX(d),2) FROM u"),
                    vec![vec!["-0.00", "-0.00"]]
                );
                assert_eq!(
                    rows(
                        &engine,
                        "SELECT FORMAT(u.d,2) FROM p STRAIGHT_JOIN u ON u.d=p.k"
                    ),
                    vec![vec!["-0.00"]]
                );
                assert_eq!(
                    rows(&engine, "SELECT COUNT(*) FROM t FORCE INDEX(kd) WHERE d=0"),
                    vec![vec!["2"]]
                );
                let plan = rows(
                    &engine,
                    "EXPLAIN ANALYZE SELECT COUNT(d) FROM t FORCE INDEX(kd)",
                )
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" ");
                assert!(
                    !plan.contains("heap_rows_fetched="),
                    "COUNT(d) must keep normalized zero keys index-only: {plan}"
                );
            });
        }
    }
}
