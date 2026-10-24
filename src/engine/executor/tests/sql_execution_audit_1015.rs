use super::*;

fn setup(statements: &[&str]) -> (tempfile::TempDir, StorageQueryEngine) {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let engine = open_test_engine(dir.path());
    for sql in statements {
        engine.execute(sql).unwrap();
    }
    (dir, engine)
}

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<String>> {
    let QueryResponse::ResultSet(result) = engine
        .execute(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .materialize()
        .unwrap()
    else {
        panic!("expected rows: {sql}")
    };
    result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| value.to_display_string())
                .collect()
        })
        .collect()
}

#[test]
fn sqlaudit_zone_options_do_not_consume_query_identifiers() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE zone_map(zone_size INT, zone_map INT)",
        "INSERT INTO zone_map VALUES(1,2),(2,3)",
        "CREATE VIEW zview AS SELECT COUNT(*) AS n FROM zone_map",
        "CREATE TABLE zcopy AS SELECT COUNT(*) AS n FROM zone_map",
        "CREATE TABLE zbody(n INT) ZONE_MAP=ON AS SELECT COUNT(*) AS n FROM zone_map",
    ]);
    for sql in [
        "SELECT COUNT(*) FROM zone_map",
        "SELECT n FROM zview",
        "SELECT n FROM zcopy",
        "SELECT n FROM zbody",
    ] {
        assert_eq!(rows(&engine, sql), vec![vec!["2"]], "{sql}");
    }
    assert_eq!(
        rows(
            &engine,
            "SELECT CONCAT('x'),zone_size FROM zone_map WHERE zone_map=2"
        ),
        vec![vec!["x", "1"]]
    );
    engine
        .execute("ALTER TABLE zone_map ZONE_SIZE=128K")
        .unwrap();
    engine
        .execute("ALTER TABLE zone_map MODIFY zone_size BIGINT")
        .unwrap();
}

#[test]
fn sqlaudit_like_prefix_bounds_match_residual_across_collations() {
    let (_dir, engine) = setup(&[]);
    for collation in [
        "utf8mb4_general_ci",
        "utf8_general_ci",
        "latin1_swedish_ci",
        "utf8mb4_unicode_ci",
        "utf8mb4_0900_ai_ci",
        "utf8mb4_bin",
    ] {
        engine.execute("DROP TABLE IF EXISTS t").unwrap();
        engine
            .execute(&format!(
                "CREATE TABLE t(id INT PRIMARY KEY,s VARCHAR(40) COLLATE {collation},KEY ix(s))"
            ))
            .unwrap();
        engine.execute("INSERT INTO t VALUES(1,'user123'),(2,'User456'),(3,'zz'),(4,'éx'),(5,'êy'),(6,NULL)").unwrap();
        for pattern in ["user%", "USER%", "zz%", "é%", "u_ser%", "%", "no%"] {
            let oracle = rows(&engine, &format!("SELECT id FROM t WHERE CONCAT(s,'') COLLATE {collation} LIKE '{pattern}' ORDER BY id"));
            for hint in ["", "FORCE INDEX(ix)", "IGNORE INDEX(ix)"] {
                let sql = format!("SELECT id FROM t {hint} WHERE s LIKE '{pattern}' ORDER BY id");
                assert_eq!(rows(&engine, &sql), oracle, "{collation}: {sql}");
            }
        }
    }
}

#[test]
fn sqlaudit_empty_outer_join_global_and_grouped_aggregates() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE p(id INT PRIMARY KEY,k INT)",
        "CREATE TABLE q(id INT PRIMARY KEY,k INT)",
        "INSERT INTO p VALUES(1,1),(2,2)",
    ]);
    for mode in [vec::config::VecMode::Off, vec::config::VecMode::On] {
        vec::config::with_vec_mode(mode, || {
            for join in [
                "p RIGHT JOIN q ON p.k=q.k",
                "q LEFT JOIN p ON p.k=q.k",
                "p JOIN q ON p.k=q.k",
            ] {
                assert_eq!(
                    rows(
                        &engine,
                        &format!("SELECT COUNT(*),SUM(q.k),MAX(p.id) FROM {join}")
                    ),
                    vec![vec!["0", "NULL", "NULL"]]
                );
                assert!(rows(
                    &engine,
                    &format!("SELECT q.k,COUNT(*) FROM {join} GROUP BY q.k")
                )
                .is_empty());
            }
            assert_eq!(
                rows(
                    &engine,
                    "SELECT COUNT(*),COUNT(q.id) FROM p LEFT JOIN q ON p.k=q.k"
                ),
                vec![vec!["2", "0"]]
            );
        });
    }
}

#[test]
fn sqlaudit_update_trigger_noop_preserves_automatic_timestamp() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,v INT,ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
        "CREATE TABLE log(ts TIMESTAMP)",
        "INSERT INTO t VALUES(1,7,'2020-01-01 00:00:00')",
        "CREATE TRIGGER bt BEFORE UPDATE ON t FOR EACH ROW INSERT INTO log VALUES(NEW.ts)",
    ]);
    let response = engine.execute("UPDATE t SET v=7 WHERE id=1").unwrap();
    assert!(matches!(
        response,
        QueryResponse::OkAffected { affected_rows: 0 }
    ));
    assert_eq!(
        rows(&engine, "SELECT ts FROM t"),
        vec![vec!["2020-01-01 00:00:00"]]
    );
    assert_eq!(
        rows(&engine, "SELECT ts FROM log"),
        vec![vec!["2020-01-01 00:00:00"]]
    );
    engine
        .execute(
            "CREATE TRIGGER bt2 BEFORE UPDATE ON t FOR EACH ROW SET NEW.ts='2021-01-01 00:00:00'",
        )
        .unwrap();
    assert!(matches!(
        engine.execute("UPDATE t SET v=7 WHERE id=1").unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    ));
    assert_eq!(
        rows(&engine, "SELECT ts FROM t"),
        vec![vec!["2021-01-01 00:00:00"]]
    );
}

#[test]
fn sqlaudit_fk_child_recreation_invalidates_parent_point_update() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE p(id INT PRIMARY KEY,x INT)",
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id) ON UPDATE CASCADE)",
        "INSERT INTO p VALUES(2,2)", "INSERT INTO c VALUES(12,2)",
        "UPDATE p SET id=20 WHERE id=2", "DROP TABLE c",
        "CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id) ON UPDATE SET NULL)",
        "INSERT INTO c VALUES(1,20)", "UPDATE p SET id=30 WHERE id=20",
    ]);
    assert_eq!(rows(&engine, "SELECT pid FROM c"), vec![vec!["NULL"]]);
    engine.execute("DROP TABLE c").unwrap();
    engine.execute("CREATE TABLE c(id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id) ON UPDATE RESTRICT)").unwrap();
    engine.execute("INSERT INTO c VALUES(1,30)").unwrap();
    assert_eq!(
        engine
            .execute("UPDATE p SET id=40 WHERE id=30")
            .unwrap_err()
            .code(),
        1451
    );
    assert_eq!(rows(&engine, "SELECT pid FROM c"), vec![vec!["30"]]);
}

#[test]
fn sqlaudit_group_dependency_uses_bound_constants_and_singleton_in() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,g INT,n VARCHAR(20))",
        "INSERT INTO t VALUES(1,1,'a'),(2,1,'b')",
    ]);
    for predicate in ["id=1", "id IN(1)", "id=-(-1)"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT g,COUNT(*) FROM t WHERE {predicate} GROUP BY g ORDER BY n")
            ),
            vec![vec!["1", "1"]]
        );
    }
    assert_eq!(
        engine
            .execute("SELECT g,COUNT(*) FROM t WHERE id IN(1,2) GROUP BY g ORDER BY n")
            .unwrap_err()
            .code(),
        1055
    );
}

#[test]
fn sqlaudit_empty_insert_select_checks_projection_width() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(a INT,b INT)",
        "CREATE TABLE s(a INT,b INT,c INT)",
    ]);
    for sql in [
        "INSERT INTO t SELECT * FROM s",
        "INSERT INTO t(a) SELECT a,b FROM s",
        "REPLACE INTO t SELECT * FROM s",
        "INSERT INTO t SELECT 1,2,3 WHERE FALSE",
        "INSERT INTO t SELECT a,b,c FROM s UNION ALL SELECT a,b,c FROM s",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1136, "{sql}");
    }
    engine.execute("INSERT INTO t SELECT a,b FROM s").unwrap();
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM t"), vec![vec!["0"]]);
}

#[test]
fn sqlaudit_delete_target_respects_alias_namespace() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE a(id INT PRIMARY KEY)",
        "CREATE TABLE b(id INT)",
        "INSERT INTO a VALUES(1),(2)",
        "INSERT INTO b VALUES(1)",
    ]);
    assert_eq!(
        engine
            .execute("DELETE a FROM a x JOIN b ON x.id=b.id")
            .unwrap_err()
            .code(),
        1109
    );
    assert_eq!(rows(&engine, "SELECT COUNT(*) FROM a"), vec![vec!["2"]]);
    engine
        .execute("DELETE x FROM a x JOIN b ON x.id=b.id")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT id FROM a"), vec![vec!["2"]]);
}

#[test]
fn sqlaudit_float_strings_preserve_declared_type() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,f FLOAT,d DOUBLE)",
        "INSERT INTO t VALUES(1,0.1,0.1),(2,0.2,0.2)",
    ]);
    for tail in ["ORDER BY id", "WHERE id=1"] {
        let result = rows(
            &engine,
            &format!("SELECT f,CONCAT(f),CAST(f AS CHAR),CONCAT(d) FROM t {tail}"),
        );
        assert_eq!(result[0], vec!["0.1", "0.1", "0.1", "0.1"]);
    }
    assert_eq!(
        rows(&engine, "SELECT DISTINCT f FROM t ORDER BY f"),
        vec![vec!["0.1"], vec!["0.2"]]
    );
}

#[test]
fn sqlaudit_temporal_comparison_rounds_at_microseconds() {
    let (_dir, engine) = setup(&["SET time_zone='+00:00'", "CREATE TABLE t(id INT PRIMARY KEY,d DATETIME(6),t TIME(6),s TIMESTAMP(6),KEY ix(d))", "INSERT INTO t VALUES(1,'2024-01-01 23:59:59.999999','-00:00:00.123457','2024-01-01 23:59:59.999999'),(2,'2024-01-02 00:00:00','-00:00:00.123456','2024-01-02 00:00:00')"]);
    for column in ["d", "s"] {
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT id FROM t WHERE {column}>='2024-01-01 23:59:59.9999995'")
            ),
            vec![vec!["2"]],
            "{column}"
        );
    }
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE t='-00:00:00.1234565'"),
        vec![vec!["1"]]
    );
}

#[test]
fn sqlaudit_outer_aggregate_subquery_uses_group_result() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,k INT,g INT)",
        "CREATE TABLE i(id INT PRIMARY KEY,k INT)",
        "INSERT INTO t VALUES(1,1,10),(2,2,10),(3,3,20),(4,4,20)",
        "INSERT INTO i VALUES(1,2),(2,2),(3,4)",
    ]);
    for mode in [vec::config::VecMode::Off, vec::config::VecMode::On] {
        vec::config::with_vec_mode(mode, || {
            assert_eq!(rows(&engine,"SELECT g,(SELECT COUNT(*) FROM i WHERE i.k=MAX(t.k)) FROM t GROUP BY g ORDER BY g"),vec![vec!["10","2"],vec!["20","1"]]);
            assert_eq!(rows(&engine,"SELECT g,(SELECT COUNT(*) FROM i WHERE i.k=MAX(o.k)) FROM t o GROUP BY g ORDER BY g"),vec![vec!["10","2"],vec!["20","1"]]);
        });
    }
}

#[test]
fn sqlaudit_or_exists_keysets_keep_null_and_composite_semantics() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT PRIMARY KEY,a INT,b INT)",
        "CREATE TABLE i(id INT PRIMARY KEY,a INT,b INT)",
        "INSERT INTO t VALUES(1,1,2),(2,2,3),(3,NULL,3),(4,NULL,NULL)",
        "INSERT INTO i VALUES(1,1,2),(2,1,2),(3,7,NULL),(4,NULL,3)",
    ]);
    assert_eq!(rows(&engine,"SELECT id FROM t o WHERE EXISTS(SELECT 1 FROM i WHERE i.a=o.a AND i.b=o.b) OR EXISTS(SELECT 1 FROM i WHERE i.id=o.id+1) ORDER BY id"),vec![vec!["1"],vec!["2"],vec!["3"]]);
    assert_eq!(rows(&engine,"SELECT id FROM t o WHERE EXISTS(SELECT 1 FROM i WHERE i.a=o.a AND i.b=o.b) OR EXISTS(SELECT 1 FROM i WHERE i.id=o.id+9) ORDER BY id"),vec![vec!["1"]]);
}

#[test]
fn sqlaudit_cross_join_limit_streams_in_row_and_vector_modes() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT)",
        "INSERT INTO t SELECT n FROM generate_series(1,3000) AS s(n)",
    ]);
    for mode in [vec::config::VecMode::Off, vec::config::VecMode::On] {
        vec::config::with_vec_mode(mode, || {
            for join in [
                "t a,t b",
                "t a JOIN t b ON 1=1",
                "t a CROSS JOIN t b",
                "t a STRAIGHT_JOIN t b ON 1=1",
            ] {
                let start = std::time::Instant::now();
                let result = rows(
                    &engine,
                    &format!("SELECT a.id,b.id FROM {join} LIMIT 3 OFFSET 2"),
                );
                assert_eq!(result.len(), 3, "{join}");
                assert!(result.iter().all(|row| row.len() == 2));
                assert!(
                    start.elapsed() < Duration::from_secs(5),
                    "{join} took {:?}",
                    start.elapsed()
                );
            }
        });
    }
}

#[test]
fn sqlaudit_top_level_diagnostics_reads_previous_area() {
    let (_dir, engine) = setup(&["CREATE TABLE t(id INT)"]);
    let mut session = SessionContext::default();
    let err = engine
        .execute_with_session(&mut session, "SELECT * FROM missing_table")
        .unwrap_err();
    engine
        .execute_with_session(&mut session, "GET DIAGNOSTICS @n=NUMBER")
        .unwrap();
    assert_eq!(session.user_variable("n"), Some(&Value::Int(1)));
    engine.execute_with_session(&mut session,"GET CURRENT DIAGNOSTICS CONDITION 1 @e=MYSQL_ERRNO,@s=RETURNED_SQLSTATE,@m=MESSAGE_TEXT").unwrap();
    assert_eq!(
        session.user_variable("e"),
        Some(&Value::Int(err.code() as i64))
    );
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES(1),(2)")
        .unwrap();
    engine
        .execute_with_session(&mut session, "GET DIAGNOSTICS @r=ROW_COUNT")
        .unwrap();
    assert_eq!(session.user_variable("r"), Some(&Value::Int(2)));
    engine
        .execute_with_session(&mut session, "GET DIAGNOSTICS @r2=ROW_COUNT")
        .unwrap();
    assert_eq!(session.user_variable("r2"), Some(&Value::Int(2)));
    for sql in [
        "GET DIAGNOSTICS",
        "GET DIAGNOSTICS @n=NUMBER,",
        "GET DIAGNOSTICS @e=MYSQL_ERRNO",
        "GET DIAGNOSTICS CONDITION 1 @n=NUMBER",
    ] {
        assert_eq!(
            engine
                .execute_with_session(&mut session, sql)
                .unwrap_err()
                .code(),
            1064,
            "{sql}"
        );
    }
    assert_eq!(
        engine
            .execute_with_session(&mut session, "GET STACKED DIAGNOSTICS @n=NUMBER")
            .unwrap_err()
            .code(),
        3004
    );
}

#[test]
fn sqlaudit_odku_preserves_generated_and_explicit_insert_ids() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY,k INT UNIQUE,v INT)",
        "INSERT INTO t VALUES(1,1,0)",
    ]);
    let mut session = SessionContext::default();
    for sql in [
        "ALTER TABLE t AUTO_INCREMENT=1000",
        "INSERT INTO t(k,v) VALUES(2,0)",
        "INSERT INTO t(k,v) VALUES(1,8) ON DUPLICATE KEY UPDATE v=VALUES(v)",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        session
            .last_insert_id
            .load(std::sync::atomic::Ordering::Relaxed),
        1000
    );
    assert_eq!(session.statement_insert_id, 1);
    engine
        .execute_with_session(&mut session, "ALTER TABLE t AUTO_INCREMENT=2000")
        .unwrap();
    engine.execute_with_session(&mut session,"INSERT INTO t(k,v) SELECT CASE WHEN n<=64 THEN 1 ELSE 3 END,n FROM generate_series(1,65) AS s(n) ON DUPLICATE KEY UPDATE v=VALUES(v)").unwrap();
    assert_eq!(session.statement_insert_id, 2000);
    assert_eq!(
        session
            .last_insert_id
            .load(std::sync::atomic::Ordering::Relaxed),
        2000
    );
    engine.execute_with_session(&mut session,"INSERT INTO t(k,v) VALUES(1,9) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id),v=VALUES(v)").unwrap();
    assert_eq!(
        session
            .last_insert_id
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn sqlaudit_group_concat_warns_only_for_actual_truncation() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE t(s VARCHAR(64))",
        "INSERT INTO t VALUES('1234567890123456'),('abcdefghijklmnop')",
    ]);
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET group_concat_max_len=32")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT GROUP_CONCAT(s) FROM t")
        .unwrap();
    assert!(session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1260));
    engine
        .execute_with_session(&mut session, "SET group_concat_max_len=33")
        .unwrap();
    engine
        .execute_with_session(&mut session, "SELECT GROUP_CONCAT(s) FROM t")
        .unwrap();
    assert!(!session
        .warnings
        .iter()
        .any(|warning| warning.code() == 1260));
}

#[test]
fn sqlaudit_load_identical_delimiters_respects_target_count() {
    let (dir, engine) = setup(&["CREATE TABLE t(a VARCHAR(30),b VARCHAR(30))"]);
    let file = dir.path().join("overlap.txt");
    for (data, separator, expected) in [
        (
            "a,b\nb,c\n",
            ",",
            vec![vec!["a", "b\nb"], vec!["c\n", "NULL"]],
        ),
        ("a||b||c||d||", "||", vec![vec!["a", "b"], vec!["c", "d"]]),
        (
            "\"a,b\",c,d,",
            ",",
            vec![vec!["a,b", "c"], vec!["d", "NULL"]],
        ),
    ] {
        std::fs::write(&file, data).unwrap();
        engine.execute("DELETE FROM t").unwrap();
        engine.execute(&format!("LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY '{separator}' ENCLOSED BY '\"' LINES TERMINATED BY '{separator}'",crate::util::file::sql_path(&file))).unwrap();
        assert_eq!(rows(&engine, "SELECT a,b FROM t"), expected, "{data}");
    }
}

#[test]
fn sqlaudit_locking_aggregates_and_scalar_subqueries_execute() {
    let (_dir, engine) = setup(&[
        "CREATE TABLE g(id INT PRIMARY KEY,k INT)",
        "INSERT INTO g VALUES(1,1),(2,2)",
    ]);
    assert_eq!(
        rows(
            &engine,
            "SELECT COUNT(*) FROM g a JOIN g b ON a.k=b.k WHERE a.id<=2 FOR UPDATE"
        ),
        vec![vec!["2"]]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT id,(SELECT MAX(k) FROM g) FROM g WHERE id<=2 ORDER BY id FOR UPDATE"
        ),
        vec![vec!["1", "2"], vec!["2", "2"]]
    );
}
