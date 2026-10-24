//! End-to-end parser contracts, including catalog reload and independent engines.
use super::*;

fn run(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Result<QueryResponse, QueryError> {
    engine.execute_with_session(session, sql)
}
fn rows(engine: &StorageQueryEngine, session: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(result) = run(engine, session, sql).unwrap() else {
        panic!()
    };
    result.rows
}
fn fixture() -> (tempfile::TempDir, StorageQueryEngine, SessionContext) {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    run(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(40))",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        r"INSERT INTO t VALUES (1,'a\\b'),(2,'ab')",
    )
    .unwrap();
    (dir, engine, session)
}

#[test]
fn quality_check_and_view_preserve_disabled_like_escape() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        r"CREATE VIEW vl AS SELECT id FROM t WHERE s LIKE 'a\\b' ESCAPE ''",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT * FROM vl"),
        vec![vec![Value::Int(1)]]
    );
    run(
        &engine,
        &mut session,
        r"CREATE TABLE chk (s VARCHAR(40), CHECK(s LIKE 'a\\b' ESCAPE ''))",
    )
    .unwrap();
    run(&engine, &mut session, r"INSERT INTO chk VALUES ('a\\b')").unwrap();
    assert_eq!(
        run(&engine, &mut session, "INSERT INTO chk VALUES ('ab')")
            .unwrap_err()
            .code(),
        3819
    );
}

#[test]
fn quality_view_retains_partition_restriction() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "CREATE TABLE p (id INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN(MAXVALUE))").unwrap();
    run(&engine, &mut session, "INSERT INTO p VALUES(1),(20)").unwrap();
    run(
        &engine,
        &mut session,
        "CREATE VIEW vp AS SELECT id FROM p PARTITION(p0)",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT * FROM vp"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn quality_view_body_is_not_parsed_twice() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE VIEW vj AS SELECT JSON_VALUE('{\"n\":12}', '$.n' RETURNING SIGNED) AS j",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT * FROM vj"),
        vec![vec![Value::Int(12)]]
    );
}

#[test]
fn quality_view_definition_survives_mode_change_and_reload() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
        run(
            &engine,
            &mut session,
            r"CREATE VIEW v AS SELECT CONCAT('a\', 'b') AS x, ROW_NUMBER() OVER() AS rn",
        )
        .unwrap();
        let expected = vec![vec![Value::String(r"a\b".into()), Value::Int(1)]];
        assert_eq!(rows(&engine, &mut session, "SELECT * FROM v"), expected);
        run(&engine, &mut session, "SET sql_mode=''").unwrap();
        run(&engine, &mut session, "ALTER SYSTEM FLUSH SQL_CACHE").unwrap();
        assert_eq!(rows(&engine, &mut session, "SELECT * FROM v"), expected);
    }
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    assert_eq!(
        rows(&engine, &mut session, "SELECT * FROM v"),
        vec![vec![Value::String(r"a\b".into()), Value::Int(1)]]
    );
}

#[test]
fn quality_view_freezes_wildcard_with_window_expression() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE VIEW v AS SELECT *,ROW_NUMBER() OVER() AS rn FROM t",
    )
    .unwrap();
    let before = rows(&engine, &mut session, "SELECT * FROM v ORDER BY id");
    run(&engine, &mut session, "ALTER TABLE t ADD COLUMN z INT").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT * FROM v ORDER BY id"),
        before
    );
}

#[test]
fn quality_view_binds_every_expression_before_catalog_write() {
    let (_dir, engine, mut session) = fixture();
    for query in [
        "SELECT id FROM t WHERE missing=1",
        "SELECT id FROM t ORDER BY missing",
        "SELECT missing_fn(id) AS x FROM t",
    ] {
        assert!(
            run(
                &engine,
                &mut session,
                &format!("CREATE VIEW bad AS {query}")
            )
            .is_err(),
            "{query}"
        );
        assert!(run(&engine, &mut session, "SHOW CREATE VIEW bad").is_err());
    }
}

#[test]
fn quality_view_cache_isolates_catalog_instances() {
    let a = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let b = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let first = open_test_engine(a.path());
    let second = open_test_engine(b.path());
    let mut s1 = SessionContext::default();
    let mut s2 = SessionContext::default();
    run(&first, &mut s1, "CREATE VIEW v AS SELECT 11 AS x").unwrap();
    run(&second, &mut s2, "CREATE VIEW v AS SELECT 22 AS x").unwrap();
    assert_eq!(
        rows(&first, &mut s1, "SELECT * FROM v"),
        vec![vec![Value::Int(11)]]
    );
    assert_eq!(
        rows(&second, &mut s2, "SELECT * FROM v"),
        vec![vec![Value::Int(22)]]
    );
}

#[test]
fn quality_view_routine_dependencies_cover_all_expression_positions() {
    let (_dir, engine, mut session) = fixture();
    for query in [
        "SELECT COUNT(*) AS n FROM t GROUP BY dep(id)",
        "SELECT dep(id) IN(SELECT id FROM t) AS n FROM t",
    ] {
        run(
            &engine,
            &mut session,
            "CREATE FUNCTION dep(x INT) RETURNS INT DETERMINISTIC RETURN x",
        )
        .unwrap();
        run(
            &engine,
            &mut session,
            &format!("CREATE VIEW vdep AS {query}"),
        )
        .unwrap();
        let view = engine.database().view_definition("test", "vdep").unwrap();
        assert!(
            view.routine_dependencies
                .iter()
                .any(|dep| dep.name == "dep"),
            "{query}"
        );
        run(&engine, &mut session, "DROP FUNCTION dep").unwrap();
        assert_eq!(
            run(&engine, &mut session, "SELECT * FROM vdep")
                .unwrap_err()
                .code(),
            1356
        );
        run(&engine, &mut session, "DROP VIEW vdep").unwrap();
    }
}

#[test]
fn quality_view_rejects_variable_assignments_before_catalog_write() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET @x=7").unwrap();
    for query in [
        "SELECT @x:=1 AS x",
        "SELECT (SELECT @x:=2) AS x",
        "WITH c AS(SELECT @x:=3 AS x) SELECT x FROM c",
    ] {
        assert_eq!(
            run(
                &engine,
                &mut session,
                &format!("CREATE VIEW forbidden AS {query}")
            )
            .unwrap_err()
            .code(),
            1351,
            "{query}"
        );
        assert!(engine
            .database()
            .view_definition("test", "forbidden")
            .is_none());
        assert_eq!(
            rows(&engine, &mut session, "SELECT @x"),
            vec![vec![Value::Int(7)]]
        );
    }
}

#[test]
fn quality_readonly_authorization_keeps_order_expression_source_privileges() {
    let (_dir, engine, mut root) = fixture();
    run(&engine, &mut root, "CREATE USER 'read_alias'@'%'").unwrap();
    run(
        &engine,
        &mut root,
        "GRANT SELECT(id) ON test.t TO 'read_alias'@'%'",
    )
    .unwrap();
    let mut user = SessionContext::default();
    user.authenticated_as = crate::engine::auth::AccountKey::new("read_alias", "%");
    assert_eq!(
        rows(&engine, &mut user, "SELECT id AS s FROM t ORDER BY s"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    for sql in [
        "SELECT id AS s FROM t ORDER BY ABS(s)",
        "SELECT id AS s FROM t GROUP BY ABS(s)",
    ] {
        assert_eq!(
            run(&engine, &mut user, sql).unwrap_err().code(),
            1143,
            "{sql}"
        );
    }
}

#[test]
fn quality_nested_query_ctes_keep_scopes_outputs_and_view_reload() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    run(&engine, &mut session, "CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    run(&engine, &mut session, "INSERT INTO t VALUES(1),(2)").unwrap();
    let body = "SELECT n FROM (WITH c AS(SELECT id AS n FROM t UNION ALL SELECT 3) SELECT n FROM c UNION ALL SELECT 4) d";
    let expected = vec![
        vec![Value::Int(1)],
        vec![Value::Int(2)],
        vec![Value::Int(3)],
        vec![Value::Int(4)],
    ];
    assert_eq!(
        rows(&engine, &mut session, &format!("{body} ORDER BY n")),
        expected
    );
    run(
        &engine,
        &mut session,
        &format!("CREATE VIEW nested_query AS {body}"),
    )
    .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT n FROM nested_query ORDER BY n"
        ),
        expected
    );
    run(&engine, &mut session, "PREPARE nested_stmt FROM 'SELECT n+? FROM (WITH c AS(SELECT id AS n FROM t UNION ALL SELECT ?) SELECT n FROM c) d ORDER BY n'").unwrap();
    run(&engine, &mut session, "SET @a=10,@b=3").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "EXECUTE nested_stmt USING @a,@b"),
        vec![
            vec![Value::Int(11)],
            vec![Value::Int(12)],
            vec![Value::Int(13)]
        ]
    );
    drop(engine);
    let reopened = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(
            &reopened,
            &mut session,
            "SELECT n FROM nested_query ORDER BY n"
        ),
        expected
    );
}

#[test]
fn quality_view_codec_deep_expression_survives_restart() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        run(&engine, &mut session, "CREATE TABLE t(id INT)").unwrap();
        run(&engine, &mut session, "INSERT INTO t VALUES(1)").unwrap();
        let expression = format!("id{}", "+1".repeat(62));
        run(
            &engine,
            &mut session,
            &format!("CREATE VIEW deep_codec AS SELECT {expression} AS n FROM t"),
        )
        .unwrap();
        assert_eq!(
            rows(&engine, &mut session, "SELECT n FROM deep_codec"),
            vec![vec![Value::Int(63)]]
        );
    }
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    assert_eq!(
        rows(&engine, &mut session, "SELECT n FROM deep_codec"),
        vec![vec![Value::Int(63)]]
    );
}

#[test]
fn quality_view_dependency_identity_uses_semantics_under_creation_mode() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE TABLE q(foo VARCHAR(3) NOT NULL)",
    )
    .unwrap();
    run(&engine, &mut session, "INSERT INTO q VALUES('bar')").unwrap();
    let definition =
        r#"CREATE VIEW mv AS SELECT CONCAT("foo") AS n,ROW_NUMBER() OVER() AS rn FROM q"#;
    run(&engine, &mut session, "SET sql_mode=''").unwrap();
    run(&engine, &mut session, definition).unwrap();
    let before = engine.database().view_definition("test", "mv").unwrap();
    let deps = vec![(
        Arc::<str>::from("test.mv"),
        before.schema_version,
        before.plan_cache_identity(),
    )];
    assert!(engine.database().view_versions_match(&deps));
    assert_eq!(
        rows(&engine, &mut session, "SELECT n FROM mv"),
        vec![vec![Value::String("foo".into())]]
    );
    run(&engine, &mut session, "DROP VIEW mv").unwrap();
    run(&engine, &mut session, "SET sql_mode='ANSI_QUOTES'").unwrap();
    run(&engine, &mut session, definition).unwrap();
    run(&engine, &mut session, "SET sql_mode=''").unwrap();
    let after = engine.database().view_definition("test", "mv").unwrap();
    assert_eq!(before.sql, after.sql);
    assert_eq!(before.definition_normalized, after.definition_normalized);
    assert_eq!(
        serde_json::to_value(&before.columns).unwrap(),
        serde_json::to_value(&after.columns).unwrap()
    );
    assert_eq!(before.schema_version, after.schema_version);
    assert_ne!(
        before.query.as_ref().unwrap().select(),
        after.query.as_ref().unwrap().select()
    );
    assert!(
        !engine.database().view_versions_match(&deps),
        "old semantic dependencies were accepted after DROP/CREATE"
    );
    assert_ne!(before.plan_cache_identity(), after.plan_cache_identity());
    assert_eq!(
        rows(&engine, &mut session, "SELECT n FROM mv"),
        vec![vec![Value::String("bar".into())]]
    );
}

#[test]
fn quality_typed_view_query_rewrites_dynamic_escapes_in_ctes() {
    let (_dir, engine, mut session) = fixture();
    let body =
        r"WITH c AS(SELECT id,s FROM t) SELECT id FROM c WHERE s LIKE 'a\\b' ESCAPE CONCAT('!','')";
    run(
        &engine,
        &mut session,
        &format!("CREATE VIEW typed_escape AS {body}"),
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT id FROM typed_escape"),
        vec![vec![Value::Int(1)]]
    );
    let view = engine
        .database()
        .view_definition("test", "typed_escape")
        .unwrap();
    assert_eq!(view.sql, body);
    run(
        &engine,
        &mut session,
        &format!("ALTER VIEW typed_escape AS {body}"),
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT id FROM typed_escape"),
        vec![vec![Value::Int(1)]]
    );
    run(
        &engine,
        &mut session,
        &format!(
            "PREPARE view_stmt FROM '{}'",
            format!("ALTER VIEW typed_escape AS {body}")
                .replace('\\', "\\\\")
                .replace('\'', "''")
        ),
    )
    .unwrap();
    run(&engine, &mut session, "EXECUTE view_stmt").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT id FROM typed_escape"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn quality_cte_annotations_are_rejected_before_execution_or_catalog_write() {
    let (_dir, engine, mut session) = fixture();
    let query = "WITH c AS(SELECT 1 n) FROM ignored SELECT n FROM c";
    for sql in [
        query.to_owned(),
        format!("SELECT * FROM ({query}) d"),
        format!("CREATE VIEW bad_annotation AS {query}"),
    ] {
        assert_eq!(
            run(&engine, &mut session, &sql).unwrap_err().code(),
            1235,
            "{sql}"
        );
    }
    assert!(engine
        .database()
        .view_definition("test", "bad_annotation")
        .is_none());
}

#[test]
fn quality_query_only_slots_reject_with_dml_before_side_effects() {
    let (_dir, engine, mut session) = fixture();
    for sql in [
        "DECLARE bad CURSOR FOR WITH q AS(SELECT 1 n) UPDATE t SET id=8",
        "DECLARE bad CURSOR FOR WITH q AS(SELECT 1 n) DELETE FROM t",
        "CREATE TABLE bad AS WITH q AS(SELECT 1 n) UPDATE t SET id=8",
        "CREATE TEMPORARY TABLE bad AS WITH q AS(SELECT 1 n) UPDATE t SET id=8",
    ] {
        assert_eq!(
            run(&engine, &mut session, sql).unwrap_err().code(),
            1064,
            "{sql}"
        );
        assert_eq!(
            rows(&engine, &mut session, "SELECT id FROM t ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        assert!(!crate::engine::cursor_registry::named_snapshot()
            .iter()
            .any(|cursor| cursor.connection_id == session.connection_id && cursor.name == "bad"));
        assert!(!engine.database().relation_name_exists("test.bad"));
        assert!(session.temp_tables.is_empty());
    }
}

#[test]
fn quality_query_only_ctas_and_cursor_keep_ctes_setops_and_parameters() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "PREPARE ctas_query FROM 'CREATE TABLE copied AS WITH q AS(SELECT id FROM t UNION ALL SELECT ?) SELECT id+? AS n FROM q'").unwrap();
    run(&engine, &mut session, "SET @a=3,@b=10").unwrap();
    run(&engine, &mut session, "EXECUTE ctas_query USING @a,@b").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT n FROM copied ORDER BY n"),
        vec![
            vec![Value::Int(11)],
            vec![Value::Int(12)],
            vec![Value::Int(13)]
        ]
    );
    run(&engine, &mut session, "DECLARE typed_cursor CURSOR FOR WITH q AS(SELECT n FROM copied) SELECT n FROM q UNION ALL SELECT 14 ORDER BY n").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "FETCH 2 FROM typed_cursor"),
        vec![vec![Value::Int(11)], vec![Value::Int(12)]]
    );
    assert_eq!(
        rows(&engine, &mut session, "FETCH 5 FROM typed_cursor"),
        vec![vec![Value::Int(13)], vec![Value::Int(14)]]
    );
    run(&engine, &mut session, "CLOSE typed_cursor").unwrap();
}

#[test]
fn quality_select_into_file_rejects_nonquery_before_data_or_file_changes() {
    let (dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET @x=7").unwrap();
    for kind in ["OUTFILE", "DUMPFILE"] {
        let path = dir.path().join(format!("bad-{kind}.txt"));
        let sql = format!(
            "WITH q AS(SELECT 1 n) UPDATE t SET id=8 INTO {kind} '{}'",
            crate::util::file::sql_path(&path)
        );
        assert_eq!(run(&engine, &mut session, &sql).unwrap_err().code(), 1064);
        assert!(!path.exists());
        assert_eq!(
            rows(&engine, &mut session, "SELECT id,@x FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(7)],
                vec![Value::Int(2), Value::Int(7)]
            ]
        );
    }
}

#[test]
fn quality_select_into_query_nodes_preserve_ctes_parameters_and_exports() {
    let (dir, engine, mut session) = fixture();
    run(&engine,&mut session,"PREPARE into_query FROM 'WITH q AS(SELECT id FROM t UNION ALL SELECT ?) SELECT SUM(id) INTO @total FROM q'").unwrap();
    run(&engine, &mut session, "SET @p=3").unwrap();
    run(&engine, &mut session, "EXECUTE into_query USING @p").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT @total"),
        vec![vec![Value::DecimalCompact(6, 0)]]
    );
    let path = dir.path().join("typed-query.tsv");
    run(&engine,&mut session,&format!("WITH q AS(SELECT id FROM t) SELECT id FROM q UNION ALL SELECT 3 ORDER BY id INTO OUTFILE '{}'",crate::util::file::sql_path(&path))).unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "1\n2\n3\n");
}

#[test]
fn quality_select_into_cardinality_stops_streams_after_two_rows() {
    use crate::engine::api::{ResultCursor, StreamingResultSet};
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct Rows {
        count: Arc<AtomicUsize>,
        limit: usize,
    }
    impl ResultCursor for Rows {
        fn next_row(&mut self) -> Result<Option<Vec<Value>>, QueryError> {
            let n = self.count.fetch_add(1, Ordering::Relaxed);
            assert!(n < 3, "INTO must not drain a multi-row result");
            Ok((n < self.limit).then(|| vec![Value::Int(n as i64)]))
        }
    }
    for (limit, expected_pulls) in [(0, 1), (1, 2), (1_000_000, 2)] {
        let count = Arc::new(AtomicUsize::new(0));
        let response = QueryResponse::StreamingResultSet(StreamingResultSet {
            columns: vec![],
            found_rows: None,
            cursor: Box::new(Rows {
                count: count.clone(),
                limit,
            }),
        });
        let result = crate::engine::executor::dispatch::take_single_into_row(response);
        if limit > 1 {
            assert_eq!(result.unwrap_err().code(), 1172);
        } else {
            assert_eq!(result.unwrap(), (limit == 1).then(|| vec![Value::Int(0)]));
        }
        assert_eq!(count.load(Ordering::Relaxed), expected_pulls);
    }
    let response = QueryResponse::SharedResultSet(crate::engine::api::SharedResultSet {
        columns: Arc::from([]),
        rows: vec![vec![Value::Int(5)]],
    });
    assert_eq!(
        crate::engine::executor::dispatch::take_single_into_row(response).unwrap(),
        Some(vec![Value::Int(5)])
    );
}

#[test]
fn quality_select_into_targets_keep_quotes_comments_and_query_grammar() {
    let (_dir, engine, mut session) = fixture();
    for sql in [
        "SELECT 9 INTO /*between*/ @'v space'",
        "SELECT 9 INTO @`v space`",
        "SELECT 9 INTO @\"v space\"",
        "WITH q AS(SELECT 9 n) SELECT n INTO @'v space' FROM q",
    ] {
        run(&engine, &mut session, sql).unwrap();
        assert_eq!(
            rows(&engine, &mut session, "SELECT @'v space'"),
            vec![vec![Value::Int(9)]],
            "{sql}"
        );
    }
    run(&engine, &mut session, "SET @x=7").unwrap();
    for sql in [
        "SELECT 1 INTO @x+1",
        "SELECT 1 INTO @x-1",
        "SELECT 1 INTO @x,@",
        "SELECT 1 INTO @@x",
    ] {
        assert_eq!(
            run(&engine, &mut session, sql).unwrap_err().code(),
            1064,
            "{sql}"
        );
        assert_eq!(
            rows(&engine, &mut session, "SELECT @x"),
            vec![vec![Value::Int(7)]]
        );
    }
}

#[test]
fn quality_insert_query_set_sources_and_self_reference() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE TABLE dst (id INT PRIMARY KEY)",
    )
    .unwrap();
    for sql in [
        "INSERT INTO dst SELECT id FROM t UNION ALL SELECT id+10 FROM t",
        "INSERT INTO dst SELECT id+20 FROM t UNION SELECT id+20 FROM t",
        "INSERT INTO dst SELECT id+30 FROM t INTERSECT SELECT 31",
        "INSERT INTO dst SELECT id+40 FROM t EXCEPT SELECT 42",
    ] {
        run(&engine, &mut session, sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT id FROM dst ORDER BY id"),
        [1, 2, 11, 12, 21, 22, 31, 41]
            .into_iter()
            .map(|id| vec![Value::Int(id)])
            .collect::<Vec<_>>()
    );
    run(
        &engine,
        &mut session,
        "INSERT INTO dst SELECT id+100 FROM dst UNION ALL SELECT id+200 FROM dst",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM dst"),
        vec![vec![Value::Int(24)]]
    );
}

#[test]
fn quality_insert_query_odku_aliases_and_explain() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE TABLE dst (id INT PRIMARY KEY, n INT)",
    )
    .unwrap();
    run(&engine, &mut session, "INSERT INTO dst VALUES (1,0),(2,0)").unwrap();
    run(&engine, &mut session,
        "INSERT INTO dst SELECT 1 AS k, 10 AS amount UNION ALL SELECT 2,20 ON DUPLICATE KEY UPDATE n=amount").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT n FROM dst ORDER BY id"),
        vec![vec![Value::Int(10)], vec![Value::Int(20)]]
    );
    for sql in [
        "EXPLAIN FORMAT=TREE INSERT INTO dst SELECT id, 1 FROM t UNION ALL SELECT id+10,2 FROM t",
        "INSERT INTO dst WITH c AS (SELECT id FROM t) SELECT id+20,3 FROM c UNION SELECT id+30,4 FROM c",
    ] {
        run(&engine, &mut session, sql).unwrap_or_else(|err| panic!("{sql}: {err:?}"));
    }
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM dst"),
        vec![vec![Value::Int(6)]]
    );
}

#[test]
fn quality_catalog_rejects_query_expressions_before_create() {
    let (_dir, engine, mut session) = fixture();
    // A real column named `expr` must not make a display placeholder valid.
    for sql in [
        "CREATE TABLE bad_generated (expr INT, g INT AS ((SELECT 7)))",
        "CREATE TABLE bad_default (expr INT DEFAULT ((SELECT 7)))",
    ] {
        assert!(run(&engine, &mut session, sql).is_err(), "{sql}");
    }
    for table in ["bad_generated", "bad_default"] {
        assert!(run(&engine, &mut session, &format!("SELECT * FROM {table}")).is_err());
    }
}

#[test]
fn quality_read_only_fast_path_checks_nested_query_locks() {
    for sql in [
        "SELECT * FROM (WITH c AS (SELECT id FROM t FOR UPDATE) SELECT id FROM c) d",
        "SELECT 1 WHERE 1 = ANY (SELECT id FROM t FOR UPDATE)",
        "SELECT 1 WHERE 1 = ALL (SELECT id FROM t FOR SHARE)",
    ] {
        let Statement::Select(select) = crate::parser::parse_statement(sql).unwrap() else {
            panic!("SELECT expected")
        };
        assert!(super::super::tx::select_has_locking_read(&select), "{sql}");
        assert!(
            !super::super::tx::can_use_read_only_select(&SessionContext::default(), &select),
            "{sql}"
        );
    }
}

#[test]
fn quality_partition_quoted_all_does_not_truncate_other_partitions() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "CREATE TABLE token_parts(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION `ALL` VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))").unwrap();
    run(
        &engine,
        &mut session,
        "INSERT INTO token_parts VALUES (1),(11)",
    )
    .unwrap();
    for (mode, name) in [("", "`ALL`"), ("ANSI_QUOTES", "\"ALL\"")] {
        run(&engine, &mut session, &format!("SET sql_mode='{mode}'")).unwrap();
        run(
            &engine,
            &mut session,
            &format!("ALTER TABLE token_parts TRUNCATE PARTITION {name}"),
        )
        .unwrap();
        assert_eq!(
            rows(
                &engine,
                &mut session,
                "SELECT id FROM token_parts ORDER BY id"
            ),
            vec![vec![Value::Int(11)]]
        );
        run(&engine, &mut session, "INSERT INTO token_parts VALUES (1)").unwrap();
    }
    assert!(run(
        &engine,
        &mut session,
        "ALTER TABLE token_parts DROP PARTITION p1 /* broken"
    )
    .is_err());
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT id FROM token_parts ORDER BY id"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(11)]]
    );
    run(
        &engine,
        &mut session,
        "ALTER TABLE token_parts TRUNCATE PARTITION ALL",
    )
    .unwrap();
    assert!(rows(&engine, &mut session, "SELECT id FROM token_parts").is_empty());
}

#[test]
fn quality_native_json_value_execution_view_and_parameters() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            r"SELECT 'a\' AS s, JSON_VALUE /* call */ ('{}', '$.x' RETURNING /* ) */ SIGNED DEFAULT 3 ON EMPTY) AS v"
        ),
        vec![vec![Value::String("a\\".into()), Value::Int(3)]]
    );
    assert_eq!(
        rows(
            &engine,
            &mut session,
            r#"SELECT JSON_VALUE(JSON_VALUE('{"x":{}}', '$.x' RETURNING JSON), '$.y' RETURNING SIGNED DEFAULT 3 ON EMPTY)"#
        ),
        vec![vec![Value::Null]]
    );
    run(&engine, &mut session, r#"CREATE VIEW native_json AS SELECT JSON_VALUE(JSON_VALUE('{"x":{}}', '$.x' RETURNING CHAR), '$.y' RETURNING SIGNED DEFAULT 3 ON EMPTY) AS n"#).unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT n FROM native_json"),
        vec![vec![Value::Int(3)]]
    );
    run(&engine, &mut session, "PREPARE native_json_stmt FROM 'SELECT JSON_VALUE(?, ''$.x'' RETURNING SIGNED DEFAULT ? ON EMPTY)'").unwrap();
    run(&engine, &mut session, "SET @doc='{}', @fallback=8").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "EXECUTE native_json_stmt USING @doc, @fallback"
        ),
        vec![vec![Value::Int(8)]]
    );
    assert_eq!(
        rows(&engine, &mut session, "SELECT JSON_VALUE('{}', NULL)"),
        vec![vec![Value::Null]]
    );
}

#[test]
fn quality_sounds_like_preserves_complete_operands() {
    let (_dir, engine, mut session) = fixture();
    for (actual, expected) in [
        ("SELECT 1+2 SOUNDS LIKE 3", "SELECT SOUNDEX(1+2)=SOUNDEX(3)"),
        ("SELECT 1 SOUNDS LIKE 2+3", "SELECT SOUNDEX(1)=SOUNDEX(2+3)"),
        (
            "SELECT NOT 'Robert' SOUNDS LIKE 'Rupert'",
            "SELECT NOT (SOUNDEX('Robert')=SOUNDEX('Rupert'))",
        ),
    ] {
        assert_eq!(
            rows(&engine, &mut session, actual),
            rows(&engine, &mut session, expected),
            "{actual}"
        );
    }
}

#[test]
fn quality_sounds_like_respects_comments_and_sql_mode() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            r"SELECT 'a\', 'Robert' SOUNDS /* ) */ LIKE 'Rupert'"
        ),
        vec![vec![Value::String("a\\".into()), Value::Int(1)]]
    );
}

#[test]
fn quality_native_predicates_and_high_prefix_match_explicit_grouping() {
    let (_dir, engine, mut session) = fixture();
    for (actual, expected) in [
        (
            "SELECT 2=1 SOUNDS LIKE 1",
            "SELECT 2=(SOUNDEX(1)=SOUNDEX(1))",
        ),
        (
            "SELECT 0 BETWEEN 0 AND 2 SOUNDS LIKE 2",
            "SELECT 0 BETWEEN 0 AND (SOUNDEX(2)=SOUNDEX(2))",
        ),
        (
            "SELECT !~'abc' SOUNDS LIKE 1",
            "SELECT SOUNDEX(!~'abc')=SOUNDEX(1)",
        ),
        ("SELECT -1^2", "SELECT (-1)^2"),
        ("SELECT ~~1*2", "SELECT (~(~1))*2"),
        ("SELECT !~~1^2", "SELECT (!(~(~1)))^2"),
        ("SELECT !~1^2", "SELECT (!(~1))^2"),
        ("SELECT ~!1-1", "SELECT (~(!1))-1"),
        (
            "SELECT BINARY 'Robert' SOUNDS LIKE 'Rupert'",
            "SELECT SOUNDEX(BINARY 'Robert')=SOUNDEX('Rupert')",
        ),
        (
            "SELECT CASE WHEN 1 THEN 'Robert' ELSE 'Smith' END SOUNDS LIKE 'Rupert'",
            "SELECT SOUNDEX(CASE WHEN 1 THEN 'Robert' ELSE 'Smith' END)=SOUNDEX('Rupert')",
        ),
    ] {
        assert_eq!(
            rows(&engine, &mut session, actual),
            rows(&engine, &mut session, expected),
            "{actual}"
        );
    }
    for sql in ["SELECT ~!1+1", "SELECT (~(!1))+1"] {
        assert_eq!(
            run(&engine, &mut session, sql).unwrap_err().code(),
            1690,
            "{sql}"
        );
    }
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(&engine, &mut session, r"SELECT 'a\', !1, !!1, !!!1, ! !1"),
        vec![vec![
            Value::String("a\\".into()),
            Value::Int(0),
            Value::Int(0),
            Value::Int(1),
            Value::Int(1)
        ]]
    );
}

#[test]
fn quality_binary_prefix_has_native_expression_boundaries() {
    let (_dir, engine, mut session) = fixture();
    for (actual, expected) in [
        (
            "SELECT BINARY CASE WHEN 1 THEN 'A' ELSE 'b' END",
            "SELECT CAST(CASE WHEN 1 THEN 'A' ELSE 'b' END AS BINARY)",
        ),
        (
            "SELECT BINARY /* operand */ 'A' = 'a'",
            "SELECT CAST('A' AS BINARY) = 'a'",
        ),
        (
            "SELECT BINARY BINARY 'A'",
            "SELECT CAST(CAST('A' AS BINARY) AS BINARY)",
        ),
        ("SELECT BINARY(16)", "SELECT CAST(16 AS BINARY)"),
    ] {
        assert_eq!(
            rows(&engine, &mut session, actual),
            rows(&engine, &mut session, expected),
            "{actual}"
        );
    }
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(&engine, &mut session, r"SELECT 'a\', BINARY 'A'='a'"),
        vec![vec![Value::String("a\\".into()), Value::Int(0)]]
    );
}

#[test]
fn quality_cast_clauses_use_token_boundaries() {
    let (_dir, engine, mut session) = fixture();
    for (actual, expected) in [
        (
            "SELECT CAST(12 AS SIGNED /* alias */ INTEGER)",
            "SELECT CAST(12 AS SIGNED)",
        ),
        (
            "SELECT CAST(12 AS UNSIGNED /* alias */ INT)",
            "SELECT CAST(12 AS UNSIGNED)",
        ),
        (
            "SELECT CAST('abc' AS CHAR(2) CHARACTER /* charset */ SET utf8mb4)",
            "SELECT CONVERT_USING(CAST('abc' AS CHAR(2)), 'utf8mb4')",
        ),
    ] {
        assert_eq!(
            rows(&engine, &mut session, actual),
            rows(&engine, &mut session, expected),
            "{actual}"
        );
    }
    let spaced = "SELECT CAST /* call */ ('abc' AS CHAR(2) CHARACTER SET utf8mb4)";
    assert!(run(&engine, &mut session, spaced).is_err());
    run(&engine, &mut session, "SET sql_mode='IGNORE_SPACE'").unwrap();
    assert_eq!(
        rows(&engine, &mut session, spaced),
        rows(
            &engine,
            &mut session,
            "SELECT CONVERT_USING(CAST('abc' AS CHAR(2)), 'utf8mb4')"
        )
    );
}

#[test]
fn quality_cast_collation_and_unicode_multi_value_index_execute() {
    let (_dir, engine, mut session) = fixture();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            r#"SELECT CAST('A' AS CHAR) COLLATE utf8mb4_bin = 'a', JSON_VALUE('{"x":"A"}', '$.x') COLLATE utf8mb4_bin = 'a'"#
        ),
        vec![vec![Value::Int(0), Value::Int(0)]]
    );
    run(
        &engine,
        &mut session,
        "CREATE TABLE 数据 (id INT PRIMARY KEY, j JSON, INDEX 多值 ((CAST(j AS UNSIGNED ARRAY))))",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        "INSERT INTO 数据 VALUES (1,'[1,2]'),(2,'[3]')",
    )
    .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT id FROM 数据 WHERE 2 MEMBER OF (j)"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn quality_char_using_native_boundaries() {
    let (_dir, engine, mut session) = fixture();
    for (sql, expected) in [
        (
            "SELECT CHAR(65 USING /* charset */ ascii)",
            "SELECT CHAR_USING(CHAR(65), 'ascii')",
        ),
        (
            "SELECT CHAR(ASCII(CHAR(65 USING ascii)) USING utf8mb4)",
            "SELECT CHAR_USING(CHAR(65), 'utf8mb4')",
        ),
    ] {
        assert_eq!(
            rows(&engine, &mut session, sql),
            rows(&engine, &mut session, expected),
            "{sql}"
        );
    }
}
#[test]
fn quality_weight_string_native_boundaries() {
    let (_dir, engine, mut session) = fixture();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT HEX(WEIGHT_STRING('ab' AS /* type */ CHAR(3)))"
        ),
        rows(
            &engine,
            &mut session,
            "SELECT HEX(WEIGHT_STRING_TYPED('ab', 'CHAR', 3))"
        )
    );
}
#[test]
fn quality_extract_native_boundaries() {
    let (_dir, engine, mut session) = fixture();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT EXTRACT(DAY_HOUR /* unit */ FROM '2026-10-03 12:34:56')"
        ),
        rows(
            &engine,
            &mut session,
            "SELECT EXTRACT_COMPOUND('DAY_HOUR', '2026-10-03 12:34:56')"
        )
    );
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            r"SELECT 'a\', EXTRACT(YEAR_MONTH FROM '2026-10-03')"
        ),
        vec![vec![Value::String("a\\".into()), Value::Int(202610)]]
    );
}

#[test]
fn quality_scalar_clauses_nest_and_bind_parameters() {
    let (_dir, engine, mut session) = fixture();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT HEX(WEIGHT_STRING(WEIGHT_STRING('ab' AS CHAR(3)) AS BINARY(4)))"
        ),
        rows(
            &engine,
            &mut session,
            "SELECT HEX(WEIGHT_STRING_TYPED(WEIGHT_STRING_TYPED('ab', 'CHAR', 3), 'BINARY', 4))"
        )
    );
    run(&engine, &mut session, "PREPARE scalar_clause FROM 'SELECT CHAR(? USING ascii), HEX(WEIGHT_STRING(? AS BINARY(2))), EXTRACT(YEAR_MONTH FROM ?)'").unwrap();
    run(&engine, &mut session, "SET @n=65, @s='ab', @d='2026-10-03'").unwrap();
    assert_eq!(rows(&engine, &mut session, "EXECUTE scalar_clause USING @n, @s, @d"), rows(&engine, &mut session, "SELECT CHAR_USING(CHAR(65), 'ascii'), HEX(WEIGHT_STRING_TYPED('ab','BINARY',2)), EXTRACT_COMPOUND('YEAR_MONTH','2026-10-03')"));
}

#[test]
fn quality_interval_native_units_and_expression_boundaries() {
    let (_dir, engine, mut session) = fixture();
    for (sql, expected) in [
        (
            "SELECT DATE_ADD('2026-10-03', INTERVAL /* value */ 1 /* unit */ DAY)",
            "SELECT DATE_ADD('2026-10-03', __INTERVAL(1,'DAY'))",
        ),
        (
            "SELECT DATE_ADD('2026-10-03', INTERVAL (1)+2 DAY)",
            "SELECT DATE_ADD('2026-10-03', __INTERVAL(3,'DAY'))",
        ),
        (
            "SELECT DATE_ADD('2026-10-03', INTERVAL ABS(-2) DAY)",
            "SELECT DATE_ADD('2026-10-03', __INTERVAL(2,'DAY'))",
        ),
    ] {
        assert_eq!(
            rows(&engine, &mut session, sql),
            rows(&engine, &mut session, expected),
            "{sql}"
        );
    }
}
#[test]
fn quality_interval_native_function_comments_and_nesting() {
    let (_dir, engine, mut session) = fixture();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT INTERVAL /* call */ (3,1,2,4)+10"
        ),
        rows(&engine, &mut session, "SELECT __INTERVAL_LIST(3,1,2,4)+10")
    );
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT DATE_ADD('2026-10-03', INTERVAL (SELECT INTERVAL(3,1,2,4)) DAY)"
        ),
        rows(
            &engine,
            &mut session,
            "SELECT DATE_ADD('2026-10-03', __INTERVAL(2,'DAY'))"
        )
    );
}

#[test]
fn quality_interval_native_modes_and_parameters() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "PREPARE interval_stmt FROM 'SELECT ? + INTERVAL (?+1) DAY'",
    )
    .unwrap();
    run(&engine, &mut session, "SET @d='2026-10-03', @n=1").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "EXECUTE interval_stmt USING @d,@n"),
        rows(
            &engine,
            &mut session,
            "SELECT DATE_ADD('2026-10-03', __INTERVAL(2,'DAY'))"
        )
    );
    assert_eq!(rows(&engine, &mut session, "SELECT DATE_ADD('2026-10-03', INTERVAL CASE WHEN 1 THEN '1 02:03' ELSE '0 00:00' END DAY_MINUTE)"), rows(&engine, &mut session, "SELECT DATE_ADD('2026-10-03', __INTERVAL('1 02:03','DAY_MINUTE'))"));
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            r"SELECT 'a\', DATE_ADD('2026-10-03', INTERVAL '1:02' HOUR_MINUTE), INTERVAL(3,1,2,4)"
        ),
        rows(
            &engine,
            &mut session,
            r"SELECT 'a\', DATE_ADD('2026-10-03', __INTERVAL('1:02','HOUR_MINUTE')), __INTERVAL_LIST(3,1,2,4)"
        )
    );
}

#[test]
fn quality_prepared_parameters_survive_date_operand_reordering() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "PREPARE source_order FROM 'SELECT INTERVAL ? DAY + ?'",
    )
    .unwrap();
    run(&engine, &mut session, "SET @amount=2, @date='2026-10-03'").unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "EXECUTE source_order USING @amount,@date"
        ),
        rows(
            &engine,
            &mut session,
            "SELECT DATE_ADD('2026-10-03', INTERVAL 2 DAY)"
        )
    );
}

#[test]
fn quality_parameter_identity_covers_dynamic_like_and_manual_tails() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "PREPARE parameter_mix FROM 'SELECT INTERVAL ? DAY + ?, ? LIKE ? ESCAPE ?, ?'",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        "SET @n=2, @d='2026-10-03', @s='a_b', @p='a!_b', @esc='!', @last=17",
    )
    .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "EXECUTE parameter_mix USING @n,@d,@s,@p,@esc,@last"
        ),
        vec![vec![
            Value::String("2026-10-05".into()),
            Value::Int(1),
            Value::Int(17)
        ]]
    );
    run(
        &engine,
        &mut session,
        "PREPARE update_tail FROM 'UPDATE t SET s=? WHERE id>? ORDER BY id+? LIMIT ?'",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        "SET @value='changed', @minimum=0, @order=0, @count=1",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        "EXECUTE update_tail USING @value,@minimum,@order,@count",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT s FROM t ORDER BY id"),
        vec![
            vec![Value::String("changed".into())],
            vec![Value::String("ab".into())]
        ]
    );
}

#[test]
fn quality_regexp_native_null_and_mode_boundaries() {
    let (_dir, engine, mut session) = fixture();
    for sql in [
        "SELECT 'abc' REGEXP /* pattern */ NULL",
        "SELECT 'abc' NOT RLIKE /* pattern */ NULL",
    ] {
        assert_eq!(
            rows(&engine, &mut session, sql),
            vec![vec![Value::Null]],
            "{sql}"
        );
    }
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(&engine, &mut session, r"SELECT 'a\', 'abc' REGEXP NULL"),
        vec![vec![Value::String("a\\".into()), Value::Null]]
    );
}

#[test]
fn quality_trim_native_direction_and_comment_boundaries() {
    let (_dir, engine, mut session) = fixture();
    assert_eq!(rows(&engine, &mut session, "SELECT TRIM(LEADING /* direction */ FROM ' a '), TRIM(TRAILING /* direction */ FROM ' a ')"), vec![vec![Value::String("a ".into()), Value::String(" a".into())]]);
    run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
    assert_eq!(
        rows(&engine, &mut session, r"SELECT 'a\', TRIM(BOTH FROM ' b ')"),
        vec![vec![Value::String("a\\".into()), Value::String("b".into())]]
    );
}

#[test]
fn quality_trim_regexp_native_prepared_and_view() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "PREPARE text_ops FROM 'SELECT TRIM(? FROM ?), TRIM(LEADING FROM ?), ? NOT REGEXP ?'",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        "SET @rem='x', @s='xabcx', @padded=' a ', @subject='abc', @pattern=NULL",
    )
    .unwrap();
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "EXECUTE text_ops USING @rem,@s,@padded,@subject,@pattern"
        ),
        rows(
            &engine,
            &mut session,
            "SELECT TRIM('xabcx','x'), LTRIM(' a '), NULL"
        )
    );
    run(&engine, &mut session, "CREATE VIEW native_text_ops AS SELECT TRIM(BOTH /* default */ FROM ' b ') AS s, 'abc' NOT RLIKE NULL AS r").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT s,r FROM native_text_ops"),
        vec![vec![Value::String("b".into()), Value::Null]]
    );
}

#[test]
fn quality_functional_index_literal_semantics_survive_modes_and_reload() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        run(&engine, &mut session, "SET sql_mode='NO_BACKSLASH_ESCAPES'").unwrap();
        run(
            &engine,
            &mut session,
            r"CREATE TABLE idx_literal (s VARCHAR(40), UNIQUE KEY uq ((REPLACE(s, '\n', ''))))",
        )
        .unwrap();
        run(
            &engine,
            &mut session,
            "INSERT INTO idx_literal VALUES ('a')",
        )
        .unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    let mut session = SessionContext::default();
    run(&engine, &mut session, "SET sql_mode=''").unwrap();
    let err = run(
        &engine,
        &mut session,
        r"INSERT INTO idx_literal VALUES ('a\\n')",
    )
    .unwrap_err();
    assert_eq!(err.code(), 1062);
    assert_eq!(
        rows(&engine, &mut session, "SELECT COUNT(*) FROM idx_literal"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn quality_concat_operator_preserves_binary_bytes() {
    let (_dir, engine, mut session) = fixture();
    run(&engine, &mut session, "SET sql_mode='PIPES_AS_CONCAT'").unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT HEX(X'FF'||X'FE')"),
        rows(&engine, &mut session, "SELECT HEX(CONCAT(X'FF',X'FE'))")
    );
}

#[test]
fn quality_show_create_keeps_multi_value_index_kind() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE TABLE idx_array (j JSON, KEY mv ((CAST(j AS UNSIGNED ARRAY))))",
    )
    .unwrap();
    let definition =
        rows(&engine, &mut session, "SHOW CREATE TABLE idx_array")[0][1].to_display_string();
    assert!(
        definition.to_ascii_uppercase().contains("UNSIGNED ARRAY"),
        "{definition}"
    );
    run(
        &engine,
        &mut session,
        &definition.replacen("`idx_array`", "`idx_array_copy`", 1),
    )
    .unwrap();
    let schema = engine.database().table("test.idx_array_copy").unwrap();
    assert!(schema
        .indexes
        .iter()
        .any(|index| index.name == "mv" && index.has_multi_valued_parts()));
}

#[test]
fn quality_insert_set_writes_quoted_dotted_column() {
    let (_dir, engine, mut session) = fixture();
    run(
        &engine,
        &mut session,
        "CREATE TABLE dotted_target (`a.b` INT, b INT DEFAULT 9)",
    )
    .unwrap();
    run(
        &engine,
        &mut session,
        "INSERT INTO dotted_target SET `a.b`=7",
    )
    .unwrap();
    assert_eq!(
        rows(&engine, &mut session, "SELECT `a.b`, b FROM dotted_target"),
        vec![vec![Value::Int(7), Value::Int(9)]]
    );
}

#[test]
fn quality_unsupported_dml_table_modifiers_do_not_modify_rows() {
    let (_dir, engine, mut session) = fixture();
    let before = rows(&engine, &mut session, "SELECT id, s FROM t ORDER BY id");
    for sql in [
        "UPDATE t WITH ORDINALITY SET s='changed'",
        "DELETE FROM t WITH ORDINALITY",
        "UPDATE t TABLESAMPLE BERNOULLI(10) SET s='changed'",
        "DELETE FROM t TABLESAMPLE BERNOULLI(10)",
    ] {
        assert!(run(&engine, &mut session, sql).is_err(), "{sql}");
        assert_eq!(
            rows(&engine, &mut session, "SELECT id, s FROM t ORDER BY id"),
            before,
            "{sql}"
        );
    }
}

#[test]
fn quality_join_using_quoted_dot_executes() {
    let (_dir, engine, mut session) = fixture();
    for sql in [
        "CREATE TABLE dotted_left (`a.b` INT, v INT)",
        "CREATE TABLE dotted_right (`a.b` INT, w INT)",
        "INSERT INTO dotted_left VALUES (1,10),(2,20)",
        "INSERT INTO dotted_right VALUES (1,30),(3,40)",
    ] {
        run(&engine, &mut session, sql).unwrap();
    }
    assert_eq!(
        rows(
            &engine,
            &mut session,
            "SELECT v,w FROM dotted_left JOIN dotted_right USING (`a.b`)"
        ),
        vec![vec![Value::Int(10), Value::Int(30)]]
    );
}
