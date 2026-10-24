//! Cross-session SQL plan-cache regression coverage.
//!
//! - A1: `(table_key, id, schema_version)` deps validate in O(deps) and the
//!   `id` component stops a DROP + CREATE that reuses a name from reviving a
//!   stale plan.
//! - A2: trigger DDL invalidates only the target table's cached plans/DML
//!   memos (via its `schema_version` bump), leaving hot plans on unrelated
//!   tables cached — and a DML memo compiled before the trigger existed is
//!   recompiled so the new trigger fires.
//! - B2: session semantics that can affect planning or expression results are
//!   part of the cache key, so plans never cross incompatible sessions.

use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected result set for {sql:?}, got {other:?}"),
    }
}

fn rows_session(
    engine: &StorageQueryEngine,
    session: &mut SessionContext,
    sql: &str,
) -> Vec<Vec<Value>> {
    match engine.execute_with_session(session, sql).unwrap() {
        QueryResponse::ResultSet(rs) => rs.rows,
        other => panic!("expected result set for {sql:?}, got {other:?}"),
    }
}

#[test]
fn exact_source_parse_cache_survives_compiled_plan_invalidation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let sql = "SELECT 42 AS Answer";

    assert_eq!(rows(&engine, sql), vec![vec![Value::Int(42)]]);
    let cold = engine.statement_cache().stats();
    assert_eq!(rows(&engine, sql), vec![vec![Value::Int(42)]]);
    let warm = engine.statement_cache().stats();
    assert_eq!(warm.hits, cold.hits + 1);

    QueryEngine::invalidate_statement_cache(&engine);
    assert_eq!(rows(&engine, sql), vec![vec![Value::Int(42)]]);
    let after_plan_invalidation = engine.statement_cache().stats();
    assert_eq!(after_plan_invalidation.hits, warm.hits + 1);
    assert_eq!(after_plan_invalidation.misses, warm.misses);
}

#[test]
fn plan_cache_switch_disables_compiled_plans_but_keeps_parse_cache() {
    with_test_env(&[("MYDB_PLAN_CACHE", Some("off"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        seed_join_tables(&engine);

        let plan_before = engine.plan_cache().stats();
        let parse_before = engine.statement_cache().stats();
        assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
        assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
        let plan_after = engine.plan_cache().stats();
        let parse_after = engine.statement_cache().stats();

        assert_eq!(plan_after.hits, plan_before.hits);
        assert_eq!(plan_after.misses, plan_before.misses);
        assert_eq!(plan_after.size, 0);
        assert!(
            parse_after.hits > parse_before.hits,
            "the execution-plan switch must not disable the independent AST cache"
        );

        let mut session = SessionContext::default();
        let select = engine.prepare_cached(&mut session, JOIN_SQL, 0).unwrap();
        let mut select_state = crate::engine::api::PreparedSelectMemoState::default();
        let compiles_before = engine.database().storage_status().prepared_plan_compiles;
        let first = QueryEngine::execute_prepared_select(
            &engine,
            &mut session,
            &mut select_state,
            &select,
            JOIN_SQL,
            &[],
        )
        .expect("plan-cache-off prepared SELECT uses the planless template")
        .unwrap();
        assert_eq!(rows_of_response(first)[0][0], Value::Int(10));
        assert!(matches!(
            select_state,
            crate::engine::api::PreparedSelectMemoState::Uncached(_)
        ));
        let second = QueryEngine::execute_prepared_select(
            &engine,
            &mut session,
            &mut select_state,
            &select,
            JOIN_SQL,
            &[],
        )
        .expect("planless template remains eligible")
        .unwrap();
        assert_eq!(rows_of_response(second)[0][0], Value::Int(10));
        assert_eq!(
            engine.database().storage_status().prepared_plan_compiles,
            compiles_before + 2,
            "physical planning must still run for every EXECUTE"
        );
        assert_eq!(
            engine.plan_cache().stats().size,
            0,
            "planless prepared templates must not populate the physical cache"
        );

        let dml_sql = "UPDATE x SET v = v + 1 WHERE id = 1";
        let dml = Arc::new(parse_statement(dml_sql).unwrap());
        let mut dml_state = crate::engine::api::PreparedDmlState::default();
        assert!(QueryEngine::execute_prepared_dml(
            &engine,
            &mut session,
            &mut dml_state,
            &dml,
            dml_sql,
            &[],
        )
        .is_none());
        assert!(matches!(
            dml_state,
            crate::engine::api::PreparedDmlState::Unknown
        ));
    });
}

fn rows_of_response(response: QueryResponse) -> Vec<Vec<Value>> {
    match response.materialize().expect("materialize result stream") {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

// Keep a join query in the cross-session cache tests so dependency validation
// covers more than one table. Single-table SELECTs use the same compiled-plan
// cache as joins.
const JOIN_SQL: &str = "SELECT x.v FROM x JOIN y ON x.id = y.id WHERE x.id = 1";

fn seed_join_tables(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE x (id INT PRIMARY KEY, v INT, u BIGINT)")
        .unwrap();
    engine
        .execute("CREATE TABLE y (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO x VALUES (1, 10, 0)").unwrap();
    engine.execute("INSERT INTO y VALUES (1)").unwrap();
}

#[test]
fn a2_unrelated_trigger_ddl_preserves_hot_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE audit (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(16))")
        .unwrap();

    // Warm the join plan into the cross-session plan cache (miss then hit).
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));

    // Create a trigger on the UNRELATED table b.
    engine
        .execute(
            "CREATE TRIGGER trg AFTER INSERT ON b FOR EACH ROW \
             INSERT INTO audit(note) VALUES ('hit')",
        )
        .unwrap();

    // Snapshot right after the DDL so we isolate the next query's effect
    // from any plan-cache activity the CREATE TRIGGER itself performed.
    let before = engine.plan_cache().stats();
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    let after = engine.plan_cache().stats();

    assert_eq!(
        after.hits,
        before.hits + 1,
        "the cached join plan must survive an unrelated trigger DDL and be served from cache"
    );
    assert_eq!(
        after.misses, before.misses,
        "no re-plan should have happened after the unrelated trigger DDL"
    );
}

#[test]
fn a2_cached_dml_memo_fires_trigger_created_after_compile() {
    // The prepared/auto-param DML fast path bakes the no-trigger decision in
    // at compile time (prepared_dml::compile_prepared_dml). This proves the
    // memo compiled before the trigger existed is invalidated (target table
    // schema_version bump) and recompiled so the new trigger fires.
    with_test_env(&[("MYDB_AUTO_PARAMETERIZE", Some("all"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());

        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("CREATE TABLE audit (id INT PRIMARY KEY AUTO_INCREMENT, note VARCHAR(16))")
            .unwrap();

        // Compile + cache the INSERT memo while t has no trigger.
        engine.execute("INSERT INTO t VALUES (1)").unwrap();

        // Now attach an AFTER INSERT trigger.
        engine
            .execute(
                "CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW \
                 INSERT INTO audit(note) VALUES ('fired')",
            )
            .unwrap();

        // Same INSERT shape → same auto-param fingerprint → looks up the
        // stale memo, which must be rejected and recompiled to fire the
        // trigger.
        engine.execute("INSERT INTO t VALUES (2)").unwrap();

        let audit = rows(&engine, "SELECT COUNT(*) FROM audit");
        assert_eq!(
            audit[0][0],
            Value::Int(1),
            "trigger created after the DML memo was compiled must still fire"
        );
    });
}

#[test]
fn a1_drop_recreate_same_name_does_not_reuse_stale_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());

    // Fresh table starts at schema_version 1.
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    assert_eq!(
        rows(&engine, "SELECT v FROM t WHERE id = 1")[0][0],
        Value::Int(100)
    );
    // Cache is warm for this SQL now.

    // DROP + CREATE with the SAME name: the new table has a different
    // table_id and its schema_version resets to 1 — identical to the cached
    // plan's recorded version. Only the id comparison in `table_versions_
    // match` prevents the stale plan (built against the old table) from
    // being served.
    engine.execute("DROP TABLE t").unwrap();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 200)").unwrap();

    let got = rows(&engine, "SELECT v FROM t WHERE id = 1");
    assert_eq!(
        got[0][0],
        Value::Int(200),
        "must plan against the recreated table, not reuse the stale same-name plan"
    );
}

#[test]
fn a1_plan_cache_hits_repeatedly_for_stable_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);

    for _ in 0..5 {
        assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    }
    let stats = engine.plan_cache().stats();
    assert!(
        stats.hits >= 4,
        "a stable table's plan should be served from cache on repeat: {stats:?}"
    );
    assert_eq!(
        stats.deps_invalidations, 0,
        "no schema change → no dep invalidations"
    );
}

#[test]
fn cached_streaming_join_reuses_plan_for_protocol_metadata() {
    with_test_env(&[("MYDB_VECTORIZED", Some("on"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        let mut session = SessionContext::default();
        engine
            .execute_with_session(&mut session, "SET optimizer_trace='enabled=on'")
            .unwrap();
        engine
            .execute_with_session(&mut session, "SET NAMES latin1")
            .unwrap();
        engine
            .execute("CREATE TABLE meta_a (id INT PRIMARY KEY, f FLOAT)")
            .unwrap();
        engine
            .execute("CREATE TABLE meta_b (id INT PRIMARY KEY, payload LONGTEXT)")
            .unwrap();
        engine
            .execute("CREATE TABLE meta_c (id INT PRIMARY KEY)")
            .unwrap();
        engine
            .execute("INSERT INTO meta_a VALUES (1, 12.5)")
            .unwrap();
        engine
            .execute("INSERT INTO meta_b VALUES (1, '4142')")
            .unwrap();
        engine.execute("INSERT INTO meta_c VALUES (1)").unwrap();

        let sql = "SELECT FORMAT(a.f, 2) AS formatted, \
                          UNHEX(b.payload) AS decoded, COUNT(*) AS n \
                   FROM meta_a a \
                   JOIN meta_b b ON b.id = a.id \
                   JOIN meta_c c ON c.id = a.id \
                   GROUP BY a.f, b.payload";
        let execute = |session: &mut SessionContext| {
            let QueryResponse::ResultSet(result) =
                engine.execute_with_session(session, sql).unwrap()
            else {
                panic!("expected materialized protocol result");
            };
            result
        };

        let cold = execute(&mut session);
        assert_eq!(cold.columns[0].source.length, Some(48));
        assert_eq!(cold.columns[1].column_type, crate::types::ColumnType::Blob);
        assert_eq!(cold.columns[1].source.length, Some(2_147_483_648));
        assert_ne!(
            cold.columns[1].source.flags & crate::mysql::protocol::BINARY_FLAG,
            0
        );
        let before = engine.plan_cache().stats();
        let warm = execute(&mut session);
        assert_eq!(warm.columns, cold.columns);
        assert_eq!(warm.rows, cold.rows);
        assert_eq!(engine.plan_cache().stats().hits, before.hits + 1);

        let QueryResponse::ResultSet(trace) = engine
            .execute_with_session(
                &mut session,
                "SELECT TRACE FROM information_schema.optimizer_trace",
            )
            .unwrap()
        else {
            panic!("expected optimizer trace result set");
        };
        let rendered = trace.rows[0][0].to_display_string();
        assert!(
            rendered.contains("cached_or_specialized_path"),
            "a cached streaming SELECT must not invoke the join planner again: {rendered}"
        );
        assert!(
            !rendered.contains("join_enumeration_result"),
            "protocol metadata unexpectedly triggered join enumeration: {rendered}"
        );
    });
}

#[test]
fn memory_resource_resize_does_not_invalidate_or_replan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);

    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    let before = engine.plan_cache().stats();

    engine
        .execute("SET GLOBAL mydb_buffer_pool_pages = 64")
        .unwrap();
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    let after = engine.plan_cache().stats();

    assert_eq!(after.hits, before.hits + 1);
    assert_eq!(after.misses, before.misses);
    assert_eq!(after.deps_invalidations, before.deps_invalidations);
}

#[test]
fn a3_caches_report_approx_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);

    assert_eq!(
        engine.plan_cache().stats().approx_bytes,
        0,
        "empty plan cache is zero bytes"
    );
    let _ = rows(&engine, JOIN_SQL);

    let plan_stats = engine.plan_cache().stats();
    assert!(plan_stats.size > 0);
    assert!(
        plan_stats.approx_bytes >= plan_stats.size,
        "populated plan cache reports non-trivial approx_bytes: {plan_stats:?}"
    );
    let sc = engine.statement_cache().stats();
    assert_eq!(
        sc.approx_bytes > 0,
        sc.size > 0,
        "statement cache approx_bytes must track occupancy: {sc:?}"
    );

    // The uniform mydb_caches view surfaces the same figure.
    let caches = rows(
        &engine,
        "SELECT approx_bytes FROM information_schema.mydb_caches \
         WHERE cache_name = 'physical_plan'",
    );
    assert_eq!(caches.len(), 1);
    let bytes = match &caches[0][0] {
        Value::Int(n) => *n,
        Value::UInt(n) => *n as i64,
        other => panic!("unexpected approx_bytes value {other:?}"),
    };
    assert!(bytes > 0, "mydb_caches.approx_bytes must be populated");
}

#[test]
fn b1_alter_view_invalidates_dependent_cached_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dim (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO base VALUES (1, 10, 20)")
        .unwrap();
    engine.execute("INSERT INTO dim VALUES (1)").unwrap();
    // A join also verifies that dependencies introduced by view expansion
    // participate in the shared compiled-plan cache.
    engine
        .execute("CREATE VIEW vw AS SELECT base.a AS x FROM base JOIN dim ON base.id = dim.id")
        .unwrap();

    let sql = "SELECT x FROM vw WHERE x > 0";
    assert_eq!(rows(&engine, sql)[0][0], Value::Int(10));
    let warm = engine.plan_cache().stats();
    assert_eq!(rows(&engine, sql)[0][0], Value::Int(10));
    assert_eq!(
        engine.plan_cache().stats().hits,
        warm.hits + 1,
        "the view query must be served from the plan cache before ALTER"
    );

    // Redefine the view to project a different column.
    engine
        .execute("ALTER VIEW vw AS SELECT base.b AS x FROM base JOIN dim ON base.id = dim.id")
        .unwrap();

    // The cached plan expanded the OLD view body; it must be invalidated via
    // the view object-dependency and re-planned to return b (20), not a (10).
    assert_eq!(
        rows(&engine, sql)[0][0],
        Value::Int(20),
        "ALTER VIEW must invalidate the plan that expanded the old view body"
    );
}

#[test]
fn b1_view_ddl_preserves_unrelated_hot_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, a INT)")
        .unwrap();

    // Warm the unrelated join plan.
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));

    // Create + alter + drop a view that does not touch tables x/y.
    engine
        .execute("CREATE VIEW vw AS SELECT a FROM base")
        .unwrap();
    engine
        .execute("ALTER VIEW vw AS SELECT a AS a2 FROM base")
        .unwrap();
    engine.execute("DROP VIEW vw").unwrap();

    let before = engine.plan_cache().stats();
    assert_eq!(rows(&engine, JOIN_SQL)[0][0], Value::Int(10));
    let after = engine.plan_cache().stats();
    assert_eq!(
        after.hits,
        before.hits + 1,
        "view DDL must not flush hot plans on unrelated tables"
    );
    assert_eq!(after.misses, before.misses);
}

#[test]
fn b1_drop_and_recreate_view_invalidates_cached_plan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    engine
        .execute("INSERT INTO base VALUES (1, 10, 20)")
        .unwrap();
    engine
        .execute("CREATE VIEW vw AS SELECT a AS x FROM base")
        .unwrap();

    let sql = "SELECT x FROM vw WHERE x > 0";
    assert_eq!(rows(&engine, sql)[0][0], Value::Int(10));
    assert_eq!(rows(&engine, sql)[0][0], Value::Int(10)); // cached

    engine.execute("DROP VIEW vw").unwrap();
    engine
        .execute("CREATE VIEW vw AS SELECT b AS x FROM base")
        .unwrap();

    assert_eq!(
        rows(&engine, sql)[0][0],
        Value::Int(20),
        "DROP + recreate VIEW must invalidate the stale cached plan"
    );
}

#[test]
fn d3_mydb_plan_cache_view_lists_entries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);

    // Empty cache → empty view.
    assert!(rows(
        &engine,
        "SELECT cached_sql FROM information_schema.mydb_plan_cache"
    )
    .is_empty());

    // Warm the join plan and hit it a couple of times.
    for _ in 0..3 {
        let _ = rows(&engine, JOIN_SQL);
    }

    let view = rows(
        &engine,
        "SELECT cached_sql, hits, table_deps, approx_bytes \
         FROM information_schema.mydb_plan_cache",
    );
    assert_eq!(view.len(), 1, "one cached join plan expected: {view:?}");
    let row = &view[0];
    // hits: 3 executions after the initial miss → >= 2 hits.
    let hits = match &row[1] {
        Value::Int(n) => *n,
        other => panic!("hits should be int, got {other:?}"),
    };
    assert!(hits >= 2, "cached plan should show per-entry hits: {row:?}");
    assert_eq!(
        row[2],
        Value::Int(2),
        "the join references two tables (x, y)"
    );
    match &row[3] {
        Value::Int(n) => assert!(*n > 0, "approx_bytes populated"),
        other => panic!("approx_bytes should be int, got {other:?}"),
    }
}

#[test]
fn b2_plan_cache_partitions_by_time_zone() {
    // Table-backed so the plan is cacheable (non-empty deps). FROM_UNIXTIME
    // is evaluated at execution against the session time_zone, so the same
    // cached plan yields per-session results.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    seed_join_tables(&engine);

    // Use a join to exercise time-zone semantics across a multi-table plan.
    let sql = "SELECT FROM_UNIXTIME(x.u) FROM x JOIN y ON x.id = y.id WHERE x.id = 1";

    let mut utc = SessionContext::default();
    engine
        .execute_with_session(&mut utc, "SET time_zone = '+00:00'")
        .unwrap();
    let utc_row = rows_session(&engine, &mut utc, sql);

    let before = engine.plan_cache().stats();
    let mut plus8 = SessionContext::default();
    engine
        .execute_with_session(&mut plus8, "SET time_zone = '+08:00'")
        .unwrap();
    let plus8_row = rows_session(&engine, &mut plus8, sql);
    let after = engine.plan_cache().stats();

    // Time zone is an explicit PlanSemantics component. Even though runtime
    // evaluation currently observes the active session, keep this boundary
    // conservative so future expression compilation cannot bake a zone into
    // an artifact and then leak it across sessions.
    assert_eq!(
        after.misses,
        before.misses + 1,
        "a different time_zone must occupy a different compiled-plan slot"
    );
    assert_ne!(
        utc_row[0][0], plus8_row[0][0],
        "FROM_UNIXTIME(0) must differ by session time_zone (+00:00 vs +08:00), \
         proving the cached plan carries no session state"
    );
}

#[test]
fn type_affecting_session_variables_are_part_of_plan_semantics() {
    let base = SessionContext::default();
    let mut changed_div = base.clone();
    changed_div.sysvars.div_precision_increment = 9;
    assert_ne!(
        super::super::plan_semantics(&base),
        super::super::plan_semantics(&changed_div),
        "AVG and decimal division metadata must be rebound"
    );

    let mut changed_group_concat = base.clone();
    changed_group_concat.group_concat_max_len = 512;
    assert_ne!(
        super::super::plan_semantics(&base),
        super::super::plan_semantics(&changed_group_concat),
        "GROUP_CONCAT VARCHAR/BLOB metadata must be rebound"
    );
}

#[test]
fn b2_plan_cache_partitions_by_exact_sql_select_limit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE x (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE y (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO x VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    let sql = "SELECT id FROM x ORDER BY id";
    let mut limit_one = SessionContext::default();
    engine
        .execute_with_session(&mut limit_one, "SET sql_select_limit = 1")
        .unwrap();
    assert_eq!(rows_session(&engine, &mut limit_one, sql).len(), 1);

    let before = engine.plan_cache().stats();
    let mut limit_three = SessionContext::default();
    engine
        .execute_with_session(&mut limit_three, "SET sql_select_limit = 3")
        .unwrap();
    assert_eq!(rows_session(&engine, &mut limit_three, sql).len(), 3);
    let mut limit_five = SessionContext::default();
    engine
        .execute_with_session(&mut limit_five, "SET sql_select_limit = 5")
        .unwrap();
    assert_eq!(rows_session(&engine, &mut limit_five, sql).len(), 5);
    let after = engine.plan_cache().stats();

    assert_eq!(
        after.misses,
        before.misses + 2,
        "sql_select_limit must be keyed by its exact value"
    );
}

#[test]
fn cache_audit_batch_literals_match_parser_after_warmup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cache_strings (id INT PRIMARY KEY, v VARCHAR(100))")
        .unwrap();
    engine.execute("INSERT INTO cache_strings VALUES (1, 'aaa'); INSERT INTO cache_strings VALUES (2, 'bbb')").unwrap();
    assert_eq!(engine.prepared_dml_batch_cache_stats_for_tests().size, 1);
    let cases = [
        (r"'a\nb'", "a\nb"),
        ("'中文😀'", "中文😀"),
        (r"'a\tb'", "a\tb"),
        (r"'a\0b'", "a\0b"),
        (r"'a\\b'", "a\\b"),
        ("'it''s'", "it's"),
    ];
    for (i, (literal, expected)) in cases.iter().enumerate() {
        let id = 3 + i * 2;
        engine.execute(&format!("INSERT INTO cache_strings VALUES ({id}, {literal}); INSERT INTO cache_strings VALUES ({}, 'x')", id + 1)).unwrap();
        assert_eq!(
            rows(
                &engine,
                &format!("SELECT v FROM cache_strings WHERE id = {id}")
            ),
            vec![vec![Value::String((*expected).into())]],
            "INSERT {literal}"
        );
    }
    engine.execute("UPDATE cache_strings SET v = 'warm' WHERE id = 1; UPDATE cache_strings SET v = 'warm' WHERE id = 2").unwrap();
    for (literal, expected) in cases {
        engine.execute(&format!("UPDATE cache_strings SET v = {literal} WHERE id = 1; UPDATE cache_strings SET v = 'x' WHERE id = 2")).unwrap();
        assert_eq!(
            rows(&engine, "SELECT v FROM cache_strings WHERE id = 1"),
            vec![vec![Value::String(expected.into())]],
            "UPDATE {literal}"
        );
    }
    let before = engine.prepared_dml_batch_cache_stats_for_tests().hits;
    engine.execute("UPDATE cache_strings SET v = 'hot' WHERE id = 1; UPDATE cache_strings SET v = 'hot' WHERE id = 2").unwrap();
    assert!(engine.prepared_dml_batch_cache_stats_for_tests().hits > before);
}

#[test]
fn cache_audit_flush_releases_expression_and_dml_caches() {
    use crate::engine::expr_program::{expr_program_stats, ExprCompiler, InputSchema};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE cache_flush (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO cache_flush VALUES (1, 10); INSERT INTO cache_flush VALUES (2, 20)")
        .unwrap();
    let expr = crate::parser::Expr::Literal(crate::parser::Literal::Int(734829));
    let schema = InputSchema::new(Vec::new());
    let before = ExprCompiler::compile_cached(&expr, &schema).unwrap();
    assert!(expr_program_stats().l2_entries > 0);
    engine.execute("ALTER SYSTEM FLUSH SQL_CACHE").unwrap();
    assert_eq!(expr_program_stats().l2_entries, 0);
    assert_eq!(expr_program_stats().l1_entries, 0);
    assert_eq!(engine.prepared_dml_batch_cache_stats_for_tests().size, 0);
    let after = ExprCompiler::compile_cached(&expr, &schema).unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
}

#[test]
fn cache_audit_routine_statements_do_not_leak_plan_hint() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    for sql in [
        "CREATE TABLE cache_source (id INT PRIMARY KEY, x INT)",
        "INSERT INTO cache_source VALUES (1, 42)",
        "CREATE PROCEDURE cache_proc() BEGIN SELECT x INTO @dummy FROM cache_source WHERE id = 1; END",
        "CREATE FUNCTION cache_fn() RETURNS INT READS SQL DATA BEGIN DECLARE v INT; SELECT x INTO v FROM cache_source WHERE id = 1; RETURN v; END",
        "ALTER SYSTEM FLUSH SQL_CACHE",
        "CALL cache_proc()",
        "SET @r = cache_fn()",
        "SELECT cache_fn()",
    ] {
        engine.execute_with_session(&mut session, sql).unwrap();
    }
    assert_eq!(
        rows_session(&engine, &mut session, "SELECT @dummy, @r"),
        vec![vec![Value::Int(42), Value::Int(42)]]
    );
    for sql in ["CALL cache_proc()", "SET @r = cache_fn()"] {
        let statement = crate::parser::parse_statement(sql).unwrap();
        QueryEngine::execute_prepared_statement(&engine, &mut session, sql, statement)
            .unwrap()
            .unwrap();
    }
    let entries = rows(
        &engine,
        "SELECT CACHED_SQL FROM information_schema.mydb_plan_cache",
    );
    assert!(
        !entries.iter().any(|row| match &row[0] {
            Value::String(sql) => sql.starts_with("call ") || sql.starts_with("set "),
            _ => false,
        }),
        "polluted cache: {entries:?}"
    );
}

#[test]
fn cache_audit_timezone_registry_reports_resident_entries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    rows(
        &engine,
        "SELECT CONVERT_TZ('2026-01-01 00:00:00', '+08:00', '+00:00')",
    );
    let (entries, bytes) = crate::engine::expr::tz_spec_cache_stats();
    assert!(entries >= 2);
    assert!(bytes > 0);
}

#[test]
fn cache_audit_batch_literals_respect_sql_modes_and_comments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE cache_modes (id INT PRIMARY KEY, v VARCHAR(100))")
        .unwrap();
    engine
        .execute("INSERT INTO cache_modes VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    for mode in [
        "",
        "NO_BACKSLASH_ESCAPES",
        "ANSI_QUOTES",
        "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
    ] {
        engine
            .execute_with_session(&mut session, &format!("SET sql_mode = '{mode}'"))
            .unwrap();
        engine.execute_with_session(&mut session, "UPDATE cache_modes SET v = 'warm' WHERE id = 1; UPDATE cache_modes SET v = 'warm' WHERE id = 2").unwrap();
        for literal in [
            r"'a\nb'",
            "'中文😀'",
            "'it''s'",
            "'semi;colon'",
            "'/*! text */'",
        ] {
            let expected = rows_session(&engine, &mut session, &format!("SELECT {literal}"));
            engine.execute_with_session(&mut session, &format!("UPDATE cache_modes SET v = {literal} WHERE id = 1; UPDATE cache_modes SET v = 'x' WHERE id = 2")).unwrap();
            assert_eq!(
                rows_session(
                    &engine,
                    &mut session,
                    "SELECT v FROM cache_modes WHERE id = 1"
                ),
                expected,
                "{mode}: {literal}"
            );
        }
        for comment in ["/*+ hint */", "/*!80000 */", "/* normal */"] {
            engine.execute_with_session(&mut session, &format!("UPDATE {comment} cache_modes SET v = 'comment' WHERE id = 1; UPDATE cache_modes SET v = 'x' WHERE id = 2")).unwrap();
            assert_eq!(
                rows_session(
                    &engine,
                    &mut session,
                    "SELECT v FROM cache_modes WHERE id = 1"
                ),
                vec![vec![Value::String("comment".into())]]
            );
        }
    }
}
