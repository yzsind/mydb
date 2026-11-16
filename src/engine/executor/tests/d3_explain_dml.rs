use super::support::result_rows as rows;
use super::*;

fn select_type_at(row: &[Value]) -> String {
    // explain columns: id, select_type, table, ...
    match &row[1] {
        Value::String(s) => s.clone(),
        _ => panic!("select_type not a string"),
    }
}

fn table_at(row: &[Value]) -> String {
    let raw = match &row[2] {
        Value::String(s) => s.clone(),
        _ => panic!("table not a string"),
    };
    // Strip the implicit `schema.` qualifier the executor adds at
    // qualification time so callers can compare against the unqualified
    // name they wrote.
    raw.rsplit('.').next().unwrap_or(&raw).to_string()
}

/// D3 — `EXPLAIN UPDATE` produces an EXPLAIN row tagged with `UPDATE` and
/// the underlying table. Index lookups on the PK should show up in the
/// `key` / `access_type` columns just like a SELECT would.
#[test]
fn explain_update_emits_dml_label() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let resp = engine
        .execute("EXPLAIN UPDATE t SET v = 99 WHERE id = 2")
        .unwrap();
    let rs = rows(resp);
    assert!(!rs.is_empty(), "EXPLAIN UPDATE produced no rows");
    assert_eq!(select_type_at(&rs[0]), "UPDATE");
    assert_eq!(table_at(&rs[0]), "t");
    // The original row count must not have changed — EXPLAIN UPDATE does
    // not execute the UPDATE.
    let stored = rows(engine.execute("SELECT v FROM t WHERE id = 2").unwrap());
    assert_eq!(stored, vec![vec![Value::Int(20)]]);
}

/// D3 — `EXPLAIN DELETE` produces a `DELETE`-tagged plan row referencing
/// the target table.
#[test]
fn explain_delete_emits_dml_label() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();
    let resp = engine
        .execute("EXPLAIN DELETE FROM t WHERE id = 1")
        .unwrap();
    let rs = rows(resp);
    assert!(!rs.is_empty());
    assert_eq!(select_type_at(&rs[0]), "DELETE");
    // Rows untouched.
    let count = rows(engine.execute("SELECT id FROM t").unwrap()).len();
    assert_eq!(count, 2);
}

/// MySQL reports the mutation target as a metadata row. VALUES is an input
/// node, not a fabricated access type or target-row estimate.
#[test]
fn explain_insert_values_uses_nullable_mysql_surface() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let resp = engine
        .execute("EXPLAIN INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let rs = rows(resp);
    assert_eq!(rs.len(), 1);
    assert_eq!(select_type_at(&rs[0]), "INSERT");
    assert_eq!(table_at(&rs[0]), "t");
    assert_eq!(rs[0][3], Value::Null, "unpartitioned target");
    assert_eq!(rs[0][4], Value::String("ALL".to_string()));
    assert_eq!(rs[0][9], Value::Null, "target estimate is SQL NULL");
    // No actual insertion happened.
    let count = rows(engine.execute("SELECT id FROM t").unwrap()).len();
    assert_eq!(count, 0);
}

/// D3 — `EXPLAIN INSERT ... SELECT` produces a prefix INSERT row and the
/// SELECT-side plan rows below it.
#[test]
fn explain_insert_select_emits_prefix_and_source() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1, 10), (2, 20)")
        .unwrap();
    let resp = engine
        .execute("EXPLAIN INSERT INTO dst SELECT id, v FROM src")
        .unwrap();
    let rs = rows(resp);
    assert!(rs.len() >= 2, "expected INSERT + SELECT body rows");
    assert_eq!(select_type_at(&rs[0]), "INSERT");
    assert_eq!(table_at(&rs[0]), "dst");
    // At least one of the following rows references `src`.
    assert!(
        rs.iter().skip(1).any(|r| table_at(r) == "src"),
        "expected a row for the SELECT side referencing 'src'"
    );
    // No actual insertion.
    let count = rows(engine.execute("SELECT id FROM dst").unwrap()).len();
    assert_eq!(count, 0);
}

/// Partition pruning belongs in the `partitions` column, not in `Extra`.
#[test]
fn explain_insert_carries_partition_marker() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    let resp = engine
        .execute("EXPLAIN INSERT INTO t PARTITION (p0) VALUES (1)")
        .unwrap();
    let rs = rows(resp);
    assert_eq!(rs.len(), 1);
    let partitions = match &rs[0][3] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert_eq!(partitions, "p0");
}

/// D3 — `EXPLAIN UPDATE` on a multi-table form surfaces all the joined
/// tables in the plan output (one row per scan side).
#[test]
fn explain_update_multi_table_lists_joined_sources() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let resp = engine
        .execute("EXPLAIN UPDATE a JOIN b ON a.id = b.id SET a.v = b.v")
        .unwrap();
    let rs = rows(resp);
    let tables: Vec<String> = rs.iter().map(|r| table_at(r.as_slice())).collect();
    assert!(tables.iter().any(|t| t == "a"));
    assert!(tables.iter().any(|t| t == "b"));
    let a = rs.iter().find(|row| table_at(row) == "a").unwrap();
    let b = rs.iter().find(|row| table_at(row) == "b").unwrap();
    assert_eq!(select_type_at(a), "UPDATE");
    assert_ne!(select_type_at(b), "UPDATE");

    let rs = rows(
        engine
            .execute("EXPLAIN UPDATE a JOIN b ON a.id = b.id SET b.v = a.v")
            .unwrap(),
    );
    let a = rs.iter().find(|row| table_at(row) == "a").unwrap();
    let b = rs.iter().find(|row| table_at(row) == "b").unwrap();
    assert_ne!(select_type_at(a), "UPDATE");
    assert_eq!(select_type_at(b), "UPDATE");
}

/// D3 — `EXPLAIN INSERT ... ON DUPLICATE KEY UPDATE` flags the conflict
/// handling via Extra.
#[test]
fn explain_insert_advertises_on_duplicate_key_update() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let resp = engine
        .execute(
            "EXPLAIN INSERT INTO t VALUES (1, 10) \
             ON DUPLICATE KEY UPDATE v = VALUES(v)",
        )
        .unwrap();
    let rs = rows(resp);
    assert_eq!(rs.len(), 1);
    let extra = match &rs[0][11] {
        Value::String(s) => s.clone(),
        _ => String::new(),
    };
    assert!(extra.contains("ON DUPLICATE"), "extra: {extra}");
}

#[test]
fn explain_dml_tree_and_json_are_structural_and_static() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let tree = rows(
        engine
            .execute("EXPLAIN FORMAT=TREE UPDATE t SET v = 20 WHERE id = 1")
            .unwrap(),
    );
    let Value::String(tree) = &tree[0][0] else {
        panic!("TREE output is not text")
    };
    assert!(tree.starts_with("-> Update on "));
    assert!(
        tree.to_ascii_lowercase().contains("index") || tree.to_ascii_lowercase().contains("scan")
    );

    let json = rows(
        engine
            .execute("EXPLAIN FORMAT=JSON DELETE FROM t WHERE id = 1")
            .unwrap(),
    );
    let Value::String(json) = &json[0][0] else {
        panic!("JSON output is not text")
    };
    let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
    assert_eq!(parsed["mutation"]["operation"], "delete");
    assert!(parsed["mutation"]["input"]["query_block"].is_object());

    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id = 1").unwrap()),
        vec![vec![Value::Int(10)]],
        "plain EXPLAIN must not mutate data"
    );
}

#[test]
fn explain_analyze_executes_all_dml_kinds_once() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let insert = rows(
        engine
            .execute("EXPLAIN ANALYZE FORMAT=TRADITIONAL INSERT INTO t VALUES (1, 10)")
            .unwrap(),
    );
    assert_eq!(insert[0].len(), 17);
    assert_eq!(insert[0][12], Value::Double(1.0));
    assert_eq!(insert[0][16], Value::Int(1));

    let update = rows(
        engine
            .execute("EXPLAIN ANALYZE FORMAT=TREE UPDATE t SET v = 20 WHERE id = 1")
            .unwrap(),
    );
    let Value::String(update) = &update[0][0] else {
        panic!("TREE output is not text")
    };
    assert!(update.contains("actual time="));
    assert!(
        update.contains("physical_reads=")
            && update.contains("cache_reads=")
            && update.contains("cache_hit_rate=")
            && update.starts_with("Query I/O: physical_reads="),
        "DML TREE root must expose its query-scoped block reads: {update}"
    );

    let replace = rows(
        engine
            .execute("EXPLAIN ANALYZE FORMAT=JSON REPLACE INTO t VALUES (1, 30)")
            .unwrap(),
    );
    let Value::String(replace) = &replace[0][0] else {
        panic!("JSON output is not text")
    };
    let replace: serde_json::Value = serde_json::from_str(replace).unwrap();
    assert_eq!(replace["mutation"]["operation"], "replace");
    assert_eq!(replace["mutation"]["actual"]["affected_rows"], 2);
    assert_eq!(replace["mutation"]["actual"]["input_rows"], 1);
    assert!(replace["mutation"]["actual"]["replaced"].is_null());
    assert!(replace["mutation"]["actual"]["first_row_ms"].is_number());
    assert!(replace["mutation"]["actual"]["last_row_ms"].is_number());

    rows(
        engine
            .execute("EXPLAIN ANALYZE DELETE FROM t WHERE id = 1")
            .unwrap(),
    );
    assert!(rows(engine.execute("SELECT id FROM t").unwrap()).is_empty());
}

#[test]
fn explain_analyze_indexed_dml_reports_logical_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE analyze_dml_candidates (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO analyze_dml_candidates VALUES (1, 10), (2, 20)")
        .unwrap();

    let update = rows(
        engine
            .execute("EXPLAIN ANALYZE FORMAT=JSON UPDATE analyze_dml_candidates SET v=v WHERE id=1")
            .unwrap(),
    );
    let Value::String(update) = &update[0][0] else {
        panic!("JSON output is not text")
    };
    let update: serde_json::Value = serde_json::from_str(update).unwrap();
    assert_eq!(update["mutation"]["actual"]["candidate_rows"], 1);
    assert_eq!(update["mutation"]["actual"]["matched"], 1);

    let rejected = rows(
        engine
            .execute(
                "EXPLAIN ANALYZE FORMAT=JSON UPDATE analyze_dml_candidates SET v=v WHERE id=1 AND v=999",
            )
            .unwrap(),
    );
    let Value::String(rejected) = &rejected[0][0] else {
        panic!("JSON output is not text")
    };
    let rejected: serde_json::Value = serde_json::from_str(rejected).unwrap();
    assert_eq!(rejected["mutation"]["actual"]["candidate_rows"], 1);
    assert_eq!(rejected["mutation"]["actual"]["matched"], 0);
}

#[test]
fn explain_analyze_dml_obeys_explicit_transaction_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "EXPLAIN ANALYZE INSERT INTO t VALUES (1, 10)")
        .unwrap();
    assert_eq!(
        rows(
            engine
                .execute_with_session(&mut session, "SELECT id FROM t")
                .unwrap()
        )
        .len(),
        1
    );
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert!(rows(engine.execute("SELECT id FROM t").unwrap()).is_empty());
}

#[test]
fn explain_dml_validates_before_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let error = engine
        .execute("EXPLAIN ANALYZE INSERT INTO t (id, missing) VALUES (1, 2)")
        .unwrap_err();
    assert_eq!(error.code(), 1054);
    assert!(rows(engine.execute("SELECT id FROM t").unwrap()).is_empty());
}

#[test]
fn prepared_dml_explain_metadata_matches_execute_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let traditional = engine
        .describe_prepared_statement(&mut session, "EXPLAIN UPDATE t SET v = ? WHERE id = ?")
        .unwrap();
    assert_eq!(traditional.len(), 12);
    assert_eq!(traditional[0].name, "id");
    assert_eq!(traditional[11].name, "Extra");

    let analyzed = engine
        .describe_prepared_statement(
            &mut session,
            "EXPLAIN ANALYZE FORMAT=TRADITIONAL INSERT INTO t VALUES (?, ?)",
        )
        .unwrap();
    assert_eq!(analyzed.len(), 17);
    assert_eq!(analyzed[12].name, "actual_rows");
    assert_eq!(analyzed[13].name, "actual_loops");
    assert_eq!(analyzed[14].name, "actual_first_row_ms");
    assert_eq!(analyzed[15].name, "actual_last_row_ms");
    assert_eq!(analyzed[16].name, "actual_rows_total");

    let json = engine
        .describe_prepared_statement(
            &mut session,
            "EXPLAIN FORMAT=JSON DELETE FROM t WHERE id = ?",
        )
        .unwrap();
    assert_eq!(json.len(), 1);
    assert_eq!(json[0].name, "EXPLAIN");
}

#[test]
fn explain_and_analyze_cte_driven_insert_share_cte_input() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO src VALUES (1, 10)").unwrap();

    let plan = rows(
        engine
            .execute(
                "EXPLAIN FORMAT=TREE WITH c AS (SELECT id, v FROM src) \
                 INSERT INTO dst SELECT id, v FROM c",
            )
            .unwrap(),
    );
    let Value::String(plan) = &plan[0][0] else {
        panic!("TREE output is not text")
    };
    assert!(plan.contains("Insert"));
    assert!(rows(engine.execute("SELECT id FROM dst").unwrap()).is_empty());

    engine
        .execute(
            "EXPLAIN ANALYZE WITH c AS (SELECT id, v FROM src) \
             INSERT INTO dst SELECT id, v FROM c",
        )
        .unwrap();
    assert_eq!(rows(engine.execute("SELECT id FROM dst").unwrap()).len(), 1);
}

#[test]
fn explain_dml_supports_updatable_views_without_static_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE base (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO base VALUES (1, 10)").unwrap();
    engine
        .execute("CREATE VIEW visible AS SELECT id, v FROM base WHERE id > 0")
        .unwrap();

    let plan = rows(
        engine
            .execute("EXPLAIN FORMAT=JSON UPDATE visible SET v = 20 WHERE id = 1")
            .unwrap(),
    );
    let Value::String(plan) = &plan[0][0] else {
        panic!("JSON output is not text")
    };
    serde_json::from_str::<serde_json::Value>(plan).unwrap();
    assert_eq!(
        rows(engine.execute("SELECT v FROM base WHERE id = 1").unwrap()),
        vec![vec![Value::Int(10)]]
    );
}

#[test]
fn explain_dml_supports_session_temporary_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TEMPORARY TABLE tmp (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    let plan = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN FORMAT=TREE INSERT INTO tmp VALUES (1, 10)",
        )
        .unwrap();
    let plan_rows = rows(plan);
    let Value::String(plan) = &plan_rows[0][0] else {
        panic!("TREE output is not text")
    };
    assert!(plan.contains("Insert"));
    assert!(rows(
        engine
            .execute_with_session(&mut session, "SELECT id FROM tmp")
            .unwrap()
    )
    .is_empty());
}

#[test]
fn static_explain_is_allowed_but_analyze_is_rejected_in_read_only_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    engine
        .execute_with_session(&mut session, "START TRANSACTION READ ONLY")
        .unwrap();

    engine
        .execute_with_session(&mut session, "EXPLAIN UPDATE t SET v = 20 WHERE id = 1")
        .unwrap();
    let error = engine
        .execute_with_session(
            &mut session,
            "EXPLAIN ANALYZE UPDATE t SET v = 20 WHERE id = 1",
        )
        .unwrap_err();
    assert_eq!(error.code(), 1792);
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id = 1").unwrap()),
        vec![vec![Value::Int(10)]]
    );
}

#[test]
#[ignore = "requires a running local mydb server; set MYDB_EXPLAIN_PROTOCOL_TEST_PORT"]
fn mysql_protocol_prepared_dml_explain_metadata_and_repeat_execute() {
    let port = std::env::var("MYDB_EXPLAIN_PROTOCOL_TEST_PORT")
        .expect("set MYDB_EXPLAIN_PROTOCOL_TEST_PORT to a running local mydb server's port");
    let port: u16 = port
        .parse()
        .expect("MYDB_EXPLAIN_PROTOCOL_TEST_PORT must be a TCP port from 1 to 65535");
    assert_ne!(port, 0, "MYDB_EXPLAIN_PROTOCOL_TEST_PORT must not be zero");
    use ::mysql::prelude::Queryable;

    let opts: ::mysql::Opts = ::mysql::OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("root"))
        .db_name(Some("test"))
        .into();
    let mut connection = ::mysql::Conn::new(opts).unwrap();
    connection
        .query_drop("DROP TABLE IF EXISTS explain_protocol")
        .unwrap();
    connection
        .query_drop("CREATE TABLE explain_protocol (id INT PRIMARY KEY, v INT)")
        .unwrap();

    let insert = connection
        .prep("EXPLAIN ANALYZE FORMAT=TRADITIONAL INSERT INTO explain_protocol VALUES (?, ?)")
        .unwrap();
    assert_eq!(insert.columns().len(), 17);
    let first: Vec<::mysql::Row> = connection.exec(&insert, (1, 10)).unwrap();
    let second: Vec<::mysql::Row> = connection.exec(&insert, (2, 20)).unwrap();
    assert_eq!(first[0].len(), 17);
    assert_eq!(second[0].len(), 17);

    let json = connection
        .prep("EXPLAIN FORMAT=JSON UPDATE explain_protocol SET v = ? WHERE id = ?")
        .unwrap();
    assert_eq!(json.columns().len(), 1);
    let rows: Vec<::mysql::Row> = connection.exec(&json, (30, 1)).unwrap();
    assert_eq!(rows[0].len(), 1);
    let count: Option<u64> = connection
        .query_first("SELECT COUNT(*) FROM explain_protocol")
        .unwrap();
    assert_eq!(
        count,
        Some(2),
        "static prepared EXPLAIN executed the UPDATE"
    );
}
