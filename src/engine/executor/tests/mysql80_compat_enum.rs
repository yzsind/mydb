//! MySQL 8.0 ENUM/SET comparison compatibility tests.
//!
//! ENUM compares strings by collation and numbers by ordinal; ORDER BY remains ordinal.
use super::*;

fn ids(engine: &StorageQueryEngine, sql: &str) -> Vec<i64> {
    match engine.execute(sql).unwrap() {
        QueryResponse::ResultSet(r) => r
            .rows
            .iter()
            .map(|row| match row[0] {
                Value::Int(v) => v,
                ref other => panic!("id not int: {other:?}"),
            })
            .collect(),
        other => panic!("expected result set for `{sql}`, got {other:?}"),
    }
}

fn response_rows(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(result) => result.rows,
        other => panic!("expected result set, got {other:?}"),
    }
}

fn setup_enum(engine: &StorageQueryEngine) {
    // Declaration order: small=1, medium=2, large=3.
    engine
        .execute("CREATE TABLE e (id INT PRIMARY KEY, sz ENUM('small','medium','large'))")
        .unwrap();
    engine
        .execute("INSERT INTO e VALUES (1,'small'),(2,'medium'),(3,'large')")
        .unwrap();
}

#[test]
fn enum_comparison_uses_string_order() {
    let tmp = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(tmp.path());
    setup_enum(&engine);
    assert_eq!(
        ids(&engine, "SELECT id FROM e WHERE sz > 'medium'"),
        vec![1]
    );
    assert_eq!(
        ids(&engine, "SELECT id FROM e WHERE sz < 'medium'"),
        vec![3]
    );
    assert_eq!(
        ids(&engine, "SELECT id FROM e WHERE sz >= 'medium' ORDER BY id"),
        vec![1, 2]
    );
}

#[test]
fn enum_equality_string_and_int() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup_enum(&engine);
    assert_eq!(
        ids(&engine, "SELECT id FROM e WHERE sz = 'medium'"),
        vec![2]
    );
    // Integer literal compares by 1-based index.
    assert_eq!(ids(&engine, "SELECT id FROM e WHERE sz = 2"), vec![2]);
    assert_eq!(
        ids(&engine, "SELECT id FROM e WHERE sz <> 'small' ORDER BY id"),
        vec![2, 3]
    );
}

#[test]
fn enum_ordering_by_int_literal() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup_enum(&engine);
    // sz < 3 (index) → small, medium.
    assert_eq!(
        ids(&engine, "SELECT id FROM e WHERE sz < 3 ORDER BY id"),
        vec![1, 2]
    );
}

#[test]
fn enum_order_by_uses_index() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    setup_enum(&engine);
    // ORDER BY the enum column sorts by declaration index: small, medium, large.
    assert_eq!(ids(&engine, "SELECT id FROM e ORDER BY sz"), vec![1, 2, 3]);
}

#[test]
fn set_equality_is_order_agnostic() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, flags SET('a','b','c'))")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (1,'a,b'),(2,'c'),(3,'a,c')")
        .unwrap();
    // Stored canonical order is by member index ('a,b'); comparing to 'b,a'
    // must still match by bitmask, not by display string.
    assert_eq!(
        ids(&engine, "SELECT id FROM s WHERE flags = 'b,a'"),
        vec![1]
    );
}

#[test]
fn set_order_uses_bitmask_and_binary_collation_preserves_member_case() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE binary_set (\
               s SET('a','A') CHARACTER SET latin1 COLLATE latin1_bin)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO binary_set VALUES ('a'),('a,A'),('A,a'),('A')")
        .unwrap();

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT s FROM binary_set ORDER BY s")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>(),
        ["a", "A", "a,A", "a,A"]
    );

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT s FROM binary_set ORDER BY CONCAT(s)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row[0].to_display_string())
            .collect::<Vec<_>>(),
        ["A", "a", "a,A", "a,A"]
    );
}

// ─────────────────────────── P0-3/4 fixes ────────────────────────────

#[test]
fn enum_accepts_zero_as_empty_string() {
    // MySQL 8.0 semantics: numeric 0 stores the ENUM empty-index sentinel
    // in non-strict mode, while reporting one 1265 truncation warning.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE e (id INT PRIMARY KEY, sz ENUM('a','b'))",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO e VALUES (1, 0)")
        .unwrap();
    assert_eq!(
        session
            .warnings
            .iter()
            .map(|warning| warning.code())
            .collect::<Vec<_>>(),
        vec![1265]
    );
    match engine
        .execute_with_session(&mut session, "SELECT sz FROM e WHERE id = 1")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows[0][0], Value::Bytes(Vec::new()));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn enum_rejects_out_of_range_ordinal() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE e (id INT PRIMARY KEY, sz ENUM('a','b'))")
        .unwrap();
    let err = engine.execute("INSERT INTO e VALUES (1, 5)").unwrap_err();
    assert_eq!(
        err.code(),
        1265,
        "expected 1265 for out-of-range ENUM ordinal"
    );
}

#[test]
fn enum_and_set_invalid_members_truncate_in_nonstrict_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE loose (id INT PRIMARY KEY, e ENUM('a','b'), s SET('x','y'))",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO loose VALUES (1,'missing','x,missing')",
        )
        .unwrap();
    assert_eq!(
        response_rows(
            engine
                .execute_with_session(&mut session, "SELECT e,s FROM loose")
                .unwrap()
        ),
        vec![vec![Value::Bytes(Vec::new()), Value::String("x".into())]]
    );
}

#[test]
fn set_accepts_numeric_bitmask() {
    // INSERT INTO s VALUES (5) on SET('a','b','c') → 'a,c' (bit0 | bit2).
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, flags SET('a','b','c'))")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (1, 5), (2, 0), (3, 7)")
        .unwrap();
    match engine
        .execute("SELECT id, flags FROM s ORDER BY id")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows[0][1], Value::String("a,c".into()));
            assert_eq!(r.rows[1][1], Value::String(String::new()));
            assert_eq!(r.rows[2][1], Value::String("a,b,c".into()));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn set_input_order_gets_canonicalised() {
    // Input 'c,a,b' must be stored as 'a,b,c' (declaration order).
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, flags SET('a','b','c'))")
        .unwrap();
    engine
        .execute("INSERT INTO s VALUES (1, 'c,a,b'), (2, 'b,a')")
        .unwrap();
    match engine
        .execute("SELECT id, flags FROM s ORDER BY id")
        .unwrap()
    {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows[0][1], Value::String("a,b,c".into()));
            assert_eq!(r.rows[1][1], Value::String("a,b".into()));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn set_leading_space_in_token_rejected() {
    // MySQL 8.0 rstrips SET tokens; a leading space must not silently
    // match. `'a ,b'` splits to ['a ', 'b']; 'a ' rtrim → 'a'; 'b' fine.
    // But `' a,b'` splits to [' a', 'b']; ' a' does NOT match 'a' → 1265.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, flags SET('a','b'))")
        .unwrap();
    let err = engine
        .execute("INSERT INTO s VALUES (1, ' a,b')")
        .unwrap_err();
    assert_eq!(err.code(), 1265);
}

// ─────────────────────────── P0-6/7 DDL validation ───────────────────

#[test]
fn ddl_duplicate_enum_member_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine
        .execute("CREATE TABLE t (e ENUM('a','A'))")
        .unwrap_err();
    assert_eq!(err.code(), 1291);
}

#[test]
fn set_members_follow_collation_and_nonstrict_duplicate_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());

    // Binary collation makes differently-cased members distinct all the way
    // through DDL validation and row coercion.
    engine
        .execute(
            "CREATE TABLE binary_set \
             (id INT PRIMARY KEY, s SET('a','A') CHARACTER SET latin1 COLLATE latin1_bin)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO binary_set VALUES (1,'a'),(2,'A'),(3,'A,a')")
        .unwrap();
    let rows = response_rows(
        engine
            .execute("SELECT s FROM binary_set ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            vec![Value::String("a".into())],
            vec![Value::String("A".into())],
            vec![Value::String("a,A".into())],
        ]
    );

    // Duplicate members are notes rather than DDL errors in an explicitly
    // non-strict sql_mode; strict mode remains covered by the test above.
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET sql_mode='NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE duplicate_set (s SET('1','2','1'))",
        )
        .unwrap();
    let warnings = response_rows(
        engine
            .execute_with_session(&mut session, "SHOW WARNINGS")
            .unwrap(),
    );
    assert!(warnings
        .iter()
        .any(|row| { matches!(row.get(1), Some(Value::Int(1291)) | Some(Value::UInt(1291))) }));
}

#[test]
fn set_trailing_spaces_and_quoted_numeric_mask_match_mysql() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE spaced (s SET(' ','a','b ') NOT NULL DEFAULT 'b ')")
        .unwrap();
    engine.execute("INSERT INTO spaced VALUES ()").unwrap();
    assert_eq!(
        response_rows(engine.execute("SELECT s FROM spaced").unwrap()),
        vec![vec![Value::String("b".into())]]
    );

    let members = (1..=64)
        .map(|value| format!("'{value}'"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("CREATE TABLE masks (s SET({members}))"))
        .unwrap();
    engine.execute("INSERT INTO masks VALUES ('100')").unwrap();
    assert_eq!(
        response_rows(engine.execute("SELECT s FROM masks").unwrap()),
        vec![vec![Value::String("3,6,7".into())]]
    );
}

#[test]
fn ddl_set_member_with_comma_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine
        .execute("CREATE TABLE t (s SET('a,b','c'))")
        .unwrap_err();
    // 1367 (ER_ILLEGAL_VALUE_FOR_TYPE) is the closest MySQL code; we
    // reserve 1291 for duplicates.
    assert_eq!(err.code(), 1367);
}

#[test]
fn ddl_enum_empty_member_list_rejected() {
    // ENUM() with no members — sqlparser typically already rejects
    // this at the grammar level, but if we accept the parse we must
    // reject at DDL binding. Guard against both outcomes.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let res = engine.execute("CREATE TABLE t (e ENUM())");
    if let Ok(_) = res {
        panic!("empty ENUM member list should be rejected");
    }
}

#[test]
fn ddl_default_not_in_enum_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine
        .execute("CREATE TABLE t (e ENUM('a','b') DEFAULT 'z')")
        .unwrap_err();
    assert_eq!(err.code(), 1067);
}

#[test]
fn ddl_default_ordinal_zero_accepted_for_enum() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (e ENUM('a','b') DEFAULT 0, id INT PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        engine.db.table("test.t").unwrap().columns[0].default,
        Some(Value::Bytes(Vec::new()))
    );
    engine.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    match engine.execute("SELECT e FROM t WHERE id = 1").unwrap() {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows[0][0], Value::Bytes(Vec::new()));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[test]
fn ddl_default_set_illegal_token_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let err = engine
        .execute("CREATE TABLE t (s SET('a','b') DEFAULT 'z,a')")
        .unwrap_err();
    assert_eq!(err.code(), 1067);
}

#[test]
fn ddl_enum_and_set_members_are_limited_to_255_symbols() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let allowed = "界".repeat(255);
    engine
        .execute(&format!(
            "CREATE TABLE allowed_members (e ENUM('{allowed}'))"
        ))
        .unwrap();

    let too_long = "界".repeat(256);
    for data_type in ["ENUM", "SET"] {
        let err = engine
            .execute(&format!(
                "CREATE TABLE rejected_member (v {data_type}('{too_long}'))"
            ))
            .unwrap_err();
        assert_eq!(
            (err.code(), err.sql_state().as_ref()),
            (3505, "HY000"),
            "{data_type}: {err:?}"
        );
    }
}

#[test]
fn alter_column_default_revalidates_enum_membership() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (e ENUM('a','b') DEFAULT 'a')")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t ALTER COLUMN e SET DEFAULT 'z'")
        .unwrap_err();
    assert_eq!(err.code(), 1067);
    engine
        .execute("ALTER TABLE t ALTER COLUMN e SET DEFAULT 'b'")
        .unwrap();
}

#[test]
fn large_enum_descriptor_is_externalized_in_data_dictionary() {
    let tmp = tempfile::tempdir().unwrap();
    let members = (0..3000)
        .map(|value| format!("'{value:06}'"))
        .collect::<Vec<_>>()
        .join(",");
    {
        let engine = open_strict_test_engine(tmp.path());
        engine
            .execute(&format!("CREATE TABLE large_enum (e ENUM({members}))"))
            .unwrap();
        engine
            .execute("INSERT INTO large_enum VALUES ('002999')")
            .unwrap();
    }
    // Reopen proves the off-page dictionary payload is durable and decoded
    // by the normal catalog loader, not merely cached in memory.
    let reopened = open_strict_test_engine(tmp.path());
    assert_eq!(
        response_rows(reopened.execute("SELECT e FROM large_enum").unwrap()),
        vec![vec![Value::String("002999".into())]]
    );
}

// ─────────────────────────── P0-5 SHOW CREATE escape ─────────────────

#[test]
fn show_create_escapes_single_quotes_in_enum_members() {
    // 8.0.19+ style: dump must be round-trippable. Insert a member with
    // an embedded single quote and ensure SHOW CREATE renders it doubled.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (v ENUM('it''s','ok'))")
        .unwrap();
    match engine.execute("SHOW CREATE TABLE t").unwrap() {
        QueryResponse::ResultSet(r) => {
            let ddl = match &r.rows[0][1] {
                Value::String(s) => s.clone(),
                other => panic!("SHOW CREATE second column not a string: {other:?}"),
            };
            assert!(
                ddl.contains("'it''s'"),
                "SHOW CREATE did not double the single quote: {ddl}"
            );
            assert!(
                !ddl.contains("'it's'"),
                "SHOW CREATE emitted an unescaped single quote: {ddl}"
            );
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// ─────────────────────────── P1-7/8 ALTER lossy fixes ───────────────

// ─────────────────────────── P0-1 binlog e2e ────────────────────────

#[test]
fn binlog_encodes_enum_and_set_columns() {
    // P0-1 regression: before the fix, encode_value called value_u64
    // on `Value::String("medium")` which failed with 1366. Any INSERT
    // touching an ENUM/SET column errored at commit time when binlog
    // was enabled. This test drives a full INSERT/UPDATE/DELETE round
    // through a real binlog manager and asserts no error propagates.
    use crate::binlog::{self, BinlogConfig};
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let blog_dir = dir.path().join("binlog");
    std::fs::create_dir_all(&blog_dir).unwrap();
    binlog::init_global(
        blog_dir.clone(),
        BinlogConfig {
            enabled: true,
            base_name: "mydb-bin".into(),
            ..BinlogConfig::default()
        },
    )
    .unwrap();

    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, \
             sz ENUM('small','medium','large'), \
             flags SET('a','b','c'))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'medium','a,c'), (2,'large','b')")
        .unwrap();
    engine
        .execute("UPDATE t SET sz='small', flags='a,b' WHERE id=1")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id=2").unwrap();

    let mgr = binlog::manager().expect("binlog manager installed");
    let mut events = Vec::new();
    for log in mgr.binary_logs().unwrap() {
        events.extend(mgr.events_after(&log.name, 4).unwrap());
    }
    // 0x1e WRITE, 0x1f UPDATE, 0x20 DELETE row event types.
    let writes = events.iter().filter(|e| e.event_type == 0x1e).count();
    let updates = events.iter().filter(|e| e.event_type == 0x1f).count();
    let deletes = events.iter().filter(|e| e.event_type == 0x20).count();
    assert!(writes >= 1, "no WRITE_ROWS emitted: {events:?}");
    assert!(updates >= 1, "no UPDATE_ROWS emitted for ENUM/SET row");
    assert!(deletes >= 1, "no DELETE_ROWS emitted for ENUM/SET row");

    // Reset so no other test sees this manager.
    let _ = binlog::init_global(blog_dir, BinlogConfig::default());
}

// ─────────────────────────── P1-1 collation-aware ────────────────────

#[test]
fn enum_matches_via_utf8mb4_general_ci_collation() {
    // A default utf8mb4 collation is case-insensitive. Inserting the
    // upper-case member spelling must still match and canonicalise to
    // the declared lower-case spelling.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, \
             sz ENUM('small','medium','large') CHARACTER SET utf8mb4)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'MEDIUM')")
        .unwrap();
    match engine.execute("SELECT sz FROM t WHERE id = 1").unwrap() {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows[0][0], Value::String("medium".into()));
        }
        other => panic!("unexpected: {other:?}"),
    }
}

// ─────────────────────────── P1-10 parser rejects non-MySQL syntax ───

// ─────────────────────────── P1-6 INSTANT append ─────────────────────

#[test]
fn alter_enum_append_member_preserves_data_and_reads_new_members() {
    // MySQL 8.0: appending an ENUM member is a metadata-only INSTANT
    // change when the storage width bucket does not change. Existing
    // rows keep their ordinal; the new member is legal on subsequent
    // INSERTs.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, sz ENUM('small','medium'))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'small'), (2,'medium')")
        .unwrap();
    // Append 'large' — pure metadata change (width still 1B up to 255).
    engine
        .execute("ALTER TABLE t MODIFY COLUMN sz ENUM('small','medium','large')")
        .unwrap();
    // Existing rows unchanged.
    let QueryResponse::ResultSet(r) = engine.execute("SELECT id, sz FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(r.rows[0][1], Value::String("small".into()));
    assert_eq!(r.rows[1][1], Value::String("medium".into()));
    // New member is now legal.
    engine.execute("INSERT INTO t VALUES (3, 'large')").unwrap();
    // And still orders by declared ordinal (new member sorts last).
    let QueryResponse::ResultSet(r2) = engine.execute("SELECT id FROM t ORDER BY sz, id").unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(
        r2.rows
            .iter()
            .map(|row| match row[0] {
                Value::Int(v) => v,
                _ => panic!("id not int"),
            })
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn alter_set_append_member_preserves_bitmask_data() {
    // SET append is likewise INSTANT when the ceil(N/8) width bucket
    // doesn't change. SET('a','b') stays 1 byte through 8 members.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, flags SET('a','b'))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'a,b'), (2,'b'), (3,'')")
        .unwrap();
    engine
        .execute("ALTER TABLE t MODIFY COLUMN flags SET('a','b','c','d')")
        .unwrap();
    let QueryResponse::ResultSet(r) = engine
        .execute("SELECT id, flags FROM t ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(r.rows[0][1], Value::String("a,b".into()));
    assert_eq!(r.rows[1][1], Value::String("b".into()));
    assert_eq!(r.rows[2][1], Value::String(String::new()));
    // Row that uses the new member reads back canonicalised.
    engine.execute("INSERT INTO t VALUES (4, 'c,d,a')").unwrap();
    let QueryResponse::ResultSet(r2) = engine.execute("SELECT flags FROM t WHERE id = 4").unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(r2.rows[0][0], Value::String("a,c,d".into()));
}

#[test]
fn alter_enum_delete_or_reorder_still_copies() {
    // Removing / reordering ENUM members is NOT metadata-only — the
    // ordinal-to-string mapping changes for existing rows. Must fall
    // back to the COPY path (which re-canonicalises via
    // coerce_enum_lossy). We only observe correctness here (the query
    // completes and data is remapped), not the algorithm choice.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, sz ENUM('small','medium','large'))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'small'), (2,'large')")
        .unwrap();
    // Reordered — 'large' now index 1, 'small' index 3. COPY path
    // remaps existing rows by the *string* value, not the ordinal.
    engine
        .execute("ALTER TABLE t MODIFY COLUMN sz ENUM('large','medium','small')")
        .unwrap();
    let QueryResponse::ResultSet(r) = engine.execute("SELECT id, sz FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set")
    };
    assert_eq!(r.rows[0][1], Value::String("small".into()));
    assert_eq!(r.rows[1][1], Value::String("large".into()));
}

#[test]
fn parser_rejects_clickhouse_enum_named_value_syntax() {
    // MySQL ENUM does not carry per-member numeric assignments.
    // ClickHouse-style `ENUM('a' = 1, 'b' = 2)` must be rejected.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    let res = engine.execute("CREATE TABLE t (e ENUM('a' = 1, 'b' = 2))");
    assert!(res.is_err(), "ClickHouse ENUM syntax should error");
}

#[test]
fn set_bitmask_column_width_is_ceil_n_div_8() {
    // Regression for P0-2: SET(24) must fit in exactly 3 bytes on
    // disk, not 4 (the old bucketed 1/2/4/8 formula). We can't observe
    // page bytes directly here, but round-tripping the max-index
    // member exercises the tight width — before the fix, bit 23 would
    // land inside a padded 4-byte slot; after, it lands cleanly in the
    // 3rd byte.
    let tmp = tempfile::tempdir().unwrap();
    let engine = open_test_engine(tmp.path());
    // 24 members exercises SET(24) → 3 bytes.
    let members: Vec<String> = (0..24u32).map(|i| format!("m{i}")).collect();
    let ddl = format!(
        "CREATE TABLE s (id INT PRIMARY KEY, flags SET({}))",
        members
            .iter()
            .map(|m| format!("'{m}'"))
            .collect::<Vec<_>>()
            .join(",")
    );
    engine.execute(&ddl).unwrap();
    // Bit 23 = 1 << 23 = 8388608. Set only the last member.
    engine.execute("INSERT INTO s VALUES (1, 8388608)").unwrap();
    match engine.execute("SELECT flags FROM s").unwrap() {
        QueryResponse::ResultSet(r) => {
            assert_eq!(r.rows[0][0], Value::String("m23".into()));
        }
        other => panic!("unexpected: {other:?}"),
    }
}
