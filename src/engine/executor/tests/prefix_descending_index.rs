//! End-to-end coverage for MySQL 8.0 prefix indexes (`col(N)`) and
//! descending-direction index columns (`col DESC`).
//!
//! Phase 1 contract verified here:
//! * Prefix indexes truncate keys on every write path (INSERT / UPDATE /
//!   DELETE / LOAD DATA / ADD INDEX backfill / unique constraint), but the
//!   filter is re-checked at the heap so false positives never leak.
//! * UNIQUE prefix indexes follow MySQL semantics: two rows whose declared
//!   key columns share the same N-char prefix collide even when the
//!   suffixes differ.
//! * Descending column metadata round-trips through SHOW INDEX,
//!   SHOW CREATE TABLE, and information_schema.statistics, while
//!   `ORDER BY ... DESC` still drives an index reverse scan rather than an
//!   ad-hoc Sort. Mixed-direction composite indexes do NOT silently claim
//!   to satisfy mixed ORDER BY — the planner falls back to Sort, which is
//!   the only safe behaviour given Phase 1's physical-ASC storage layout.
use super::*;
use crate::types::Value;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected result set for: {sql}");
    };
    rs.rows
}

fn one_int_col(engine: &StorageQueryEngine, sql: &str) -> Vec<i64> {
    rows(engine, sql)
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Int(v)) => v,
            other => panic!("expected i64, got {other:?}"),
        })
        .collect()
}

fn explain_text(engine: &StorageQueryEngine, sql: &str) -> String {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected explain result set");
    };
    rs.rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|value| match value {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// --- Prefix index correctness ------------------------------------------

#[test]
fn prefix_index_equality_filters_false_positives_at_heap() {
    // The index truncates `name` to 2 chars, so both rows below share
    // the same B-tree key 'ab'. The IndexLookup's residual filter must
    // discard the unrelated row so `WHERE name = 'abXYZ'` is precise.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, INDEX idx_name (name(2)))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abXYZ'), (2, 'abCDE'), (3, 'cdAAA')")
        .unwrap();

    let explain = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE name = 'abXYZ'");
    assert!(
        explain.contains("idx_name"),
        "explain should reference the prefix index: {explain}"
    );

    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'abXYZ'"),
        vec![1]
    );
    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'abCDE'"),
        vec![2]
    );
    // Sanity: filter that matches no row even after the prefix scan is
    // honest about it.
    assert!(rows(&engine, "SELECT id FROM t WHERE name = 'abZZZ'").is_empty());
}

#[test]
fn prefix_index_unique_collides_on_prefix_match() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, UNIQUE INDEX uq_prefix (name(3)))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abcdef')")
        .unwrap();

    // Different suffix but identical first 3 chars → duplicate.
    let err = engine
        .execute("INSERT INTO t VALUES (2, 'abcxyz')")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1062,
        "expected duplicate-key (1062), got {err:?}"
    );
    assert!(
        err.message().contains("uq_prefix"),
        "error should name the violating index: {err:?}"
    );

    // Same row inserted again at a non-overlapping prefix is fine.
    engine
        .execute("INSERT INTO t VALUES (3, 'xyzabc')")
        .unwrap();

    let mut ids = one_int_col(&engine, "SELECT id FROM t ORDER BY id");
    ids.sort();
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn prefix_index_update_rebuilds_key_on_value_change() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, INDEX idx_name (name(2)))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'aaXYZ'), (2, 'bbXYZ')")
        .unwrap();

    // Update changes the *prefix* — the old entry under 'aa' must be
    // gone, the new one under 'cc' must be visible.
    engine
        .execute("UPDATE t SET name = 'ccXYZ' WHERE id = 1")
        .unwrap();

    assert!(rows(&engine, "SELECT id FROM t WHERE name = 'aaXYZ'").is_empty());
    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'ccXYZ'"),
        vec![1]
    );
    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'bbXYZ'"),
        vec![2]
    );

    // Update where the prefix stays the same but the suffix flips —
    // the B-tree key doesn't change, but the heap row does, so the
    // residual filter on `name = ...` must still pick the right row.
    engine
        .execute("UPDATE t SET name = 'ccZZZ' WHERE id = 1")
        .unwrap();
    assert!(rows(&engine, "SELECT id FROM t WHERE name = 'ccXYZ'").is_empty());
    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'ccZZZ'"),
        vec![1]
    );
}

#[test]
fn prefix_index_delete_clears_btree_entry() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, INDEX idx_name (name(2)))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abXYZ'), (2, 'abCDE')")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 1").unwrap();

    assert!(rows(&engine, "SELECT id FROM t WHERE name = 'abXYZ'").is_empty());
    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'abCDE'"),
        vec![2]
    );
}

#[test]
fn prefix_index_add_after_data_backfills_truncated_keys() {
    // ADD INDEX backfill must produce the same truncated keys as the
    // live insert path; this is the regression check for the v2 batched
    // backfill (it explicitly calls `apply_key_part_prefix`).
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abcXYZ'), (2, 'abcCDE'), (3, 'zzzzzz')")
        .unwrap();
    engine
        .execute("CREATE INDEX idx_name ON t (name(3))")
        .unwrap();

    let explain = explain_text(&engine, "EXPLAIN SELECT id FROM t WHERE name = 'abcXYZ'");
    assert!(
        explain.contains("idx_name"),
        "post-backfill prefix index must be selectable: {explain}"
    );

    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'abcXYZ'"),
        vec![1]
    );
    assert_eq!(
        one_int_col(&engine, "SELECT id FROM t WHERE name = 'abcCDE'"),
        vec![2]
    );
}

#[test]
fn prefix_index_unique_constraint_survives_add_index_backfill() {
    // CREATE UNIQUE INDEX on existing rows whose prefixes collide must
    // fail at backfill — not silently accept and then later report odd
    // results.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abcXYZ'), (2, 'abcCDE')")
        .unwrap();

    let err = engine
        .execute("CREATE UNIQUE INDEX uq_name ON t (name(3))")
        .unwrap_err();
    assert_eq!(
        err.code(),
        1062,
        "expected duplicate-key (1062) at backfill, got {err:?}"
    );
}

#[test]
fn prefix_index_range_falls_back_on_prefix_column() {
    // A range predicate on a prefix-indexed column would normally pin
    // the B-tree's range boundaries to the *truncated* key, which can't
    // satisfy `BETWEEN` against the full string. The planner correctly
    // refuses to use the index for the range here — the result still
    // has to be precise.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, INDEX idx_name (name(2)))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, 'aaa'), (2, 'aab'), (3, 'aba'), (4, 'abb'), \
             (5, 'baa'), (6, 'bab'), (7, 'bba'), (8, 'bbb')",
        )
        .unwrap();
    let mut got = one_int_col(
        &engine,
        "SELECT id FROM t WHERE name >= 'aab' AND name <= 'baa' ORDER BY id",
    );
    got.sort();
    assert_eq!(got, vec![2, 3, 4, 5]);
}

#[test]
fn min_max_do_not_return_truncated_prefix_index_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (
                id INT PRIMARY KEY,
                name VARCHAR(32) NOT NULL,
                INDEX idx_name (name(4))
            )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'aaaabbbb'), (2, 'aaaacccc'), (3, 'zzzzzzzz')")
        .unwrap();

    let result = rows(&engine, "SELECT MIN(name), MAX(name) FROM t");
    assert_eq!(
        result,
        vec![vec![
            Value::String("aaaabbbb".to_string()),
            Value::String("zzzzzzzz".to_string()),
        ]]
    );
    let explain = explain_text(&engine, "EXPLAIN SELECT MAX(name) FROM t");
    assert!(
        !explain.contains("Using index for aggregate"),
        "prefix keys cannot satisfy MIN/MAX without heap tie resolution: {explain}"
    );
}

#[test]
fn numeric_predicates_on_varchar_do_not_use_character_index_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (x VARCHAR(2), KEY idx_x (x))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('01'), ('1'), ('3'), ('5'), ('7'), ('9'), ('11'), ('13')")
        .unwrap();

    assert_eq!(
        rows(&engine, "SELECT MIN(x) FROM t WHERE x BETWEEN 9 AND 20"),
        vec![vec![Value::String("11".to_string())]]
    );
    assert_eq!(
        rows(&engine, "SELECT MIN(x) FROM t WHERE x = 1"),
        vec![vec![Value::String("01".to_string())]]
    );
    let explain = explain_text(&engine, "EXPLAIN SELECT x FROM t WHERE x = 1");
    assert!(
        explain.lines().any(|line| line == "ALL"),
        "numeric VARCHAR predicate must fall back to a full scan: {explain}"
    );
}

#[test]
fn prefix_index_is_null_predicate_works() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), INDEX idx_name (name(3)))")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'alpha'), (2, NULL), (3, 'alphaXX'), (4, NULL)")
        .unwrap();

    let mut nulls = one_int_col(&engine, "SELECT id FROM t WHERE name IS NULL ORDER BY id");
    nulls.sort();
    assert_eq!(nulls, vec![2, 4]);

    let mut alphas = one_int_col(&engine, "SELECT id FROM t WHERE name = 'alpha' ORDER BY id");
    alphas.sort();
    assert_eq!(alphas, vec![1]);
}

// --- Descending column direction ---------------------------------------

#[test]
fn descending_single_column_index_satisfies_order_by_desc_via_reverse_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT NOT NULL, INDEX idx_v_desc (v DESC))")
        .unwrap();
    for i in 1..=20 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }

    // ORDER BY ... DESC LIMIT must drive an IndexScan with reverse=true
    // rather than a Sort node. We assert there's no Sort in the plan.
    let explain = explain_text(&engine, "EXPLAIN SELECT v FROM t ORDER BY v DESC LIMIT 5");
    assert!(
        explain.contains("idx_v_desc"),
        "DESC index should be used: {explain}"
    );
    assert!(
        !explain.to_lowercase().contains("sort"),
        "ORDER BY ... DESC LIMIT on a DESC index must avoid a Sort node: {explain}"
    );

    assert_eq!(
        one_int_col(&engine, "SELECT v FROM t ORDER BY v DESC LIMIT 5"),
        vec![20, 19, 18, 17, 16]
    );
    assert_eq!(
        one_int_col(&engine, "SELECT v FROM t ORDER BY v ASC LIMIT 5"),
        vec![1, 2, 3, 4, 5]
    );
}

#[test]
fn descending_index_metadata_round_trips_to_show_and_information_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             a INT NOT NULL, \
             b VARCHAR(20) NOT NULL, \
             INDEX idx_mix (a DESC, b(3) DESC))",
        )
        .unwrap();

    // SHOW INDEX direction column ("Collation") -- 'D' for DESC, 'A' for ASC.
    let show = rows(&engine, "SHOW INDEX FROM t");
    let mix_rows: Vec<_> = show
        .iter()
        .filter(|row| matches!(&row[2], Value::String(s) if s == "idx_mix"))
        .collect();
    assert_eq!(mix_rows.len(), 2);
    for row in &mix_rows {
        assert_eq!(row[5], Value::String("D".into()), "expected DESC marker");
    }
    // Sub_part column for the second key part should be 3.
    assert_eq!(mix_rows[1][7], Value::Int(3));
    assert_eq!(mix_rows[0][7], Value::Null);

    // SHOW CREATE TABLE round-trips DESC + (3) syntax.
    let create = rows(&engine, "SHOW CREATE TABLE t");
    let Value::String(sql) = &create[0][1] else {
        panic!("expected sql string");
    };
    assert!(
        sql.contains("KEY `idx_mix` (`a` DESC, `b`(3) DESC)"),
        "SHOW CREATE TABLE missing DESC/prefix syntax: {sql}"
    );

    // information_schema.statistics also exposes COLLATION='D' and SUB_PART=3.
    let stats = rows(
        &engine,
        "SELECT collation, sub_part FROM information_schema.statistics \
         WHERE table_name = 't' AND index_name = 'idx_mix' \
         ORDER BY seq_in_index",
    );
    assert_eq!(
        stats,
        vec![
            vec![Value::String("D".into()), Value::Null],
            vec![Value::String("D".into()), Value::Int(3)],
        ]
    );
}

#[test]
fn descending_composite_index_serves_uniform_desc_order_by() {
    // `INDEX (a DESC, b DESC)` declares uniform DESC direction. Phase 1
    // stores keys physically ASC but identifies a reverse scan as the
    // correct way to provide ORDER BY a DESC, b DESC -- so no Sort.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             a INT NOT NULL, b INT NOT NULL, \
             INDEX idx_ab_desc (a DESC, b DESC))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, 1, 1), (2, 1, 2), (3, 2, 1), (4, 2, 2), \
             (5, 3, 1), (6, 3, 2)",
        )
        .unwrap();

    let explain = explain_text(
        &engine,
        "EXPLAIN SELECT a, b FROM t ORDER BY a DESC, b DESC LIMIT 3",
    );
    assert!(
        !explain.to_lowercase().contains("sort"),
        "uniform-DESC index should avoid Sort: {explain}"
    );

    let result = rows(
        &engine,
        "SELECT a, b FROM t ORDER BY a DESC, b DESC LIMIT 3",
    );
    assert_eq!(
        result,
        vec![
            vec![Value::Int(3), Value::Int(2)],
            vec![Value::Int(3), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
        ]
    );
}

#[test]
fn mixed_direction_composite_index_does_not_falsely_satisfy_mixed_order_by() {
    // `INDEX (a ASC, b DESC)` is physically ASC-only in Phase 1. The
    // planner must NOT pretend it satisfies `ORDER BY a ASC, b DESC` —
    // doing so would emit rows in the wrong order. A Sort node is the
    // correct (if less efficient) outcome.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             a INT NOT NULL, b INT NOT NULL, \
             INDEX idx_mix (a ASC, b DESC))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, 1, 1), (2, 1, 2), (3, 1, 3), \
             (4, 2, 1), (5, 2, 2)",
        )
        .unwrap();

    // Result correctness is the hard contract here; whether the plan
    // uses Sort or the index for ordering is a CBO choice, but the rows
    // must come out as the SQL spec requires.
    let result = rows(&engine, "SELECT a, b FROM t ORDER BY a ASC, b DESC");
    assert_eq!(
        result,
        vec![
            vec![Value::Int(1), Value::Int(3)],
            vec![Value::Int(1), Value::Int(2)],
            vec![Value::Int(1), Value::Int(1)],
            vec![Value::Int(2), Value::Int(2)],
            vec![Value::Int(2), Value::Int(1)],
        ]
    );
}

#[test]
fn descending_index_string_column_orders_correctly() {
    // DESC on a string column: phase 1 reverse scan over physical-ASC
    // storage must still produce strict reverse lexicographic order.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             name VARCHAR(20) NOT NULL, \
             INDEX idx_name_desc (name DESC))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta')")
        .unwrap();

    let result: Vec<String> = rows(&engine, "SELECT name FROM t ORDER BY name DESC")
        .into_iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }.to_string(),
            other => panic!("expected string, got {other:?}"),
        })
        .collect();
    assert_eq!(result, vec!["gamma", "delta", "beta", "alpha"]);
}

#[test]
fn descending_index_with_prefix_truncates_and_orders_correctly() {
    // Combined: prefix + DESC on a string column. ORDER BY name DESC can
    // be ambiguous beyond the prefix (the index alone can't decide between
    // values that share the first N chars), but a reverse scan still gives
    // a correct result *after* a heap-side tie-breaker has run. Phase 1
    // tolerates a Sort here; the correctness contract is what's tested.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (\
             id INT PRIMARY KEY, \
             name VARCHAR(20) NOT NULL, \
             INDEX idx_name (name(2) DESC))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abXYZ'), (2, 'abCDE'), (3, 'zzAAA'), (4, 'azBBB')")
        .unwrap();

    // Result rows by descending name must be lex-sorted.
    let result: Vec<String> = rows(&engine, "SELECT name FROM t ORDER BY name DESC")
        .into_iter()
        .map(|row| match &row[0] {
            Value::String(s) => s.clone(),
            Value::SmallString(b) => unsafe { std::str::from_utf8_unchecked(b) }.to_string(),
            other => panic!("expected string, got {other:?}"),
        })
        .collect();
    assert_eq!(result, vec!["zzAAA", "azBBB", "abXYZ", "abCDE"]);
}

// --- Prepared-statement parameter routing through a prefix index -------

#[test]
fn prepared_param_lookup_routes_through_prefix_index_and_filters_at_heap() {
    // Phase 2.5: planner emits `IndexLookup { key: [Param(...)] }`. The
    // storage boundary truncates the resolved Param to the index's
    // declared prefix length before probing the B-tree, then the
    // residual filter at the heap rejects rows whose full value differs
    // from the bound parameter. Prior to this wire-up the planner
    // bailed and fell back to a seq scan, which under TPC-C-style
    // prepared-statement contention is catastrophic.
    use crate::engine::api::QueryEngine as _;
    use crate::engine::executor::indexes::{best_index_access, EngineIndexAccess};
    use crate::engine::params::with_params;
    use crate::parser::{number_statement_placeholders, parse_statement, Statement};
    use crate::storage::KeyPart;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, \
             INDEX idx_name (name(3)))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'abcXYZ'), (2, 'abcCDE'), (3, 'zzzAAA')")
        .unwrap();

    // First check: the index-access selector recognises the prefix
    // index for a placeholder-driven equality predicate. (Before this
    // wiring, it bailed and returned None.)
    let table = engine.db_for_tests().table("t").unwrap();
    let mut stmt = parse_statement("SELECT id FROM t WHERE name = ?").unwrap();
    let mut next = 0;
    number_statement_placeholders(&mut stmt, &mut next);
    let Statement::Select(select) = stmt else {
        panic!("expected Select");
    };
    let where_expr = select.selection.as_ref().expect("WHERE clause");
    let (index, access) =
        best_index_access(&table, where_expr).expect("placeholder must pick the prefix index");
    assert_eq!(
        index.name, "idx_name",
        "expected the prefix index, got {:?}",
        index.name
    );
    let EngineIndexAccess::Lookup { key, exact } = access else {
        panic!("expected Lookup variant from prefix-index placeholder probe");
    };
    assert!(!exact, "prefix index lookup must be marked non-exact");
    assert!(
        matches!(key.0[0], KeyPart::Param { idx: 0, .. }),
        "prefix lookup must defer truncation until param bind, got {:?}",
        key.0[0]
    );

    // Second check: end-to-end prepared lookup. The storage boundary
    // truncates the resolved Param against `idx_name`'s 3-char prefix,
    // and the residual filter at the heap distinguishes 'abcXYZ' from
    // 'abcCDE' even though both share the same B-tree key.
    let mut session = SessionContext::default();
    let sql = "SELECT id FROM t WHERE name = ?";
    let prepared = engine.prepare_cached(&mut session, sql, 1).unwrap();

    let bind_and_select = |bound: &str| -> Vec<Vec<Value>> {
        let stmt = (*prepared).clone();
        let mut sess = session.clone();
        let response = with_params(vec![Value::String(bound.to_string())], || {
            engine
                .execute_prepared_statement(&mut sess, sql, stmt)
                .unwrap()
                .unwrap()
        });
        let response = response.materialize().unwrap();
        let QueryResponse::ResultSet(rs) = response else {
            panic!("expected result set");
        };
        rs.rows
    };
    assert_eq!(bind_and_select("abcXYZ"), vec![vec![Value::Int(1)]]);
    assert_eq!(bind_and_select("abcCDE"), vec![vec![Value::Int(2)]]);
    // Bound value whose prefix exists but whose full value matches
    // nothing must return zero rows (heap filter rejects false positives).
    assert!(bind_and_select("abcZZZ").is_empty());
    // Bound value whose prefix doesn't exist at all is also zero rows.
    assert!(bind_and_select("qqqqqq").is_empty());
}

// --- Foreign key over prefix-bearing parent index ----------------------

#[test]
fn foreign_key_referencing_prefix_indexed_column_is_rejected_at_create() {
    // MySQL forbids declaring a foreign key against a column with a
    // prefix index because the prefix index can't uniquely identify the
    // referenced row. We make sure that constraint is enforced rather
    // than silently allowing an ambiguous reference.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE p (\
             id INT PRIMARY KEY, \
             name VARCHAR(20) NOT NULL, \
             UNIQUE INDEX uq_name (name(3)))",
        )
        .unwrap();

    let err = engine
        .execute(
            "CREATE TABLE c (\
             id INT PRIMARY KEY, \
             parent_name VARCHAR(20) NOT NULL, \
             CONSTRAINT fk_c FOREIGN KEY (parent_name) REFERENCES p(name))",
        )
        .unwrap_err();
    // Either a "missing index" (1822) or a generic FK-related error is
    // acceptable; what's NOT acceptable is silently creating the FK.
    assert!(
        err.code() != 0,
        "FK over prefix-only index should not silently succeed: {err:?}"
    );
}

#[test]
fn mixed_order_by_over_index_prefix_uses_incremental_sort() {
    // Phase-1 physical ASC layout cannot fully satisfy ORDER BY a ASC,b DESC,
    // but forward (a,b) index order already provides the a ASC prefix; choose
    // IncrementalSort(presorted=1) rather than a full sort.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, KEY k_ab (a, b))")
        .unwrap();
    let mut insert = String::from("INSERT INTO t VALUES ");
    for i in 0..200 {
        if i > 0 {
            insert.push(',');
        }
        insert.push_str(&format!("({}, {}, {})", i, i % 10, i % 7));
    }
    engine.execute(&insert).unwrap();
    let explain = explain_text(
        &engine,
        "EXPLAIN SELECT a, b FROM t WHERE a >= 0 ORDER BY a ASC, b DESC",
    );
    assert!(
        explain.to_ascii_lowercase().contains("incremental"),
        "mixed ORDER BY over (a,b) index prefix should use incremental sort: {explain}"
    );
    // Correctness: match total order from the unindexed reference table.
    let sorted = rows(
        &engine,
        "SELECT a, b FROM t WHERE a >= 0 ORDER BY a ASC, b DESC, id",
    );
    let mut expected = rows(&engine, "SELECT a, b FROM t");
    expected.sort_by(|l, r| {
        let (la, lb) = (&l[0], &l[1]);
        let (ra, rb) = (&r[0], &r[1]);
        let key = |v: &Value| match v {
            Value::Int(x) => *x,
            _ => unreachable!(),
        };
        key(la).cmp(&key(ra)).then(key(rb).cmp(&key(lb)))
    });
    assert_eq!(sorted.len(), expected.len());
    for (s, e) in sorted.iter().zip(expected.iter()) {
        assert_eq!(s[0], e[0]);
        assert_eq!(s[1], e[1]);
    }
}

#[test]
fn write_index_ranges_match_scan_for_cross_type_bounds() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE bounds_refactor (id INT PRIMARY KEY, s VARCHAR(32), n INT, marked INT DEFAULT 0, INDEX s_idx(s), INDEX n_idx(n))").unwrap();
    engine.execute("INSERT INTO bounds_refactor(id,s,n) VALUES (1,'9',9),(2,'10',10),(3,'20',20),(4,'0010',10),(5,'0',0),(6,NULL,NULL)").unwrap();
    for predicate in [
        "s BETWEEN 9 AND 20",
        "s >= 9",
        "s = 10",
        "10 = s",
        "n < 9.5",
        "n >= 9.5",
        "s LIKE '00%'",
        "s IS NULL",
    ] {
        let expected = rows(&engine, &format!("SELECT id FROM bounds_refactor IGNORE INDEX(s_idx,n_idx) WHERE {predicate} ORDER BY id"));
        engine
            .execute("UPDATE bounds_refactor SET marked=0")
            .unwrap();
        engine
            .execute(&format!(
                "UPDATE bounds_refactor SET marked=1 WHERE {predicate}"
            ))
            .unwrap();
        let actual = rows(
            &engine,
            "SELECT id FROM bounds_refactor WHERE marked=1 ORDER BY id",
        );
        assert_eq!(actual, expected, "UPDATE candidate bounds: {predicate}");
        let all = rows(&engine, "SELECT id FROM bounds_refactor ORDER BY id");
        let remaining: Vec<_> = all
            .into_iter()
            .filter(|row| !expected.contains(row))
            .collect();
        engine
            .execute(&format!("DELETE FROM bounds_refactor WHERE {predicate}"))
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT id FROM bounds_refactor ORDER BY id"),
            remaining,
            "DELETE candidate bounds: {predicate}"
        );
        engine.execute("TRUNCATE TABLE bounds_refactor").unwrap();
        engine.execute("INSERT INTO bounds_refactor(id,s,n) VALUES (1,'9',9),(2,'10',10),(3,'20',20),(4,'0010',10),(5,'0',0),(6,NULL,NULL)").unwrap();
    }
    engine
        .execute("INSERT INTO bounds_refactor(id,s,n) VALUES (7,'bad',0)")
        .unwrap();
    let before = rows(&engine, "SELECT id,marked FROM bounds_refactor ORDER BY id");
    let err = engine
        .execute("UPDATE bounds_refactor SET marked=1 WHERE s BETWEEN 9 AND 20")
        .unwrap_err();
    assert_eq!(err.code(), 1292);
    assert_eq!(
        rows(&engine, "SELECT id,marked FROM bounds_refactor ORDER BY id"),
        before
    );
}
