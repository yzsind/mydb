//! ORDER BY NULLS FIRST/LAST regressions: this is a mydb extension rejected by MySQL 8.0
//! with 1064. Defaults match MySQL (ASC NULL first, DESC NULL last); explicit clauses
//! override them. Cover row Sort/TopK, vector TopN, disabling incompatible index ordering,
//! windows, internal GROUP_CONCAT sorting, UPDATE/DELETE ORDER BY LIMIT,
//! outer UNION sorting, and derived-table flattening.
//!
//! Also guard legacy GROUP_CONCAT/window partition sorting, which used WHERE comparator
//! compare_values that returned Less for NULL in both directions, violating antisymmetry.

use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

fn rows_of(response: QueryResponse) -> Vec<Vec<Value>> {
    match response {
        QueryResponse::ResultSet(r) => r.rows,
        QueryResponse::StreamingResultSet(mut r) => {
            let mut out = Vec::new();
            while let Ok(Some(row)) = r.cursor.next_row() {
                out.push(row);
            }
            out
        }
        other => panic!("expected result set, got {other:?}"),
    }
}

fn setup_t(engine: &StorageQueryEngine) {
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, s VARCHAR(16))")
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             (1, 3, 'c'), (2, NULL, 'a'), (3, 1, NULL), (4, NULL, 'b'), (5, 2, 'a')",
        )
        .unwrap();
}

fn ids(rows: &[Vec<Value>]) -> Vec<i64> {
    rows.iter()
        .map(|r| match &r[0] {
            Value::Int(v) => *v,
            other => panic!("expected int id, got {other:?}"),
        })
        .collect()
}

#[test]
fn nulls_last_asc_overrides_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // Default ASC places NULL first, IDs 2/4 before others.
    let rows = rows_of(engine.execute("SELECT id FROM t ORDER BY v, id").unwrap());
    assert_eq!(ids(&rows), vec![2, 4, 3, 5, 1]);
    // NULLS LAST moves NULL to the end while non-NULL values remain ascending.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY v NULLS LAST, id")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![3, 5, 1, 2, 4]);
}

#[test]
fn nulls_first_desc_overrides_default() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // Default DESC places NULL last.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY v DESC, id")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![1, 5, 3, 2, 4]);
    // DESC NULLS FIRST moves NULL to the front.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY v DESC NULLS FIRST, id")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![2, 4, 1, 5, 3]);
}

#[test]
fn explicit_default_placement_matches_implicit() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    for (explicit, implicit) in [
        ("ORDER BY v NULLS FIRST, id", "ORDER BY v, id"),
        ("ORDER BY v DESC NULLS LAST, id", "ORDER BY v DESC, id"),
    ] {
        let lhs = rows_of(
            engine
                .execute(&format!("SELECT id FROM t {explicit}"))
                .unwrap(),
        );
        let rhs = rows_of(
            engine
                .execute(&format!("SELECT id FROM t {implicit}"))
                .unwrap(),
        );
        assert_eq!(lhs, rhs, "{explicit} vs {implicit}");
    }
}

#[test]
fn multi_key_mixed_directions_and_placements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // s ASC NULLS LAST orders a,a,b,c,NULL; within s, use v DESC NULLS FIRST.
    // For s='a', id2 with NULL v precedes id5 with v=2.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY s NULLS LAST, v DESC NULLS FIRST")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![2, 5, 4, 1, 3]);
}

#[test]
fn topk_limit_honours_custom_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // LIMIT uses the TopK heap; NULLS LAST returns v=1/2 before NULL.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY v NULLS LAST, id LIMIT 2")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![3, 5]);
    // OFFSET crosses the non-NULL segment into the NULL segment.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY v NULLS LAST, id LIMIT 2 OFFSET 3")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![2, 4]);
}

#[test]
fn vec_and_row_paths_agree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    let sql = "SELECT id FROM t ORDER BY v NULLS LAST, id LIMIT 4";
    let row_path = with_vec_mode(VecMode::Off, || rows_of(engine.execute(sql).unwrap()));
    let vec_path = with_vec_mode(VecMode::On, || rows_of(engine.execute(sql).unwrap()));
    assert_eq!(row_path, vec_path);
    assert_eq!(ids(&row_path), vec![3, 5, 1, 2]);
}

#[test]
fn indexed_column_does_not_reuse_index_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    engine.execute("CREATE INDEX idx_v ON t (v)").unwrap();
    // Forward index order puts NULL first; NULLS LAST requires an explicit sort.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t ORDER BY v NULLS LAST, id")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![3, 5, 1, 2, 4]);
    // Range predicates plus custom NULL placement also cannot bypass sorting via index order.
    let rows = rows_of(
        engine
            .execute("SELECT id FROM t WHERE v IS NULL OR v >= 1 ORDER BY v NULLS LAST, id")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![3, 5, 1, 2, 4]);
}

#[test]
fn window_order_by_custom_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // RANK under v ASC NULLS LAST gives 1->1, 2->2, 3->3, NULL->4 with ties.
    let rows = rows_of(
        engine
            .execute("SELECT id, RANK() OVER (ORDER BY v NULLS LAST) AS r FROM t ORDER BY id")
            .unwrap(),
    );
    let ranks: Vec<i64> = rows
        .iter()
        .map(|r| match &r[1] {
            Value::Int(v) => *v,
            Value::UInt(v) => *v as i64,
            other => panic!("expected int rank, got {other:?}"),
        })
        .collect();
    assert_eq!(ranks, vec![3, 4, 1, 4, 2]);
}

#[test]
fn group_concat_inner_order_custom_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // Default v ASC places NULL sort keys first; NULLS LAST moves them to the end.
    // Use s as the value and v as the key; id3 has NULL s and is omitted
    // because GROUP_CONCAT skips NULL values.
    let rows = rows_of(
        engine
            .execute("SELECT GROUP_CONCAT(s ORDER BY v NULLS LAST, id) FROM t")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::String("a,c,a,b".into()));
    // Default-order control places NULL sort keys first.
    let rows = rows_of(
        engine
            .execute("SELECT GROUP_CONCAT(s ORDER BY v, id) FROM t")
            .unwrap(),
    );
    assert_eq!(rows[0][0], Value::String("a,b,a,c".into()));
}

#[test]
fn update_delete_order_by_custom_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    // NULLS LAST starts with v=1/id3; default starts with NULL/id2.
    engine
        .execute("UPDATE t SET s = 'hit' ORDER BY v NULLS LAST, id LIMIT 1")
        .unwrap();
    let rows = rows_of(engine.execute("SELECT id FROM t WHERE s = 'hit'").unwrap());
    assert_eq!(ids(&rows), vec![3]);
    engine
        .execute("DELETE FROM t ORDER BY v DESC NULLS FIRST, id LIMIT 1")
        .unwrap();
    // DESC NULLS FIRST starts with id2, whose v is NULL.
    let rows = rows_of(engine.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(ids(&rows), vec![1, 3, 4, 5]);
}

#[test]
fn union_order_by_custom_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    let rows = rows_of(
        engine
            .execute(
                "SELECT v FROM t WHERE id <= 2 UNION ALL SELECT v FROM t WHERE id >= 4 \
                 ORDER BY v NULLS LAST",
            )
            .unwrap(),
    );
    let vals: Vec<Value> = rows.into_iter().map(|mut r| r.remove(0)).collect();
    assert_eq!(
        vals,
        vec![Value::Int(2), Value::Int(3), Value::Null, Value::Null]
    );
}

#[test]
fn derived_table_flatten_preserves_placement() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_t(&engine);
    let rows = rows_of(
        engine
            .execute("SELECT id FROM (SELECT id, v FROM t) AS d ORDER BY d.v NULLS LAST, d.id")
            .unwrap(),
    );
    assert_eq!(ids(&rows), vec![3, 5, 1, 2, 4]);
}

/// MYDB_ORDER_BY_NULLS=last selects PostgreSQL-style process defaults, treating NULL as greatest.
/// Without explicit clauses, ASC puts NULL last and DESC first; explicit clauses still override.
/// Index columns also require actual sorting rather than native B-tree order reuse.
#[test]
fn pg_style_default_places_nulls_high() {
    crate::util::env::with_vars(&[("MYDB_ORDER_BY_NULLS", Some("last"))], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_test_engine(dir.path());
        setup_t(&engine);

        // Default ASC now puts NULL last; MySQL would place IDs 2/4 first.
        let rows = rows_of(engine.execute("SELECT id FROM t ORDER BY v, id").unwrap());
        assert_eq!(ids(&rows), vec![3, 5, 1, 2, 4]);

        // Default DESC now puts NULL first; MySQL would place NULL last.
        let rows = rows_of(
            engine
                .execute("SELECT id FROM t ORDER BY v DESC, id")
                .unwrap(),
        );
        assert_eq!(ids(&rows), vec![2, 4, 1, 5, 3]);

        // Explicit clauses still override process defaults.
        let rows = rows_of(
            engine
                .execute("SELECT id FROM t ORDER BY v NULLS FIRST, id")
                .unwrap(),
        );
        assert_eq!(ids(&rows), vec![2, 4, 3, 5, 1]);

        // For indexed PK ORDER BY, PostgreSQL defaults differ from native NULL-low B-tree order,
        // requiring actual sorting. With non-NULL IDs the result stays 1..5 but follows the sort path,
        // guarding has_custom_nulls against incorrectly reusing index order.
        let rows = rows_of(engine.execute("SELECT id FROM t ORDER BY id").unwrap());
        assert_eq!(ids(&rows), vec![1, 2, 3, 4, 5]);
    });
}

/// Vector TopN and row execution agree under PostgreSQL-style defaults.
#[test]
fn pg_style_default_vec_and_row_agree() {
    crate::util::env::with_vars(&[("MYDB_ORDER_BY_NULLS", Some("last"))], || {
        for mode in [VecMode::Off, VecMode::On] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            setup_t(&engine);
            let rows = with_vec_mode(mode, || {
                rows_of(
                    engine
                        .execute("SELECT id FROM t ORDER BY v, id LIMIT 3")
                        .unwrap(),
                )
            });
            assert_eq!(ids(&rows), vec![3, 5, 1], "mode={mode:?}");
        }
    });
}

#[test]
fn group_concat_order_resolves_column_and_explicit_collation() {
    for mode in [VecMode::Off, VecMode::On] {
        with_vec_mode(mode, || {
            let dir = tempfile::tempdir().unwrap();
            let engine = open_test_engine(dir.path());
            engine
                .execute("CREATE TABLE collated_order (c VARCHAR(32) COLLATE utf8mb4_bin)")
                .unwrap();
            engine
                .execute("INSERT INTO collated_order VALUES ('a'), ('Z'), ('b'), ('A')")
                .unwrap();
            for (order, expected) in [
                ("c", "A,Z,a,b"),
                ("c DESC", "b,a,Z,A"),
                ("c COLLATE utf8mb4_bin", "A,Z,a,b"),
                ("c COLLATE utf8mb4_general_ci, c", "A,a,b,Z"),
            ] {
                let sql = format!("SELECT GROUP_CONCAT(c ORDER BY {order}) FROM collated_order");
                assert_eq!(
                    rows_of(engine.execute(&sql).unwrap()),
                    vec![vec![Value::String(expected.into())]],
                    "{sql}"
                );
            }
            let sql = "SELECT GROUP_CONCAT(c ORDER BY c COLLATE utf8mb4_bin) FROM (SELECT 'a' AS c UNION ALL SELECT 'Z' UNION ALL SELECT 'b' UNION ALL SELECT 'A') q";
            let result = rows_of(engine.execute(sql).unwrap());
            assert_eq!(crate::storage::value_to_string(&result[0][0]), "A,Z,a,b");
        });
    }
}
