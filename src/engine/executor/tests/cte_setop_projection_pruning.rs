//! Projection narrowing for set-operation CTE/derived bodies.
//!
//! `prune_unused_cte_columns` drops output columns that no consumer reads.
//! Previously it only fired on a plain `SELECT` body; an all-`UNION ALL` body
//! now narrows every branch consistently. `UNION`/`INTERSECT`/`EXCEPT` without
//! ALL (and any per-branch DISTINCT) deduplicate over the full output tuple,
//! so dropping a column would change which rows survive — those must NOT be
//! narrowed.
//!
//! Two layers:
//!  * AST-level: run `prune_unused_cte_columns` and assert the post-rewrite
//!    branch arities (positive narrowing + every dedup carve-out untouched).
//!  * Execution-level: the same query with `MYDB_CTE_PUSHDOWN` on vs off must
//!    return byte-identical rows — narrowing is a pure plan-shape change.

use super::support::query_rows as rows_of;
use super::*;
use crate::parser::{CteDef, Statement};

#[test]
fn nested_group_pruning_uses_nonnullable_unique_keys_and_keeps_observed_columns() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE fd_dim (id INT PRIMARY KEY, label VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE fd_nullable (id INT UNIQUE, label VARCHAR(16))")
        .unwrap();
    engine
        .execute("INSERT INTO fd_dim VALUES (1,'a'),(2,'b')")
        .unwrap();
    engine
        .execute("INSERT INTO fd_nullable VALUES (NULL,'a'),(NULL,'b'),(1,'c')")
        .unwrap();
    for (table, keys) in [("fd_dim", 1), ("fd_nullable", 2)] {
        let sql = format!(
            "WITH c AS (SELECT id,n FROM \
            (SELECT id,label,COUNT(*) n FROM {table} GROUP BY id,label) d) \
            SELECT a.id,a.n FROM c a JOIN c b ON a.id <=> b.id ORDER BY a.id,a.n"
        );
        let Statement::WithCte(mut with) = crate::parser::parse_statement(&sql).unwrap() else {
            panic!("WITH")
        };
        crate::engine::executor::cte::minimize_fd_group_by_keys(
            engine.db_for_tests(),
            "test",
            &mut with.ctes,
        );
        let crate::parser::QueryExpr::Select(outer) = &with.ctes[0].query else {
            panic!("SELECT")
        };
        let inner = outer
            .derived
            .as_ref()
            .and_then(crate::parser::SubQuery::as_select)
            .unwrap();
        assert_eq!(inner.group_by.len(), keys, "{table}");
        assert_eq!(inner.projection.len(), 2);
        let expected = with_test_env(&[("MYDB_FD_GROUPKEY_MINIMIZE", Some("off"))], || {
            rows_of(&engine, &sql)
        });
        assert_eq!(rows_of(&engine, &sql), expected);
    }
    let observed = "WITH c AS (SELECT id,label,n FROM (SELECT id,label,COUNT(*) n FROM fd_dim GROUP BY id,label) d) SELECT a.label FROM c a JOIN c b ON a.id=b.id ORDER BY a.label";
    let Statement::WithCte(mut with) = crate::parser::parse_statement(observed).unwrap() else {
        panic!("WITH")
    };
    crate::engine::executor::cte::minimize_fd_group_by_keys(
        engine.db_for_tests(),
        "test",
        &mut with.ctes,
    );
    let crate::parser::QueryExpr::Select(outer) = &with.ctes[0].query else {
        panic!("SELECT")
    };
    assert_eq!(
        outer
            .derived
            .as_ref()
            .and_then(crate::parser::SubQuery::as_select)
            .unwrap()
            .group_by
            .len(),
        2
    );
}

/// Parse a `WITH …`, run `prune_unused_cte_columns`, return the CTE defs.
fn prune_ctes(sql: &str) -> Vec<CteDef> {
    let stmt = crate::parser::parse_statement(sql).expect("parse WITH");
    let Statement::WithCte(w) = stmt else {
        panic!("expected a WITH statement");
    };
    let mut ctes = w.ctes;
    let body = *w.body;
    crate::engine::executor::cte::prune_unused_cte_columns(&mut ctes, &body);
    ctes
}

/// Projection arities of every leaf SELECT in a named CTE body, left-to-right.
fn branch_arities(ctes: &[CteDef], name: &str) -> Vec<usize> {
    fn walk<'q>(stmt: impl Into<crate::parser::QueryRef<'q>>, out: &mut Vec<usize>) {
        let stmt = stmt.into();
        match stmt {
            crate::parser::QueryRef::Select(s) => out.push(s.projection.len()),
            crate::parser::QueryRef::SetOperation(set) => {
                walk(&set.left, out);
                walk(&set.right, out);
            }
            _ => {}
        }
    }
    let cte = ctes
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
        .unwrap_or_else(|| panic!("no CTE named {name}"));
    let mut out = Vec::new();
    walk(&cte.query, &mut out);
    out
}

/// UNION ALL body, outer reads one of three columns → every branch narrows
/// to that single column.
#[test]
fn union_all_body_narrows_all_branches() {
    let ctes = prune_ctes(
        "WITH u AS (SELECT a, b, c FROM t1 UNION ALL SELECT a, b, c FROM t2) \
         SELECT a FROM u ORDER BY a",
    );
    assert_eq!(
        branch_arities(&ctes, "u"),
        vec![1, 1],
        "both UNION ALL branches must drop the two unused columns"
    );
}

/// Nested all-UNION-ALL spine narrows at every leaf.
#[test]
fn nested_union_all_narrows_every_leaf() {
    let ctes = prune_ctes(
        "WITH u AS ( \
            SELECT a, b, c FROM t1 \
            UNION ALL SELECT a, b, c FROM t2 \
            UNION ALL SELECT a, b, c FROM t3) \
         SELECT a, c FROM u",
    );
    assert_eq!(
        branch_arities(&ctes, "u"),
        vec![2, 2, 2],
        "every leaf of the UNION ALL spine keeps only {{a, c}}"
    );
}

/// UNION *without* ALL deduplicates over all columns — narrowing would change
/// which rows are distinct, so the body must stay full-width.
#[test]
fn union_distinct_body_is_not_narrowed() {
    let ctes = prune_ctes(
        "WITH u AS (SELECT a, b, c FROM t1 UNION SELECT a, b, c FROM t2) \
         SELECT a FROM u",
    );
    assert_eq!(
        branch_arities(&ctes, "u"),
        vec![3, 3],
        "UNION (DISTINCT) must not be narrowed"
    );
}

/// INTERSECT / EXCEPT likewise deduplicate — never narrowed.
#[test]
fn intersect_and_except_bodies_are_not_narrowed() {
    for op in ["INTERSECT", "EXCEPT"] {
        let ctes = prune_ctes(&format!(
            "WITH u AS (SELECT a, b, c FROM t1 {op} SELECT a, b, c FROM t2) \
             SELECT a FROM u"
        ));
        assert_eq!(
            branch_arities(&ctes, "u"),
            vec![3, 3],
            "{op} must not be narrowed"
        );
    }
}

/// A per-branch DISTINCT dedups within that branch; the leaf is not narrowable
/// so the whole set-op body stays full-width.
#[test]
fn per_branch_distinct_blocks_narrowing() {
    let ctes = prune_ctes(
        "WITH u AS (SELECT DISTINCT a, b, c FROM t1 UNION ALL SELECT a, b, c FROM t2) \
         SELECT a FROM u",
    );
    assert_eq!(
        branch_arities(&ctes, "u"),
        vec![3, 3],
        "a DISTINCT branch must block set-op narrowing"
    );
}

// --------------------------------------------------------------------------
// Execution-level differential: narrowing is a pure plan-shape change.
// --------------------------------------------------------------------------

fn setup_union_tables(engine: &StorageQueryEngine) {
    for t in ["s1", "s2", "s3"] {
        engine
            .execute(&format!(
                "CREATE TABLE {t} (a INT, b INT, c TEXT, pad TEXT)"
            ))
            .unwrap();
    }
    // Keep every overlapping key and duplicate across all three sources.
    for table in ["test.s1", "test.s2", "test.s3"] {
        insert_fixture_rows(
            engine,
            table,
            (0..60).map(|i| {
                vec![
                    Value::Int(i % 10),
                    Value::Int(i % 7),
                    Value::String(format!("c{}", i % 5)),
                    Value::String(format!("pad-long-text-{i}")),
                ]
            }),
        );
    }
}

/// Outer reads a strict subset of a UNION ALL CTE's columns: gate on/off must
/// agree row-for-row, including the duplicate-preserving UNION ALL semantics.
#[test]
fn union_all_cte_pruning_matches_unpruned() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);

    let queries = [
        // Plain subset projection.
        "WITH u AS (SELECT a, b, c FROM s1 UNION ALL SELECT a, b, c FROM s2) \
         SELECT a FROM u ORDER BY a",
        // Subset feeding a GROUP BY (duplicates must survive into the count).
        "WITH u AS (SELECT a, b, c FROM s1 UNION ALL SELECT a, b, c FROM s2) \
         SELECT a, COUNT(*) FROM u GROUP BY a ORDER BY a",
        // Two columns used, one dropped; nested spine.
        "WITH u AS ( \
            SELECT a, b, c FROM s1 \
            UNION ALL SELECT a, b, c FROM s2 \
            UNION ALL SELECT a, b, c FROM s3) \
         SELECT a, b FROM u ORDER BY a, b",
        // Outer joins the narrowed CTE to a base table on a kept column.
        "WITH u AS (SELECT a, b, c FROM s1 UNION ALL SELECT a, b, c FROM s2) \
         SELECT u.a, s3.c FROM u JOIN s3 ON u.a = s3.a ORDER BY u.a, s3.c LIMIT 50",
    ];

    for sql in queries {
        let pruned = rows_of(&engine, sql);
        let unpruned = with_test_env(&[("MYDB_CTE_PUSHDOWN", Some("off"))], || {
            rows_of(&engine, sql)
        });
        assert!(!unpruned.is_empty(), "fixture produced no rows: {sql}");
        assert_eq!(pruned, unpruned, "narrowing changed results: {sql}");
    }
}

/// UNION (DISTINCT) over a subset projection: still correct (narrowing is
/// skipped, but verify end-to-end the dedup semantics hold gate on/off).
#[test]
fn union_distinct_cte_matches_gate_off() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);

    let sql = "WITH u AS (SELECT a, b, c FROM s1 UNION SELECT a, b, c FROM s2) \
         SELECT a FROM u ORDER BY a";
    let pruned = rows_of(&engine, sql);
    let unpruned = with_test_env(&[("MYDB_CTE_PUSHDOWN", Some("off"))], || {
        rows_of(&engine, sql)
    });
    assert_eq!(pruned, unpruned, "UNION DISTINCT result must be stable");
}

/// Regression: a CTE body that is a parenthesised set operation with a trailing
/// `ORDER BY ... LIMIT n` must actually return only `n` rows. The parser used to
/// drop the trailing ORDER BY/LIMIT, so the CTE returned every row.
#[test]
fn parenthesised_setop_cte_limit_takes_effect() {
    let _guard = test_env_lock();
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    setup_union_tables(&engine);

    // Unlimited body: 60 + 60 = 120 rows (UNION ALL keeps duplicates).
    let unlimited = rows_of(
        &engine,
        "WITH u AS (SELECT a, b FROM s1 UNION ALL SELECT a, b FROM s2) \
         SELECT COUNT(*) FROM u",
    );
    assert_eq!(unlimited[0][0], Value::Int(120), "fixture sanity");

    // Same body parenthesised with a trailing ORDER BY/LIMIT 5: exactly 5 rows.
    let limited = rows_of(
        &engine,
        "WITH u AS ((SELECT a, b FROM s1 UNION ALL SELECT a, b FROM s2) ORDER BY a, b LIMIT 5) \
         SELECT COUNT(*) FROM u",
    );
    assert_eq!(
        limited[0][0],
        Value::Int(5),
        "trailing LIMIT on a parenthesised set-op CTE body must cap the rows"
    );

    // OFFSET is honoured too: LIMIT 5 OFFSET 3 over 120 ordered rows → 5 rows,
    // and they must be the deterministically ordered window (not the first 5).
    let head = rows_of(
        &engine,
        "WITH u AS ((SELECT a, b FROM s1 UNION ALL SELECT a, b FROM s2) ORDER BY a, b LIMIT 5) \
         SELECT a, b FROM u ORDER BY a, b",
    );
    let offset = rows_of(
        &engine,
        "WITH u AS ((SELECT a, b FROM s1 UNION ALL SELECT a, b FROM s2) ORDER BY a, b LIMIT 5 OFFSET 3) \
         SELECT a, b FROM u ORDER BY a, b",
    );
    assert_eq!(offset.len(), 5, "LIMIT 5 OFFSET 3 must return 5 rows");
    assert_ne!(head, offset, "OFFSET must shift the window");
}
