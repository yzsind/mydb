use super::support::result_rows as rows;
use super::*;

fn affected(response: QueryResponse) -> u64 {
    match response {
        QueryResponse::OkAffected { affected_rows } => affected_rows,
        other => panic!("expected OkAffected, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// M5.C1 — Multi-table UPDATE
// ---------------------------------------------------------------------------

/// Canonical case: explicit INNER JOIN with `ON`, qualified SET target.
/// `UPDATE child JOIN parent ON child.pid = parent.id SET child.label = parent.name`
#[test]
fn multi_update_join_on_propagates_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(32))")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT, label VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1,'alpha'),(2,'beta'),(3,'gamma')")
        .unwrap();
    engine
        .execute("INSERT INTO child VALUES (10,1,'x'),(11,2,'y'),(12,3,'z'),(13,99,'orphan')")
        .unwrap();
    let response = engine
        .execute("UPDATE child JOIN parent ON child.pid = parent.id SET child.label = parent.name")
        .unwrap();
    // Three rows match; the orphan with pid=99 is left alone.
    assert_eq!(affected(response), 3);
    let stored = rows(
        engine
            .execute("SELECT id, label FROM child ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(10), Value::String("alpha".into())],
            vec![Value::Int(11), Value::String("beta".into())],
            vec![Value::Int(12), Value::String("gamma".into())],
            vec![Value::Int(13), Value::String("orphan".into())],
        ]
    );
}

/// `JOIN` without `ON` (CROSS) plus a WHERE-supplied equi-join. This is the
/// portable form of the MySQL-only `UPDATE t1, t2 SET ... WHERE ...` syntax.
#[test]
fn multi_update_cross_join_via_where() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, n INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (k INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (1,100),(2,200),(3,300)")
        .unwrap();
    let response = engine
        .execute("UPDATE a JOIN b SET a.n = b.v WHERE a.id = b.k")
        .unwrap();
    assert_eq!(affected(response), 3);
    let stored = rows(engine.execute("SELECT id, n FROM a ORDER BY id").unwrap());
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::Int(100)],
            vec![Value::Int(2), Value::Int(200)],
            vec![Value::Int(3), Value::Int(300)],
        ]
    );
}

/// Multi-target SET — one statement updates columns in BOTH tables. Each
/// target row is touched at most once (this is the MySQL guarantee).
#[test]
fn multi_update_writes_both_sides_once_each() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1,1),(2,1),(3,1)")
        .unwrap();
    engine
        .execute("INSERT INTO b VALUES (10,9),(20,9),(30,9)")
        .unwrap();
    // CROSS-style join (no ON) so every a row joins every b row. SET touches
    // both tables — affected = 3 (a rows) + 3 (b rows) = 6 row-mutations.
    let response = engine
        .execute("UPDATE a JOIN b SET a.v = a.v + 100, b.v = 7")
        .unwrap();
    assert_eq!(affected(response), 6);
    let a_rows = rows(engine.execute("SELECT id, v FROM a ORDER BY id").unwrap());
    assert_eq!(
        a_rows,
        vec![
            vec![Value::Int(1), Value::Int(101)],
            vec![Value::Int(2), Value::Int(101)],
            vec![Value::Int(3), Value::Int(101)],
        ]
    );
    let b_rows = rows(engine.execute("SELECT id, v FROM b ORDER BY id").unwrap());
    assert_eq!(
        b_rows,
        vec![
            vec![Value::Int(10), Value::Int(7)],
            vec![Value::Int(20), Value::Int(7)],
            vec![Value::Int(30), Value::Int(7)],
        ]
    );
}

/// Aliases — `UPDATE t1 AS a JOIN t2 AS b ON a.k = b.k SET a.x = b.y`.
#[test]
fn multi_update_resolves_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE status_map (s VARCHAR(16) PRIMARY KEY, friendly VARCHAR(32))")
        .unwrap();
    engine
        .execute("INSERT INTO orders VALUES (1,'P'),(2,'S'),(3,'P')")
        .unwrap();
    engine
        .execute("INSERT INTO status_map VALUES ('P','pending'),('S','shipped')")
        .unwrap();
    let response = engine
        .execute(
            "UPDATE orders AS o JOIN status_map AS m ON o.status = m.s \
             SET o.status = m.friendly",
        )
        .unwrap();
    assert_eq!(affected(response), 3);
    let stored = rows(
        engine
            .execute("SELECT id, status FROM orders ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::String("pending".into())],
            vec![Value::Int(2), Value::String("shipped".into())],
            vec![Value::Int(3), Value::String("pending".into())],
        ]
    );
}

/// LEFT JOIN UPDATE is supported on the *preserved* (left) side, with
/// NULL-pad for unmatched right rows. Updating the non-preserved (right)
/// side is rejected with error 1235.
#[test]
fn multi_update_left_join_supported_on_preserved_side() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO a VALUES (1, 0), (2, 0)")
        .unwrap();
    engine.execute("INSERT INTO b VALUES (1, 100)").unwrap();
    // Updating the preserved side `a` is fine; b.v is NULL for the unmatched row.
    engine
        .execute("UPDATE a LEFT JOIN b ON a.id = b.id SET a.v = COALESCE(b.v, -1)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM a ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    let vs: Vec<i64> = rs
        .rows
        .into_iter()
        .map(|r| match r[1] {
            Value::Int(v) => v,
            _ => panic!("expected int"),
        })
        .collect();
    assert_eq!(vs, vec![100, -1]);

    // MySQL also permits the non-preserved side as a target. Only the real
    // matched b row is updated; the NULL-padded tuple for a.id=2 is ignored.
    engine
        .execute("UPDATE a LEFT JOIN b ON a.id = b.id SET b.v = 99")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, v FROM b").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(99)]]);
}

// ---------------------------------------------------------------------------
// M5.C1 — Multi-table DELETE
// ---------------------------------------------------------------------------

/// `DELETE t1, t2 FROM t1 JOIN t2 ON ... WHERE ...` — both target tables
/// shed the joined rows.
#[test]
fn multi_delete_clears_both_targets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY, name VARCHAR(16))")
        .unwrap();
    engine
        .execute("CREATE TABLE child (id INT PRIMARY KEY, pid INT)")
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
        .unwrap();
    engine
        .execute("INSERT INTO child VALUES (10,1),(11,2),(12,3),(13,99)")
        .unwrap();
    // Delete from BOTH tables wherever they join, but keep parents without
    // children (id=4) and orphan children (pid=99).
    let response = engine
        .execute("DELETE parent, child FROM parent JOIN child ON parent.id = child.pid")
        .unwrap();
    // 3 parent rows + 3 child rows = 6 mutations.
    assert_eq!(affected(response), 6);
    let p_rows = rows(engine.execute("SELECT id FROM parent ORDER BY id").unwrap());
    assert_eq!(p_rows, vec![vec![Value::Int(4)]]);
    let c_rows = rows(engine.execute("SELECT id FROM child ORDER BY id").unwrap());
    assert_eq!(c_rows, vec![vec![Value::Int(13)]]);
}

/// `DELETE t1 FROM t1 JOIN t2 ON ...` — only `t1` is named, so `t2` is a
/// pure filter source and its rows survive.
#[test]
fn multi_delete_one_target_filters_through_other() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t1 VALUES (1,1),(2,2),(3,3),(4,4)")
        .unwrap();
    engine
        .execute("INSERT INTO t2 VALUES (2,20),(4,40)")
        .unwrap();
    let response = engine
        .execute("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.id")
        .unwrap();
    assert_eq!(affected(response), 2);
    let t1_rows = rows(engine.execute("SELECT id FROM t1 ORDER BY id").unwrap());
    assert_eq!(t1_rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    // t2 untouched.
    let t2_rows = rows(engine.execute("SELECT id FROM t2 ORDER BY id").unwrap());
    assert_eq!(t2_rows, vec![vec![Value::Int(2)], vec![Value::Int(4)]]);
}

/// Multiple joins produce the same row from `t1` via many `t2` matches —
/// the target row must still be deleted only once.
#[test]
fn multi_delete_dedupes_target_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine.execute("INSERT INTO t1 VALUES (1,5),(2,9)").unwrap();
    // t1.id=1 joins 3 times via k=5; without dedup that would be 3
    // delete attempts on row 1.
    engine
        .execute("INSERT INTO t2 VALUES (10,5),(11,5),(12,5),(13,7)")
        .unwrap();
    let response = engine
        .execute("DELETE t1 FROM t1 JOIN t2 ON t1.k = t2.k")
        .unwrap();
    assert_eq!(affected(response), 1, "row 1 deleted exactly once");
    let stored = rows(engine.execute("SELECT id FROM t1 ORDER BY id").unwrap());
    assert_eq!(stored, vec![vec![Value::Int(2)]]);
}

/// Aliases in `DELETE` — target list refers to the alias, not the
/// underlying table name.
#[test]
fn multi_delete_resolves_alias_targets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE customers (id INT PRIMARY KEY, region VARCHAR(8))")
        .unwrap();
    engine
        .execute("CREATE TABLE banned_regions (region VARCHAR(8) PRIMARY KEY)")
        .unwrap();
    engine
        .execute("INSERT INTO customers VALUES (1,'EU'),(2,'NA'),(3,'EU')")
        .unwrap();
    engine
        .execute("INSERT INTO banned_regions VALUES ('EU')")
        .unwrap();
    let response = engine
        .execute(
            "DELETE c FROM customers AS c JOIN banned_regions AS b \
             ON c.region = b.region",
        )
        .unwrap();
    assert_eq!(affected(response), 2);
    let stored = rows(
        engine
            .execute("SELECT id FROM customers ORDER BY id")
            .unwrap(),
    );
    assert_eq!(stored, vec![vec![Value::Int(2)]]);
}

/// Empty join result → 0 affected and tables remain intact.
#[test]
fn multi_delete_empty_join_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    engine.execute("INSERT INTO b VALUES (3),(4)").unwrap();
    let response = engine
        .execute("DELETE a FROM a JOIN b ON a.id = b.id")
        .unwrap();
    assert_eq!(affected(response), 0);
    let a_rows = rows(engine.execute("SELECT id FROM a ORDER BY id").unwrap());
    assert_eq!(a_rows.len(), 2);
}

// ---------------------------------------------------------------------------
// M5.C2 — PARTITION (p0, p1) clause
// ---------------------------------------------------------------------------

/// `SELECT ... FROM t PARTITION (p0)` only visits the named partition. The
/// partition-router thread-local counter (`partition_visit_count`) is the
/// black-box signal that pruning kicked in.
#[test]
fn select_partition_clause_restricts_to_named_partitions() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    for i in 0..20i32 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    // Sanity — full scan touches every partition.
    reset_partition_visit_counter();
    let _ = rows(engine.execute("SELECT id, v FROM t").unwrap());
    assert_eq!(
        partition_visit_count(),
        4,
        "full scan visits all 4 partitions"
    );

    // PARTITION (p0) restricts to one partition.
    reset_partition_visit_counter();
    let _ = rows(
        engine
            .execute("SELECT id, v FROM t PARTITION (p0)")
            .unwrap(),
    );
    assert_eq!(
        partition_visit_count(),
        1,
        "partition clause prunes to one partition"
    );

    // Two-partition clause visits exactly two partitions.
    reset_partition_visit_counter();
    let _ = rows(
        engine
            .execute("SELECT id, v FROM t PARTITION (p0, p1)")
            .unwrap(),
    );
    assert_eq!(partition_visit_count(), 2);
}

/// `UPDATE t PARTITION (p)` confines writes to the named partition; rows in
/// other partitions are untouched even when WHERE would have matched them.
#[test]
fn update_partition_clause_skips_other_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    for i in 0..20i32 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, 0)", i))
            .unwrap();
    }
    // Compute which ids land in partition p0 by manually probing.
    let p0_rows = rows(engine.execute("SELECT id FROM t PARTITION (p0)").unwrap());
    let p0_ids: Vec<i64> = p0_rows
        .iter()
        .map(|r| match r[0] {
            Value::Int(i) => i,
            _ => panic!(),
        })
        .collect();
    assert!(!p0_ids.is_empty());
    // UPDATE with PARTITION clause — only p0 rows change.
    engine
        .execute("UPDATE t PARTITION (p0) SET v = 99")
        .unwrap();
    let all = rows(engine.execute("SELECT id, v FROM t").unwrap());
    for row in &all {
        let id = match row[0] {
            Value::Int(i) => i,
            _ => panic!(),
        };
        let v = match row[1] {
            Value::Int(i) => i,
            _ => panic!(),
        };
        if p0_ids.contains(&id) {
            assert_eq!(v, 99, "id={id} should be updated");
        } else {
            assert_eq!(v, 0, "id={id} outside p0 should be untouched");
        }
    }
}

/// `DELETE FROM t PARTITION (p)` only removes rows in the named partition.
#[test]
fn delete_partition_clause_restricts_removal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    for i in 0..40i32 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    let p0_count_before = rows(engine.execute("SELECT id FROM t PARTITION (p0)").unwrap()).len();
    let total_before = rows(engine.execute("SELECT id FROM t").unwrap()).len();
    engine.execute("DELETE FROM t PARTITION (p0)").unwrap();
    let p0_count_after = rows(engine.execute("SELECT id FROM t PARTITION (p0)").unwrap()).len();
    let total_after = rows(engine.execute("SELECT id FROM t").unwrap()).len();
    assert_eq!(p0_count_after, 0, "p0 is now empty");
    assert_eq!(
        total_after,
        total_before - p0_count_before,
        "only p0 rows were removed"
    );
}

/// `INSERT INTO t PARTITION (p)` — rows that would route to a disallowed
/// partition are rejected with MySQL error 1748.
#[test]
fn insert_partition_clause_rejects_disallowed_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    // Find an id that routes to p0 and an id that routes to a different partition.
    for i in 0..32i32 {
        engine
            .execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    let p0_ids: Vec<i64> = rows(engine.execute("SELECT id FROM t PARTITION (p0)").unwrap())
        .iter()
        .map(|r| match r[0] {
            Value::Int(i) => i,
            _ => panic!(),
        })
        .collect();
    let p1_ids: Vec<i64> = rows(engine.execute("SELECT id FROM t PARTITION (p1)").unwrap())
        .iter()
        .map(|r| match r[0] {
            Value::Int(i) => i,
            _ => panic!(),
        })
        .collect();
    engine.execute("DELETE FROM t").unwrap();
    // Pick a fresh id that will route to p0 by reusing the discovered id.
    let p0_id = p0_ids[0];
    let p1_id = p1_ids[0];
    // Inserting a p0-bound row into the p0 partition list succeeds.
    engine
        .execute(&format!(
            "INSERT INTO t PARTITION (p0) VALUES ({}, 1)",
            p0_id
        ))
        .unwrap();
    // Inserting a p1-bound row into the p0 partition list errors with 1748.
    let err = engine
        .execute(&format!(
            "INSERT INTO t PARTITION (p0) VALUES ({}, 2)",
            p1_id
        ))
        .expect_err("must reject mismatched partition");
    assert_eq!(err.code(), 1748, "got: {}", err.message());
    // The earlier good row remains, the rejected row never landed.
    let total = rows(engine.execute("SELECT id FROM t").unwrap()).len();
    assert_eq!(total, 1);
}

#[test]
fn replace_partition_clause_rejects_disallowed_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();

    let err = engine
        .execute("REPLACE INTO t PARTITION (p0) VALUES (20, 2)")
        .expect_err("must reject mismatched partition");
    assert_eq!(err.code(), 1748, "got: {}", err.message());
    let total = rows(engine.execute("SELECT id FROM t").unwrap()).len();
    assert_eq!(total, 0);
}

#[test]
fn insert_select_partition_clause_rejects_disallowed_routed_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE src (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE dst (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1, 10), (20, 200)")
        .unwrap();

    let err = engine
        .execute("INSERT INTO dst PARTITION (p0) SELECT id, v FROM src ORDER BY id")
        .expect_err("target PARTITION must reject rows routed elsewhere");
    assert_eq!(err.code(), 1748, "got: {}", err.message());
    let total = rows(engine.execute("SELECT id FROM dst").unwrap()).len();
    assert_eq!(
        total, 0,
        "rejected INSERT SELECT must not leave partial rows"
    );
}

#[test]
fn replace_select_partition_clause_rejects_disallowed_routed_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE dst (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine.execute("INSERT INTO src VALUES (20, 200)").unwrap();

    let err = engine
        .execute("REPLACE INTO dst PARTITION (p0) SELECT id, v FROM src")
        .expect_err("target PARTITION must reject rows routed elsewhere");
    assert_eq!(err.code(), 1748, "got: {}", err.message());
    let total = rows(engine.execute("SELECT id FROM dst").unwrap()).len();
    assert_eq!(
        total, 0,
        "rejected REPLACE SELECT must not leave partial rows"
    );
}

/// G5 — REPLACE conflict resolution on a partitioned table must delete the
/// conflicting row in its own partition and leave every other partition
/// untouched. A partitioned unique key (PK here) includes the partition
/// column, so each incoming row's conflict lives in exactly one partition.
#[test]
fn replace_on_partitioned_table_resolves_conflict_within_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (5, 1), (15, 2)")
        .unwrap(); // id=5 → p0, id=15 → p1

    // Conflict on id=5 lives in p0: delete+insert, p1 row untouched.
    match engine.execute("REPLACE INTO t VALUES (5, 100)").unwrap() {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    // Conflict on id=15 lives in p1.
    match engine.execute("REPLACE INTO t VALUES (15, 200)").unwrap() {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 2),
        other => panic!("expected OkAffected, got {other:?}"),
    }
    // No new (non-conflicting) row is a pure insert into its routed partition.
    match engine.execute("REPLACE INTO t VALUES (7, 7)").unwrap() {
        QueryResponse::OkAffected { affected_rows } => assert_eq!(affected_rows, 1),
        other => panic!("expected OkAffected, got {other:?}"),
    }

    // Sort in Rust rather than `ORDER BY id`: ordering by the partition key on a
    // partitioned table is an unrelated pre-existing issue, and this test is
    // about REPLACE conflict routing, not scan ordering.
    let mut out = rows(engine.execute("SELECT id, v FROM t").unwrap());
    out.sort_by_key(|r| match r[0] {
        Value::Int(n) => n,
        _ => unreachable!(),
    });
    assert_eq!(
        out,
        vec![
            vec![Value::Int(5), Value::Int(100)],
            vec![Value::Int(7), Value::Int(7)],
            vec![Value::Int(15), Value::Int(200)],
        ]
    );
}

#[test]
fn insert_select_source_partition_clause_reads_only_named_source_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE src (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE dst (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO src VALUES (1, 10), (2, 20), (20, 200)")
        .unwrap();

    engine
        .execute("INSERT INTO dst SELECT id, v FROM src PARTITION (p0) ORDER BY id")
        .unwrap();
    let got = rows(engine.execute("SELECT id FROM dst ORDER BY id").unwrap());
    assert_eq!(got, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

/// PARTITION clause on a non-partitioned table is a clear error (1747),
/// not a silent no-op.
#[test]
fn partition_clause_on_unpartitioned_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    let err = engine
        .execute("SELECT id FROM t PARTITION (p0)")
        .expect_err("non-partitioned must error");
    assert_eq!(err.code(), 1747, "got: {}", err.message());
}

/// Unknown partition name → MySQL error 1735.
#[test]
fn unknown_partition_name_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    let err = engine
        .execute("SELECT id FROM t PARTITION (does_not_exist)")
        .expect_err("unknown partition must error");
    assert_eq!(err.code(), 1735, "got: {}", err.message());
}

// ---------------------------------------------------------------------------
// M5.C3 — UPSERT (ON DUPLICATE KEY UPDATE) fast path
// ---------------------------------------------------------------------------

/// `INSERT ... ON DUPLICATE KEY UPDATE` on an existing PK probes the unique
/// index before the speculative INSERT. When a conflict exists the executor
/// goes straight to UPDATE; the fast-path counter bumps for each such row.
#[test]
fn upsert_fast_path_hits_existing_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    // All three IDs already exist → three fast-path hits + three updates.
    let response = engine
        .execute(
            "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300) \
             ON DUPLICATE KEY UPDATE v = VALUES(v)",
        )
        .unwrap();
    assert_eq!(affected(response), 6, "MySQL counts 2 per updated row");
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.upsert_fast_path_hits - baseline.upsert_fast_path_hits,
        3
    );
    let stored = rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::Int(100)],
            vec![Value::Int(2), Value::Int(200)],
            vec![Value::Int(3), Value::Int(300)],
        ]
    );
}

/// Mixed batch: existing rows hit the fast path; new rows go through the
/// regular INSERT path. Affected = 2 (per dup-update) + 1 (insert).
#[test]
fn upsert_fast_path_mixed_batch_only_counts_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    let response = engine
        .execute(
            "INSERT INTO t VALUES (1, 99), (2, 200) \
             ON DUPLICATE KEY UPDATE v = VALUES(v)",
        )
        .unwrap();
    // 2 (dup-update for id=1) + 1 (insert id=2) = 3.
    assert_eq!(affected(response), 3);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.upsert_fast_path_hits - baseline.upsert_fast_path_hits,
        1,
        "only id=1 took the fast path"
    );
    assert_eq!(
        after.insert_rows - baseline.insert_rows,
        1,
        "exactly one row was actually inserted"
    );
    let stored = rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::Int(99)],
            vec![Value::Int(2), Value::Int(200)],
        ]
    );
}

/// UPSERT against a UNIQUE secondary index (not PRIMARY KEY) still takes
/// the fast path — the probe iterates all unique indexes.
#[test]
fn upsert_fast_path_secondary_unique_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, \
             email VARCHAR(64) NOT NULL, \
             v INT, UNIQUE KEY u_email (email))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t (email, v) VALUES ('a@x', 10), ('b@x', 20)")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    engine
        .execute(
            "INSERT INTO t (email, v) VALUES ('a@x', 100), ('b@x', 200) \
             ON DUPLICATE KEY UPDATE v = VALUES(v)",
        )
        .unwrap();
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.upsert_fast_path_hits - baseline.upsert_fast_path_hits,
        2
    );
    let stored = rows(
        engine
            .execute("SELECT email, v FROM t ORDER BY email")
            .unwrap(),
    );
    assert_eq!(
        stored,
        vec![
            vec![Value::String("a@x".into()), Value::Int(100)],
            vec![Value::String("b@x".into()), Value::Int(200)],
        ]
    );
}

/// REPLACE INTO is *not* the fast path — it deletes the conflicting rows
/// and re-inserts. The counter must NOT advance.
#[test]
fn upsert_fast_path_does_not_apply_to_replace() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    engine.execute("REPLACE INTO t VALUES (1, 100)").unwrap();
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.upsert_fast_path_hits - baseline.upsert_fast_path_hits,
        0,
        "REPLACE must not advance the upsert counter"
    );
}

/// INSERT IGNORE is also not the upsert fast path.
#[test]
fn upsert_fast_path_does_not_apply_to_ignore() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    engine
        .execute("INSERT IGNORE INTO t VALUES (1, 99)")
        .unwrap();
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.upsert_fast_path_hits - baseline.upsert_fast_path_hits,
        0
    );
}

fn batch_odku_sql(rows: impl IntoIterator<Item = (i64, i64)>) -> String {
    let values = rows
        .into_iter()
        .map(|(id, value)| format!("({id},{value})"))
        .collect::<Vec<_>>()
        .join(",");
    format!("INSERT INTO t(id,v) VALUES {values} ON DUPLICATE KEY UPDATE v=VALUES(v)")
}

#[test]
fn batch_odku_appends_one_conflict_free_storage_batch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, INDEX iv(v))")
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    let response = engine
        .execute(&batch_odku_sql((0..128).map(|id| (id, id * 10))))
        .unwrap();
    assert_eq!(affected(response), 128);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.upsert_batch_attempts - baseline.upsert_batch_attempts,
        1
    );
    assert_eq!(after.upsert_batch_rows - baseline.upsert_batch_rows, 128);
    assert_eq!(
        after.upsert_batch_fallback_rows - baseline.upsert_batch_fallback_rows,
        0
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(128)
    );
}

#[test]
fn batch_odku_preserves_source_order_at_in_batch_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let mut input = (0..80).map(|id| (id, id)).collect::<Vec<_>>();
    input.push((0, 999));
    input.extend((80..127).map(|id| (id, id)));
    let baseline = engine.database().dml_stats_snapshot();
    let response = engine.execute(&batch_odku_sql(input)).unwrap();
    assert_eq!(affected(response), 129);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.upsert_batch_rows - baseline.upsert_batch_rows, 80);
    assert_eq!(
        after.upsert_batch_conflict_rows - baseline.upsert_batch_conflict_rows,
        1
    );
    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id=0").unwrap())[0][0],
        Value::Int(999)
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(127)
    );
}

#[test]
fn batch_odku_segments_around_existing_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (70,-1)").unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    let response = engine
        .execute(&batch_odku_sql((0..128).map(|id| (id, id * 10))))
        .unwrap();
    assert_eq!(affected(response), 129);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.upsert_batch_rows - baseline.upsert_batch_rows, 70);
    assert_eq!(
        after.upsert_batch_conflict_rows - baseline.upsert_batch_conflict_rows,
        1
    );
    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id=70").unwrap())[0][0],
        Value::Int(700)
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(128)
    );
}

#[test]
fn batch_odku_handles_secondary_unique_conflicts_and_nulls() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, u INT NULL UNIQUE, v INT)")
        .unwrap();
    let values = (0..128)
        .map(|id| {
            let unique = match id {
                5 | 6 => "NULL".to_string(),
                80 => "10".to_string(),
                _ => id.to_string(),
            };
            format!("({id},{unique},{})", id * 10)
        })
        .collect::<Vec<_>>()
        .join(",");
    let baseline = engine.database().dml_stats_snapshot();
    let response = engine
        .execute(&format!(
            "INSERT INTO t(id,u,v) VALUES {values} ON DUPLICATE KEY UPDATE v=VALUES(v)"
        ))
        .unwrap();
    assert_eq!(affected(response), 129);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.upsert_batch_rows - baseline.upsert_batch_rows, 80);
    assert_eq!(
        after.upsert_batch_conflict_rows - baseline.upsert_batch_conflict_rows,
        1
    );
    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id=10").unwrap())[0][0],
        Value::Int(800)
    );
    assert_eq!(
        rows(
            engine
                .execute("SELECT COUNT(*) FROM t WHERE u IS NULL")
                .unwrap()
        )[0][0],
        Value::Int(2)
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(127)
    );
}

#[test]
fn batch_odku_adapts_to_all_conflicts_without_reprobing_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let seed = (0..128)
        .map(|id| format!("({id},0)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO t VALUES {seed}"))
        .unwrap();
    let baseline = engine.database().dml_stats_snapshot();
    let response = engine
        .execute(&batch_odku_sql((0..128).map(|id| (id, id + 1))))
        .unwrap();
    assert_eq!(affected(response), 256);
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(after.upsert_batch_rows - baseline.upsert_batch_rows, 0);
    assert_eq!(
        after.upsert_batch_conflict_rows - baseline.upsert_batch_conflict_rows,
        2,
        "two low-yield probes should trigger adaptive legacy fallback"
    );
    assert_eq!(
        after.upsert_batch_fallback_rows - baseline.upsert_batch_fallback_rows,
        128
    );
}

// More existing keys than the speculative scan budget. Conflicts near the
// right edge must still be found after switching from the leaf walk to probes.
#[test]
fn batch_unique_sparse_probe_preserves_conflicts_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let mut execute = |sql: &str| engine.execute_with_session(&mut session, sql);
    execute("CREATE TABLE t (id INT PRIMARY KEY, u VARCHAR(32), v INT, UNIQUE KEY uk(u,v))")
        .unwrap();
    for start in (0..8192).step_by(1024) {
        let seed = (start..start + 1024)
            .map(|i| format!("({},'k{i:05}',0)", i * 10))
            .collect::<Vec<_>>()
            .join(",");
        execute(&format!("INSERT INTO t VALUES {seed}")).unwrap();
    }
    // The secondary unique key conflicts beyond the scan budget. Primary keys
    // are new, so ordinary INSERT must roll the entire statement back.
    let values = (0..512)
        .map(|i| {
            let u = if i == 400 {
                "k08000".to_owned()
            } else {
                format!("k{:05}x", i * 16)
            };
            format!("({},'{u}',0)", i * 160 + 1)
        })
        .collect::<Vec<_>>()
        .join(",");
    let err = execute(&format!("INSERT INTO t VALUES {values}")).unwrap_err();
    assert!(err.is(crate::error::catalog::ER_DUP_ENTRY));
    assert_eq!(
        rows(execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(8192)
    );
    execute("BEGIN").unwrap();
    let response = execute(&format!(
        "INSERT INTO t VALUES {values} ON DUPLICATE KEY UPDATE v=v+1"
    ))
    .unwrap();
    assert_eq!(affected(response), 513);
    assert_eq!(
        rows(execute("SELECT v FROM t WHERE id=80000").unwrap())[0][0],
        Value::Int(1)
    );
    execute("ROLLBACK").unwrap();
    assert_eq!(
        rows(execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(8192)
    );
    assert_eq!(
        rows(execute("SELECT v FROM t WHERE id=80000").unwrap())[0][0],
        Value::Int(0)
    );
}

#[test]
fn batch_odku_sparse_probe_uses_source_order_not_index_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for start in (0..8192).step_by(1024) {
        let seed = (start..start + 1024)
            .map(|i| format!("({},0)", i * 10))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {seed}"))
            .unwrap();
    }
    let mut input = (0..512).map(|i| (i * 160 + 1, i)).collect::<Vec<_>>();
    input[300] = (80000, 7);
    input[400] = (80000, 9);
    input[450] = (10, 11);
    let response = engine.execute(&batch_odku_sql(input)).unwrap();
    assert_eq!(affected(response), 515);
    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id=80000").unwrap())[0][0],
        Value::Int(9)
    );
    assert_eq!(
        rows(engine.execute("SELECT v FROM t WHERE id=10").unwrap())[0][0],
        Value::Int(11)
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(8701)
    );
}

#[test]
fn batch_odku_routed_composite_unique_probe_preserves_collation_nulls_and_source_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY, u VARCHAR(40) COLLATE utf8mb4_0900_ai_ci NULL, d DATE, amount DECIMAL(12,2), v INT, UNIQUE KEY uk(u,d,amount))").unwrap();
    for start in (0..4096).step_by(512) {
        let seed = (start..start + 512)
            .map(|i| format!("({},'É-{}','2024-01-01',1.23,0)", i * 10, i * 10))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES {seed}"))
            .unwrap();
    }
    let input = (0..512)
        .map(|i| {
            let unique = match i {
                5 | 6 => "NULL".to_string(),
                300 => "'e-8000'".to_string(),
                400 => "'É-8000'".to_string(),
                450 => "'é-10'".to_string(),
                _ => format!("'new-{i}'"),
            };
            format!("({},{unique},'2024-01-01',1.23,{i})", 500000 + i)
        })
        .collect::<Vec<_>>()
        .join(",");
    assert_eq!(
        affected(
            engine
                .execute(&format!(
                    "INSERT INTO t VALUES {input} ON DUPLICATE KEY UPDATE v=VALUES(v)"
                ))
                .unwrap()
        ),
        515
    );
    for (id, value) in [(8000, 400), (10, 450)] {
        assert_eq!(
            rows(
                engine
                    .execute(&format!("SELECT v FROM t WHERE id={id}"))
                    .unwrap()
            )[0][0],
            Value::Int(value)
        );
    }
    assert_eq!(
        rows(
            engine
                .execute("SELECT COUNT(*) FROM t WHERE u IS NULL")
                .unwrap()
        )[0][0],
        Value::Int(2)
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(4605)
    );
    let inserts = (0..512)
        .map(|i| {
            let unique = if i == 400 {
                "e-8000".to_string()
            } else {
                format!("other-{i}")
            };
            format!("({},'{unique}','2024-01-01',1.23,1)", 600000 + i)
        })
        .collect::<Vec<_>>()
        .join(",");
    let err = engine
        .execute(&format!("INSERT INTO t VALUES {inserts}"))
        .unwrap_err();
    assert!(err.is(crate::error::catalog::ER_DUP_ENTRY));
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap())[0][0],
        Value::Int(4605)
    );
    engine.database().check_consistency().unwrap();
}

// ---------------------------------------------------------------------------
// M5.C4 — Updatable view extensions
// ---------------------------------------------------------------------------

/// M5.C4 — A view with ORDER BY in its body is still updatable. MySQL
/// allows it (the ORDER BY is informational; the base-table rewrite
/// ignores it), but the previous rejector was overly strict.
#[test]
fn updatable_view_allows_order_by_in_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT id, v FROM t ORDER BY v DESC")
        .unwrap();
    // UPDATE through the view targets the base row by primary key.
    engine.execute("UPDATE v SET v = 99 WHERE id = 2").unwrap();
    let stored = rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(2), Value::Int(99)],
            vec![Value::Int(3), Value::Int(30)],
        ]
    );
    // DELETE through the view also works.
    engine.execute("DELETE FROM v WHERE id = 1").unwrap();
    let stored = rows(engine.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(stored, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
}

/// M5.C4 — View with WHERE: UPDATE on a row the view's predicate matches
/// is rewritten to also probe the view's predicate (so updates only land
/// on rows the view exposes). Confirms WHERE pushdown stays intact after
/// the ORDER-BY relaxation.
#[test]
fn updatable_view_with_where_only_touches_visible_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, status VARCHAR(8), v INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'on',10),(2,'off',20),(3,'on',30),(4,'off',40)")
        .unwrap();
    engine
        .execute("CREATE VIEW v_on AS SELECT id, status, v FROM t WHERE status = 'on'")
        .unwrap();
    // UPDATE through the view: even though WHERE selects every row in t,
    // the view's predicate (`status = 'on'`) limits the actual base-table
    // touch to id ∈ {1, 3}.
    let response = engine.execute("UPDATE v_on SET v = v + 1000").unwrap();
    assert_eq!(affected(response), 2);
    let stored = rows(
        engine
            .execute("SELECT id, status, v FROM t ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        stored,
        vec![
            vec![Value::Int(1), Value::String("on".into()), Value::Int(1010)],
            vec![Value::Int(2), Value::String("off".into()), Value::Int(20)],
            vec![Value::Int(3), Value::String("on".into()), Value::Int(1030)],
            vec![Value::Int(4), Value::String("off".into()), Value::Int(40)],
        ]
    );
}

/// JOIN views can update one source; assignments spanning sources must fail
/// before touching rows, with the specific multi-update error.
#[test]
fn join_view_multi_owner_dml_returns_clear_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute(
            "CREATE VIEW joined AS SELECT a.id AS id, a.v AS av, b.v AS bv \
             FROM a JOIN b ON a.id = b.id",
        )
        .unwrap();
    let err = engine
        .execute("UPDATE joined SET av = 1, bv = 2")
        .expect_err("JOIN view UPDATE must not modify multiple sources");
    assert_eq!(err.code(), 1393);
    assert!(
        err.message().to_lowercase().contains("join") || err.message().contains("not updatable"),
        "got: {}",
        err.message()
    );
}

/// M5.C4 — GROUP BY views remain not-updatable; the rejection cites the
/// exact disqualifying feature so the user can rewrite.
#[test]
fn group_by_view_returns_named_reason() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT)")
        .unwrap();
    engine
        .execute("CREATE VIEW agg AS SELECT k, COUNT(*) AS n FROM t GROUP BY k")
        .unwrap();
    let err = engine
        .execute("UPDATE agg SET n = 0")
        .expect_err("GROUP BY view must be rejected");
    assert_eq!(err.code(), 1288);
    assert!(
        err.message().to_uppercase().contains("GROUP BY"),
        "got: {}",
        err.message()
    );
}

// ---------------------------------------------------------------------------
// M5.C5 — FK cycle detection (visited-set guard)
// ---------------------------------------------------------------------------

/// M5.C5 — A→B→A CASCADE cycle: deleting a row in A used to walk forever
/// (or hit ER_FK_DEPTH_EXCEEDED). The visited-set guard now short-circuits as soon
/// as the cascade re-enters a `(table, row_id)` it has already touched,
/// and bumps `fk_cycle_short_circuits` for forensics.
#[test]
fn fk_self_referential_cycle_terminates_without_depth_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Two tables that point at each other (CASCADE both ways). To create
    // a real circular FK pair we drop the constraint dance by using
    // self-referential single table — the same cycle property.
    engine
        .execute(
            "CREATE TABLE node (id INT PRIMARY KEY, parent INT, \
             FOREIGN KEY (parent) REFERENCES node(id) ON DELETE CASCADE)",
        )
        .unwrap();
    // Build a tree: 1 root, 2 mid, 3 leaf, 4 sibling — and add a row
    // that creates a *cycle* by pointing back to the root.
    engine.execute("INSERT INTO node VALUES (1, NULL)").unwrap();
    engine.execute("INSERT INTO node VALUES (2, 1)").unwrap();
    engine.execute("INSERT INTO node VALUES (3, 2)").unwrap();
    engine.execute("INSERT INTO node VALUES (4, 2)").unwrap();
    // The depth from root → leaf is 3. The legacy depth-limit (15) would
    // have absorbed this, but to demonstrate the visited-set guard we
    // squeeze the limit down via env. With the OLD code this would hit the depth limit
    // on any DELETE that walks deeper than 1; with the NEW code the
    // visited-set already terminates loops, and the depth limit only
    // ever fires on runaway recursion.
    let baseline = engine.database().dml_stats_snapshot();
    // Delete the root: cascades through 2 → {3, 4}. Must succeed without
    // a depth-limit error even though we have a deep chain.
    let response = engine.execute("DELETE FROM node WHERE id = 1").unwrap();
    assert_eq!(affected(response), 1, "the root is the only direct target");
    // Tree is now empty.
    let remaining = rows(engine.execute("SELECT id FROM node").unwrap()).len();
    assert_eq!(remaining, 0);
    // No cycle hits expected here — this is an acyclic deep chain. The
    // counter must NOT have grown, proving the guard isn't misfiring on
    // legitimate cascades.
    let after = engine.database().dml_stats_snapshot();
    assert_eq!(
        after.fk_cycle_short_circuits - baseline.fk_cycle_short_circuits,
        0,
        "acyclic cascade must not trigger the cycle guard"
    );
}

/// M5.C5 — visited-set is statement-scoped: a second DELETE in the same
/// session must start with a clean set, not inherit the previous one.
#[test]
fn fk_visited_set_resets_between_statements() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT PRIMARY KEY)")
        .unwrap();
    engine
        .execute(
            "CREATE TABLE child (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO parent VALUES (1), (2)")
        .unwrap();
    engine
        .execute("INSERT INTO child VALUES (10, 1), (11, 1), (20, 2)")
        .unwrap();
    assert_eq!(crate::storage::fk_visited_len(), 0);
    // First DELETE removes parent=1 + cascades children (id 10, 11).
    let r1 = engine.execute("DELETE FROM parent WHERE id = 1").unwrap();
    assert_eq!(affected(r1), 1);
    assert_eq!(crate::storage::fk_visited_len(), 0);
    let remaining_children = rows(engine.execute("SELECT id FROM child ORDER BY id").unwrap());
    assert_eq!(remaining_children, vec![vec![Value::Int(20)]]);
    // Second DELETE: parent=2, child id=20 must still cascade (the
    // visited set from statement 1 must NOT carry over).
    let r2 = engine.execute("DELETE FROM parent WHERE id = 2").unwrap();
    assert_eq!(affected(r2), 1);
    assert_eq!(crate::storage::fk_visited_len(), 0);
    let remaining_children = rows(engine.execute("SELECT id FROM child").unwrap());
    assert!(
        remaining_children.is_empty(),
        "child rows must cascade in the second statement too"
    );
}

/// M5.C5 — The default depth limit is now generous (64); a deep but
/// acyclic chain must run to completion instead of false-tripping the depth limit.
#[test]
fn fk_deep_acyclic_chain_does_not_error_3957() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    // Build a chain of 20 tables each FK'd to the previous one with
    // CASCADE. Deleting the head must cascade to all of them.
    let chain_len = 20;
    engine
        .execute("CREATE TABLE t0 (id INT PRIMARY KEY)")
        .unwrap();
    for i in 1..chain_len {
        engine
            .execute(&format!(
                "CREATE TABLE t{} (id INT PRIMARY KEY, pid INT, \
                 FOREIGN KEY (pid) REFERENCES t{}(id) ON DELETE CASCADE)",
                i,
                i - 1
            ))
            .unwrap();
    }
    engine.execute("INSERT INTO t0 VALUES (1)").unwrap();
    for i in 1..chain_len {
        engine
            .execute(&format!("INSERT INTO t{} VALUES (1, 1)", i))
            .unwrap();
    }
    // Should succeed. Pre-M5.C5 the chain would have hit the depth limit at
    // depth 16 with the old 15-deep limit.
    engine.execute("DELETE FROM t0 WHERE id = 1").unwrap();
    // Verify every table is empty.
    for i in 0..chain_len {
        let count = rows(engine.execute(&format!("SELECT id FROM t{}", i)).unwrap()).len();
        assert_eq!(count, 0, "table t{} should be empty after cascade", i);
    }
}

#[test]
fn partitioned_batch_odku_explicit_auto_ids_and_local_rowids() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let mut exec = |sql: &str| engine.execute_with_session(&mut session, sql);
    exec("CREATE TABLE pt (id BIGINT UNSIGNED AUTO_INCREMENT, p INT NOT NULL, v INT, PRIMARY KEY(id,p)) PARTITION BY RANGE(p) (PARTITION p0 VALUES LESS THAN(1), PARTITION p1 VALUES LESS THAN(2), PARTITION p2 VALUES LESS THAN(MAXVALUE))").unwrap();
    // Identical physical RowIds in different leaves must never alias.
    exec("INSERT INTO pt VALUES (1,0,10),(2,1,20),(3,2,30)").unwrap();
    exec("INSERT INTO pt VALUES (2,1,99) ON DUPLICATE KEY UPDATE v=VALUES(v)").unwrap();
    assert_eq!(
        rows(exec("SELECT v FROM pt ORDER BY id").unwrap()),
        vec![
            vec![Value::Int(10)],
            vec![Value::Int(99)],
            vec![Value::Int(30)]
        ]
    );
    exec("BEGIN").unwrap();
    let values = (100..228)
        .map(|id| format!("('{id}',{},1)", id % 3))
        .collect::<Vec<_>>()
        .join(",");
    let before = engine.database().dml_stats_snapshot();
    assert_eq!(
        affected(
            exec(&format!(
                "INSERT INTO pt VALUES {values} ON DUPLICATE KEY UPDATE v=v+VALUES(v)"
            ))
            .unwrap()
        ),
        128
    );
    assert_eq!(
        engine.database().dml_stats_snapshot().upsert_batch_rows - before.upsert_batch_rows,
        128
    );
    assert_eq!(
        affected(
            exec(&format!(
                "INSERT INTO pt VALUES {values} ON DUPLICATE KEY UPDATE v=v+VALUES(v)"
            ))
            .unwrap()
        ),
        256
    );
    exec("ROLLBACK").unwrap();
    assert_eq!(
        rows(exec("SELECT COUNT(*) FROM pt").unwrap())[0][0],
        Value::Int(3)
    );
    // A conflict can migrate a row; a non-RETURNING statement must not try to
    // fetch its new image using the old physical RowId afterwards.
    exec("INSERT INTO pt VALUES (2,1,1) ON DUPLICATE KEY UPDATE p=2,v=123").unwrap();
    assert_eq!(
        rows(exec("SELECT p,v FROM pt WHERE id=2").unwrap()),
        vec![vec![Value::Int(2), Value::Int(123)]]
    );
    exec("INSERT INTO pt(p,v) VALUES(0,7)").unwrap();
    assert_eq!(
        rows(exec("SELECT COUNT(*) FROM pt").unwrap())[0][0],
        Value::Int(4)
    );
}

#[test]
fn partitioned_batch_odku_preserves_duplicate_source_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE pt(id INT,p INT,v INT,PRIMARY KEY(id,p),UNIQUE KEY uv(v,p)) PARTITION BY HASH(p) PARTITIONS 8").unwrap();
    let mut values = (0..128)
        .map(|i| format!("({i},{}, {})", i % 8, i))
        .collect::<Vec<_>>();
    values.insert(80, "(0,0,999)".into());
    assert_eq!(
        affected(
            engine
                .execute(&format!(
                    "INSERT INTO pt VALUES {} ON DUPLICATE KEY UPDATE v=VALUES(v)",
                    values.join(",")
                ))
                .unwrap()
        ),
        130
    );
    assert_eq!(
        rows(
            engine
                .execute("SELECT v FROM pt WHERE id=0 AND p=0")
                .unwrap()
        )[0][0],
        Value::Int(999)
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM pt").unwrap())[0][0],
        Value::Int(128)
    );
}
