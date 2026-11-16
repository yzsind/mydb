use super::support::result_rows as rows;
use super::*;

/// Regression: on a RANGE-partitioned table, `SELECT ... ORDER BY <partition/pk column>`
/// must return the same rows as `ORDER BY <non-partition column>` and an unordered scan.
/// A bug caused the index-ordered scan path to return an empty result set on partitioned
/// tables (the partition-router prune set left over from planning pruned every partition).
#[test]
fn range_partition_order_by_pk_returns_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (5, 1), (15, 2)")
        .unwrap();

    let unordered = rows(engine.execute("SELECT id, v FROM t").unwrap());
    assert_eq!(unordered.len(), 2, "unordered scan should see both rows");

    let by_v = rows(engine.execute("SELECT id, v FROM t ORDER BY v").unwrap());
    assert_eq!(
        by_v,
        vec![
            vec![Value::Int(5), Value::Int(1)],
            vec![Value::Int(15), Value::Int(2)],
        ]
    );

    let by_id = rows(engine.execute("SELECT id, v FROM t ORDER BY id").unwrap());
    assert_eq!(
        by_id,
        vec![
            vec![Value::Int(5), Value::Int(1)],
            vec![Value::Int(15), Value::Int(2)],
        ],
        "ORDER BY partition/pk column must return all rows across partitions"
    );

    // DESC direction across partitions too.
    let by_id_desc = rows(
        engine
            .execute("SELECT id, v FROM t ORDER BY id DESC")
            .unwrap(),
    );
    assert_eq!(
        by_id_desc,
        vec![
            vec![Value::Int(15), Value::Int(2)],
            vec![Value::Int(5), Value::Int(1)],
        ]
    );
}

/// Same shape but with more rows per partition and a covering-only projection
/// (`SELECT id`), which exercises the index-only ordered scan path.
#[test]
fn range_partition_order_by_pk_covering_projection() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (20), \
               PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(5,50),(12,120),(15,150),(25,250),(30,300)")
        .unwrap();

    let by_id = rows(engine.execute("SELECT id FROM t ORDER BY id").unwrap());
    assert_eq!(
        by_id,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(5)],
            vec![Value::Int(12)],
            vec![Value::Int(15)],
            vec![Value::Int(25)],
            vec![Value::Int(30)],
        ]
    );

    // With a LIMIT (index-ordered limit path).
    let by_id_limit = rows(
        engine
            .execute("SELECT id FROM t ORDER BY id LIMIT 3")
            .unwrap(),
    );
    assert_eq!(
        by_id_limit,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(5)],
            vec![Value::Int(12)]
        ]
    );

    // MIN/MAX over the partition/pk column must span all partitions, not
    // report NULL from the (empty) base-table index.
    let max_id = rows(engine.execute("SELECT MAX(id) FROM t").unwrap());
    assert_eq!(max_id, vec![vec![Value::Int(30)]]);
    let min_id = rows(engine.execute("SELECT MIN(id) FROM t").unwrap());
    assert_eq!(min_id, vec![vec![Value::Int(1)]]);

    // A range predicate on the partition/pk column with an ORDER BY on it
    // (residual-free index range + ordered scan) must also return rows
    // across partitions.
    let ranged = rows(
        engine
            .execute("SELECT id FROM t WHERE id >= 5 AND id <= 25 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        ranged,
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(12)],
            vec![Value::Int(15)],
            vec![Value::Int(25)],
        ]
    );
}

/// Broad coverage of B-tree index access on a RANGE-partitioned table
/// whose partition key is also the primary key — the shape that exposed
/// the ambiguous cross-partition RowId bug. Every query below must
/// return the same rows as a partition-aware sequential scan.
#[test]
fn range_partition_index_access_matches_seq_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT) \
             PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (20), \
               PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,10),(5,50),(12,120),(15,150),(25,250),(30,300)")
        .unwrap();

    // Non-covering range scan across all partitions.
    assert_eq!(
        rows(
            engine
                .execute("SELECT id, v FROM t WHERE id > 3 ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(5), Value::Int(50)],
            vec![Value::Int(12), Value::Int(120)],
            vec![Value::Int(15), Value::Int(150)],
            vec![Value::Int(25), Value::Int(250)],
            vec![Value::Int(30), Value::Int(300)],
        ]
    );
    // Covering-only range scan.
    assert_eq!(
        rows(
            engine
                .execute("SELECT id FROM t WHERE id BETWEEN 5 AND 25 ORDER BY id")
                .unwrap()
        ),
        vec![
            vec![Value::Int(5)],
            vec![Value::Int(12)],
            vec![Value::Int(15)],
            vec![Value::Int(25)],
        ]
    );
    // ORDER BY + LIMIT across partitions (Sort must not be elided).
    assert_eq!(
        rows(
            engine
                .execute("SELECT id FROM t WHERE id > 3 ORDER BY id LIMIT 2")
                .unwrap()
        ),
        vec![vec![Value::Int(5)], vec![Value::Int(12)]]
    );
    assert_eq!(
        rows(
            engine
                .execute("SELECT id, v FROM t WHERE id > 3 ORDER BY id LIMIT 2")
                .unwrap()
        ),
        vec![
            vec![Value::Int(5), Value::Int(50)],
            vec![Value::Int(12), Value::Int(120)],
        ]
    );
    // Aggregates across partitions must not read the empty base index.
    assert_eq!(
        rows(
            engine
                .execute("SELECT COUNT(*) FROM t WHERE id > 3")
                .unwrap()
        ),
        vec![vec![Value::Int(5)]]
    );
    assert_eq!(
        rows(engine.execute("SELECT COUNT(*) FROM t").unwrap()),
        vec![vec![Value::Int(6)]]
    );
    assert_eq!(
        rows(engine.execute("SELECT MAX(id), MIN(id) FROM t").unwrap()),
        vec![vec![Value::Int(30), Value::Int(1)]]
    );
    // Point lookup pruned to a single partition still works.
    assert_eq!(
        rows(engine.execute("SELECT id, v FROM t WHERE id = 15").unwrap()),
        vec![vec![Value::Int(15), Value::Int(150)]]
    );
}
