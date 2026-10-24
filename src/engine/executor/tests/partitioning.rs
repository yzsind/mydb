//! End-to-end tests for MySQL-compatible partition tables.
//!
//! These tests open a fresh storage engine, run CREATE TABLE with a
//! PARTITION BY clause, INSERT rows that route to different
//! partitions, then SELECT them back to verify the per-partition heap
//! files actually receive the right rows.

use super::*;

#[test]
fn partition_audit_1001_exchange_recovers_auto_increment_highwater() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for crash_point in ["exchange_after_catalog", "exchange_after_swap"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        {
            let engine = open_partition_recovery_test_engine(dir.path());
            engine.execute("CREATE TABLE ai_part(id INT AUTO_INCREMENT,p INT,PRIMARY KEY(id,p)) PARTITION BY RANGE(p) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
            engine.execute("CREATE TABLE ai_stage(id INT AUTO_INCREMENT,p INT,PRIMARY KEY(id,p)) AUTO_INCREMENT=1000").unwrap();
            engine.execute("INSERT INTO ai_part VALUES(1,5)").unwrap();
            engine
                .execute("INSERT INTO ai_stage VALUES(100,6)")
                .unwrap();
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", crash_point);
            engine
                .execute("ALTER TABLE ai_part EXCHANGE PARTITION p0 WITH TABLE ai_stage")
                .unwrap();
        }
        let engine = open_partition_recovery_test_engine(dir.path());
        engine.execute("INSERT INTO ai_stage(p) VALUES(7)").unwrap();
        assert_eq!(
            partition_test_rows(&engine, "SELECT id FROM ai_stage ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(1000)]],
            "{crash_point}"
        );
        assert_eq!(
            partition_test_rows(&engine, "SELECT id FROM ai_part"),
            vec![vec![Value::Int(100)]],
            "{crash_point}"
        );
    }
}

#[test]
fn partition_audit_1001_join_scopes_are_per_reference() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    for name in ["pa", "pb"] {
        engine.execute(&format!("CREATE TABLE {name}(id INT,p INT,PRIMARY KEY(id,p)) PARTITION BY RANGE(p) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(1,5),(2,50)"))
            .unwrap();
    }
    for sql in [
        "SELECT x.id FROM pa PARTITION(p0) x JOIN pb y ON x.id=y.id",
        "SELECT x.id FROM pa PARTITION(p0) x JOIN pa PARTITION(p1) y ON x.id<>y.id",
        "SELECT x.id FROM pa PARTITION(p0) x JOIN pa y ON x.id<>y.id",
        "SELECT x.id FROM pb y JOIN pa PARTITION(p0) x ON x.id=y.id",
        "SELECT x.id FROM pa PARTITION(p0) x JOIN pb y ON x.id=y.id WHERE x.id=1",
        "SELECT id FROM pa PARTITION(p0) WHERE (SELECT COUNT(*) FROM pb)=2",
        "SELECT id FROM pa PARTITION(p0) WHERE (SELECT COUNT(*) FROM pa)=2",
        "SELECT id FROM pa PARTITION(p0) WHERE id IN (SELECT id FROM pb)",
    ] {
        assert_eq!(
            partition_test_rows(&engine, sql),
            vec![vec![Value::Int(1)]],
            "{sql}"
        );
    }
    engine
        .execute("UPDATE pa PARTITION(p0) x JOIN pb y ON x.id=y.id SET x.p=6,y.p=7")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT p FROM pb ORDER BY id"),
        vec![vec![Value::Int(7)], vec![Value::Int(50)]]
    );
    engine
        .execute("DELETE x FROM pa PARTITION(p0) x JOIN pb y ON x.id=y.id")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM pa"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn partition_audit_1001_routines_restore_caller_scope() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    for name in ["rt", "rt_other"] {
        engine.execute(&format!("CREATE TABLE {name}(id INT,p INT,PRIMARY KEY(id,p)) PARTITION BY RANGE(p) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES(1,5),(2,50)"))
            .unwrap();
    }
    for (name, table) in [("cnt_same", "rt"), ("cnt_other", "rt_other")] {
        engine.execute(&format!("CREATE FUNCTION {name}() RETURNS INT READS SQL DATA BEGIN DECLARE n INT; SELECT COUNT(*) INTO n FROM {table}; RETURN n; END")).unwrap();
        for partition in ["p0", "p1"] {
            assert_eq!(
                partition_test_rows(
                    &engine,
                    &format!("SELECT {name}() FROM rt PARTITION({partition})")
                ),
                vec![vec![Value::Int(2)]]
            );
        }
    }
    engine.execute("CREATE FUNCTION write_other() RETURNS INT MODIFIES SQL DATA BEGIN UPDATE rt_other SET p=p+1; RETURN 1; END").unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT write_other() FROM rt PARTITION(p0)"),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT p FROM rt_other ORDER BY id"),
        vec![vec![Value::Int(6)], vec![Value::Int(51)]]
    );
    // The outer scope is restored after a routine error as well.
    engine.execute("CREATE FUNCTION fail_scope() RETURNS INT BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fail'; RETURN 0; END").unwrap();
    assert!(engine
        .execute("SELECT fail_scope() FROM rt PARTITION(p0)")
        .is_err());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM rt PARTITION(p1)"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn partition_audit_1001_collation_layout_validation() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (collation, left, right) in [
        ("utf8mb4_0900_ai_ci", "ABC", "abc"),
        ("utf8mb4_0900_ai_ci", "é", "e"),
        ("utf8mb4_general_ci", "a ", "a"),
    ] {
        let sql = format!("CREATE TABLE bad(c VARCHAR(10) COLLATE {collation}) PARTITION BY LIST COLUMNS(c) (PARTITION p0 VALUES IN('{left}'),PARTITION p1 VALUES IN('{right}'))");
        assert_eq!(engine.execute(&sql).unwrap_err().code(), 1495, "{sql}");
    }
    assert_eq!(engine.execute("CREATE TABLE bad(c VARCHAR(10) COLLATE utf8mb4_0900_ai_ci) PARTITION BY RANGE COLUMNS(c) (PARTITION p0 VALUES LESS THAN('B'),PARTITION p1 VALUES LESS THAN('a'))").unwrap_err().code(), 1493);
    engine.execute("CREATE TABLE good(c VARCHAR(10) COLLATE utf8mb4_bin) PARTITION BY LIST COLUMNS(c) (PARTITION p0 VALUES IN('ABC'),PARTITION p1 VALUES IN('abc'))").unwrap();
    engine
        .execute("INSERT INTO good VALUES('ABC'),('abc')")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT c FROM good PARTITION(p1)"),
        vec![vec![Value::String("abc".into())]]
    );
    engine.execute("CREATE TABLE al(c VARCHAR(10) COLLATE utf8mb4_0900_ai_ci,n INT) PARTITION BY LIST COLUMNS(c,n) (PARTITION p0 VALUES IN(('A',1)))").unwrap();
    assert_eq!(
        engine
            .execute("ALTER TABLE al ADD PARTITION(PARTITION p1 VALUES IN(('a',1)))")
            .unwrap_err()
            .code(),
        1495
    );
    engine
        .execute("ALTER TABLE al ADD PARTITION(PARTITION p1 VALUES IN(('a',2)))")
        .unwrap();
}

#[test]
fn partition_audit_1001_ddl_unknown_columns_and_prefix_keys() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    for expr in ["YEAR(no_such)", "ABS(no_such)", "id+no_such"] {
        assert_eq!(engine.execute(&format!("CREATE TABLE bad(id INT) PARTITION BY RANGE({expr}) (PARTITION p0 VALUES LESS THAN(2000))")).unwrap_err().code(), 1054);
    }
    assert_eq!(engine.execute("CREATE TABLE bad(id INT) PARTITION BY RANGE(id) SUBPARTITION BY HASH(no_such) (PARTITION p0 VALUES LESS THAN(10))").unwrap_err().code(),1054);
    assert_eq!(engine.execute("CREATE TABLE bad(vc VARCHAR(100),UNIQUE KEY uk(vc(10))) PARTITION BY KEY(vc) PARTITIONS 4").unwrap_err().code(),1503);
    engine.execute("CREATE TABLE good(vc VARCHAR(100),s VARCHAR(100),UNIQUE KEY uk(vc,s(10))) PARTITION BY KEY(vc) PARTITIONS 4").unwrap();
    assert_eq!(engine.execute("CREATE TABLE bad(id INT) PARTITION BY HASH(id) PARTITIONS 4 (PARTITION p0,PARTITION p1)").unwrap_err().code(),1484);
    for sql in [
        "CREATE TABLE bad(id INT) PARTITION BY HASH(id) PARTITIONS 0",
        "ALTER TABLE good ADD PARTITION PARTITIONS 0",
        "ALTER TABLE good COALESCE PARTITION 0",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1504, "{sql}");
    }
}

#[test]
fn partition_audit_1001_hex_literals_and_trailing_ddl() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (name, literal, expected) in [
        ("hx0", "0x41", "41000000"),
        ("hx1", "X'41'", "41000000"),
        ("hx2", "0xABC", "0ABC0000"),
    ] {
        engine.execute(&format!("CREATE TABLE {name}(b BINARY(4)) PARTITION BY LIST COLUMNS(b) (PARTITION p0 VALUES IN({literal}))")).unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES({literal})"))
            .unwrap();
        assert_eq!(
            partition_test_rows(&engine, &format!("SELECT HEX(b) FROM {name}")),
            vec![vec![Value::String(expected.into())]]
        );
    }
    // MySQL 8.0.46 recognizes these digit-leading names as identifiers;
    // non-finite exponent literals have their own numeric diagnostic.
    for (literal, code) in [
        ("0xZZ", 1054),
        ("0x", 1054),
        ("1bad", 1054),
        ("1e9999", 1367),
    ] {
        assert_eq!(engine.execute(&format!("CREATE TABLE bad(b BINARY(4)) PARTITION BY LIST COLUMNS(b) (PARTITION p0 VALUES IN({literal}))")).unwrap_err().code(),code);
    }
    engine.execute("CREATE TABLE tg(id INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("INSERT INTO tg VALUES(1),(20)").unwrap();
    // The engine API intentionally supports batches. The extractor itself
    // must reject a second statement rather than swallowing its source.
    assert!(crate::parser_partition::extract_alter_partition_actions(
        "ALTER TABLE tg DROP PARTITION p0; SELECT 1"
    )
    .is_err());
    for tail in ["RANDOM_GARBAGE", ","] {
        assert_eq!(
            engine
                .execute(&format!("ALTER TABLE tg DROP PARTITION p0 {tail}"))
                .unwrap_err()
                .code(),
            1064
        );
        assert_eq!(
            partition_test_rows(&engine, "SELECT COUNT(*) FROM tg"),
            vec![vec![Value::Int(2)]]
        );
    }
    engine.execute("ALTER TABLE tg DROP PARTITION p0;").unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM tg"),
        vec![vec![Value::Int(20)]]
    );
}

#[test]
fn partition_audit_1001_metadata_roundtrip_explain_and_stats() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE sp(id INT,y INT) PARTITION BY RANGE(y) SUBPARTITION BY HASH(id) (PARTITION p0 VALUES LESS THAN(10) (SUBPARTITION p0sp0,SUBPARTITION p0sp1),PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION p1sp0,SUBPARTITION p1sp1))").unwrap();
    let ddl = partition_test_rows(&engine, "SHOW CREATE TABLE sp")[0][1].to_display_string();
    engine
        .execute(&ddl.replacen("`sp`", "`sp_copy`", 1))
        .unwrap();
    let source = engine.db.table("sp").unwrap();
    let copy = engine.db.table("sp_copy").unwrap();
    assert_eq!(source.storage_ids().len(), 4);
    assert_eq!(copy.storage_ids().len(), 4);
    engine
        .execute("CREATE TABLE ord(id INT) PARTITION BY HASH(id) PARTITIONS 11")
        .unwrap();
    let rows = partition_test_rows(&engine,"SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='ord'");
    assert_eq!(
        rows,
        (0..11)
            .map(|i| vec![Value::String(format!("p{i}"))])
            .collect::<Vec<_>>()
    );
    engine
        .execute(
            "CREATE TABLE st(id INT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10))",
        )
        .unwrap();
    engine.execute("INSERT INTO st VALUES(1),(2)").unwrap();
    engine.execute("ANALYZE TABLE st").unwrap();
    engine
        .execute("ALTER TABLE st ADD PARTITION(PARTITION p1 VALUES LESS THAN(20))")
        .unwrap();
    engine
        .execute("INSERT INTO st VALUES(11),(12),(13)")
        .unwrap();
    assert_eq!(partition_test_rows(&engine,"SELECT TABLE_ROWS FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA='test' AND TABLE_NAME='st' ORDER BY PARTITION_ORDINAL_POSITION"),vec![vec![Value::Int(2)],vec![Value::Int(3)]]);
    for (sql, expected) in [
        ("EXPLAIN SELECT * FROM st PARTITION(p0)", "p0"),
        (
            "EXPLAIN SELECT * FROM st PARTITION(p0) WHERE id=11",
            "empty",
        ),
    ] {
        assert_eq!(
            partition_test_rows(&engine, sql)[0][3].to_display_string(),
            expected
        );
    }
}

fn partition_test_rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap().materialize().unwrap() else {
        panic!("expected rows: {sql}")
    };
    rs.rows
}

#[test]
fn partition_key_pruning_normalizes_literals_without_losing_coercion_matches() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (name, kind, value, predicate) in [
        (
            "date_key",
            "DATETIME",
            "'2026-09-10 07:03:56'",
            "k='2026-09-10 07:03:56'",
        ),
        ("int_key", "INT", "42", "k='42'"),
        ("string_key", "VARCHAR(16)", "'01'", "k=1"),
        ("decimal_key", "DECIMAL(10,2)", "1.00", "k=1.0"),
    ] {
        engine
            .execute(&format!(
                "CREATE TABLE {name} (id INT, k {kind}) PARTITION BY KEY(k) PARTITIONS 17"
            ))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO {name} VALUES (1,{value})"))
            .unwrap();
        assert_eq!(
            partition_test_rows(&engine, &format!("SELECT id FROM {name} WHERE {predicate}")),
            vec![vec![Value::Int(1)]],
            "{name}"
        );
        engine
            .execute(&format!("UPDATE {name} SET id=2 WHERE {predicate} LIMIT 1"))
            .unwrap();
        assert_eq!(
            partition_test_rows(&engine, &format!("SELECT id FROM {name}")),
            vec![vec![Value::Int(2)]],
            "{name}"
        );
    }
    engine.execute("CREATE TABLE sub_typed (id INT, k DATETIME) PARTITION BY RANGE(id) SUBPARTITION BY KEY(k) SUBPARTITIONS 17 (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute(
            "INSERT INTO sub_typed VALUES (1,'2026-09-10 07:03:56'),(11,'2026-09-10 07:03:56')",
        )
        .unwrap();
    assert_eq!(
        partition_test_rows(
            &engine,
            "SELECT id FROM sub_typed WHERE k='2026-09-10 07:03:56' ORDER BY id"
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(11)]]
    );
}

#[test]
fn partition_expression_pruning_matrix_and_parameter_rebinding() {
    use super::super::partition_router::pruned_storage_ids_from_filter;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE prune_matrix (id INT, dt DATETIME) PARTITION BY RANGE (YEAR(dt)) (PARTITION old VALUES LESS THAN (2020), PARTITION mid VALUES LESS THAN (2025), PARTITION recent VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("INSERT INTO prune_matrix VALUES (1,'2019-12-31 23:59:59'),(2,'2020-01-01'),(3,'2024-12-31 23:59:59'),(4,'2025-01-01'),(5,NULL)").unwrap();
    let table = engine.db.table("test.prune_matrix").unwrap();
    for (predicate, expected_leaves, expected_ids) in [
        ("dt='2025-01-01'", vec![2], vec![4]),
        ("dt IS NULL", vec![0], vec![5]),
        (
            "dt IN ('2019-12-31 23:59:59','2025-01-01')",
            vec![0, 2],
            vec![1, 4],
        ),
        (
            "dt>='2020-01-01' AND dt<'2025-01-01'",
            vec![1, 2],
            vec![2, 3],
        ),
        ("dt<'2020-01-01'", vec![0, 1], vec![1]),
        (
            "dt='2019-12-31 23:59:59' OR dt='2025-01-01'",
            vec![0, 2],
            vec![1, 4],
        ),
    ] {
        let expr = crate::parser::parse_expr_fragment(predicate).unwrap();
        let ids = pruned_storage_ids_from_filter(&table, Some(&expr)).unwrap();
        assert_eq!(
            ids,
            expected_leaves
                .iter()
                .map(|i| table.storage_ids()[*i])
                .collect::<Vec<_>>(),
            "{predicate}"
        );
        assert_eq!(
            partition_test_rows(
                &engine,
                &format!("SELECT id FROM prune_matrix WHERE {predicate} ORDER BY id")
            ),
            expected_ids
                .into_iter()
                .map(|i| vec![Value::Int(i)])
                .collect::<Vec<_>>(),
            "{predicate}"
        );
    }
    let parameter = Expr::Binary {
        left: Box::new(Expr::Identifier("dt".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Placeholder(0)),
    };
    for (date, ordinal) in [("2019-01-01", 0), ("2026-01-01", 2), ("2022-01-01", 1)] {
        crate::engine::params::with_params(vec![Value::String(date.into())], || {
            assert_eq!(
                pruned_storage_ids_from_filter(&table, Some(&parameter)),
                Some(vec![table.storage_ids()[ordinal]])
            );
        });
    }
    let unknown = crate::parser::parse_expr_fragment("dt='2026-01-01' OR id=1").unwrap();
    assert!(pruned_storage_ids_from_filter(&table, Some(&unknown)).is_none());
}

#[test]
fn partition_expression_multicolumn_and_subexpression_bindings() {
    use super::super::partition_router::pruned_storage_ids_from_filter;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE compound_part (a INT, b INT) PARTITION BY HASH(a+b) PARTITIONS 4")
        .unwrap();
    engine
        .execute("INSERT INTO compound_part VALUES (1,2),(2,2),(3,2)")
        .unwrap();
    let table = engine.db.table("test.compound_part").unwrap();
    for (predicate, expected) in [
        ("a=1 AND b=2", Some(vec![3])),
        ("a IN (1,2) AND b=2", Some(vec![0, 3])),
        ("a=1", None),
    ] {
        let expr = crate::parser::parse_expr_fragment(predicate).unwrap();
        assert_eq!(
            pruned_storage_ids_from_filter(&table, Some(&expr)),
            expected.map(|indices| indices
                .into_iter()
                .map(|i| table.storage_ids()[i])
                .collect())
        );
    }
    engine.execute("CREATE TABLE child_expr (a INT, b INT) PARTITION BY RANGE(a) SUBPARTITION BY HASH(b+1) SUBPARTITIONS 4 (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    let table = engine.db.table("test.child_expr").unwrap();
    for (predicate, leaves) in [("b=1", 2), ("b=1 OR b=2", 4), ("a=1 AND b=1", 1)] {
        let expr = crate::parser::parse_expr_fragment(predicate).unwrap();
        assert_eq!(
            pruned_storage_ids_from_filter(&table, Some(&expr))
                .unwrap()
                .len(),
            leaves
        );
    }
}

#[test]
fn partition_dml_native_explain_matches_candidate_access() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE native_part (id INT, dt DATETIME, v INT, PRIMARY KEY(id,dt)) PARTITION BY RANGE(YEAR(dt)) (PARTITION old VALUES LESS THAN(2020), PARTITION recent VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("INSERT INTO native_part VALUES (1,'2019-01-01',10),(2,'2026-01-01',20)")
        .unwrap();
    let sql = "UPDATE native_part SET v=v+1 WHERE id=2 AND dt='2026-01-01' LIMIT 1";
    let rows = partition_test_rows(&engine, &format!("EXPLAIN FORMAT=TREE {sql}"));
    let Value::String(tree) = &rows[0][0] else {
        panic!()
    };
    assert!(tree.contains("Partition DML const using PRIMARY"), "{tree}");
    assert!(tree.contains("partitions: recent"), "{tree}");
    assert!(
        !tree.contains("pipeline="),
        "must not report a SELECT pipeline: {tree}"
    );
    let rows = partition_test_rows(&engine, &format!("EXPLAIN {sql}"));
    assert_eq!(rows[0][3], Value::String("recent".into()));
    assert_eq!(rows[0][6], Value::String("PRIMARY".into()));
    let rows = partition_test_rows(&engine, &format!("EXPLAIN ANALYZE FORMAT=TREE {sql}"));
    let Value::String(tree) = &rows[0][0] else {
        panic!()
    };
    assert!(tree.contains("candidates=1"), "{tree}");
    assert_eq!(
        partition_test_rows(&engine, "SELECT v FROM native_part WHERE id=2"),
        vec![vec![Value::Int(21)]]
    );
    let rows = partition_test_rows(
        &engine,
        "EXPLAIN FORMAT=JSON DELETE FROM native_part WHERE id=2 AND dt='2026-01-01' LIMIT 1",
    );
    let Value::String(json) = &rows[0][0] else {
        panic!()
    };
    let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
    assert!(parsed.to_string().contains("partition_dml_candidates"));
    assert_eq!(
        partition_test_rows(&engine, "SELECT COUNT(*) FROM native_part"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn partition_dml_origin_scope_movement_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE origin_part (id INT, p INT, v INT, PRIMARY KEY(id,p), KEY(v)) PARTITION BY HASH(p) PARTITIONS 4").unwrap();
    engine
        .execute("INSERT INTO origin_part VALUES (1,0,10),(2,1,20),(3,2,30),(4,3,40)")
        .unwrap();
    engine
        .execute("UPDATE origin_part SET v=99 WHERE id=3")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT v FROM origin_part ORDER BY id"),
        vec![
            vec![Value::Int(10)],
            vec![Value::Int(20)],
            vec![Value::Int(99)],
            vec![Value::Int(40)]
        ]
    );
    engine
        .execute("UPDATE origin_part PARTITION(p0) SET v=100 WHERE id=3 AND p=2 LIMIT 1")
        .unwrap();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE origin_part SET p=1,v=77 WHERE id=3 AND p=2 LIMIT 1",
        )
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "DELETE FROM origin_part WHERE id IN(1,2) ORDER BY id DESC LIMIT 1",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id,p,v FROM origin_part ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(0), Value::Int(10)],
            vec![Value::Int(2), Value::Int(1), Value::Int(20)],
            vec![Value::Int(3), Value::Int(2), Value::Int(99)],
            vec![Value::Int(4), Value::Int(3), Value::Int(40)]
        ]
    );
    let returned = partition_test_rows(
        &engine,
        "DELETE FROM origin_part WHERE id IN(1,3) RETURNING id",
    );
    assert_eq!(returned.len(), 2);
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM origin_part ORDER BY id"),
        vec![vec![Value::Int(2)], vec![Value::Int(4)]]
    );
}

#[test]
fn partition_expression_equality_prunes_compiled_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE expr_prune (id INT, dt DATETIME) PARTITION BY RANGE (TO_DAYS(dt)) (PARTITION old VALUES LESS THAN (739039), PARTITION recent VALUES LESS THAN MAXVALUE)").unwrap();
    let table = engine.db.table("test.expr_prune").unwrap();
    let filter = Expr::Binary {
        left: Box::new(Expr::Identifier("dt".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::String("2026-09-10 07:03:56".into()))),
    };
    assert_eq!(
        super::super::partition_router::pruned_storage_ids_from_filter(&table, Some(&filter)),
        Some(vec![table.storage_ids()[1]])
    );
}

#[test]
fn partition_subpartition_prunes_without_parent_binding() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE sub_prune (id INT, y INT) PARTITION BY RANGE (y) SUBPARTITION BY HASH(id) SUBPARTITIONS 4 (PARTITION old VALUES LESS THAN (2020), PARTITION recent VALUES LESS THAN MAXVALUE)").unwrap();
    let table = engine.db.table("test.sub_prune").unwrap();
    let filter = Expr::Binary {
        left: Box::new(Expr::Identifier("id".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::Int(1))),
    };
    let ids = super::super::partition_router::pruned_storage_ids_from_filter(&table, Some(&filter))
        .expect("child pruning is independent of parent pruning");
    assert_eq!(ids.len(), 2);
}

#[test]
fn partition_limited_dml_uses_index_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE limited_part (id INT, p INT, v INT, PRIMARY KEY(id,p)) PARTITION BY HASH(p) PARTITIONS 4").unwrap();
    engine
        .execute("INSERT INTO limited_part SELECT n, MOD(n,4), n FROM generate_series(1,100) g(n)")
        .unwrap();
    let table = engine.db.table("test.limited_part").unwrap();
    let filter = Expr::Binary {
        left: Box::new(Expr::Identifier("id".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Literal::Int(99))),
    };
    let txn = engine
        .db
        .read_only_statement_view(IsolationLevel::ReadCommitted, None);
    crate::engine::api::begin_statement_dml_candidate_count();
    let candidates = super::super::dml::ordered_limited_dml_candidates(
        &engine.db,
        txn,
        &table,
        Some(&filter),
        &[],
        Some(&LimitValue::Rows(1)),
        true,
    )
    .unwrap()
    .unwrap();
    assert_eq!(candidates.rows.len(), 1);
    assert_eq!(
        crate::engine::api::take_statement_dml_candidate_rows(),
        Some(1),
        "must probe indexes instead of scanning heaps"
    );
}

/// Partition semantics do not require an fsync for every DDL/DML statement.
/// Recovery-specific cases below opt into the strict fixture explicitly.
fn open_partition_test_engine(data_dir: &std::path::Path) -> StorageQueryEngine {
    open_test_engine(data_dir)
}

fn open_partition_recovery_test_engine(data_dir: &std::path::Path) -> StorageQueryEngine {
    open_strict_test_engine(data_dir)
}

fn object_file_names(data_dir: &std::path::Path) -> Vec<String> {
    let mut names = Vec::new();
    let root = crate::storage::naming::objects_root(data_dir);
    let mut pending = vec![root.clone()];
    while let Some(dir) = pending.pop() {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                } else if let Ok(relative) = path.strip_prefix(&root) {
                    names.push(relative.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
    names
}

#[test]
fn create_range_partitioned_table_round_trips_inserts_and_selects() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE sales (\
             id BIGINT NOT NULL, \
             year INT NOT NULL, \
             amount INT NOT NULL\
             ) PARTITION BY RANGE (year) (\
               PARTITION p_old VALUES LESS THAN (2020), \
               PARTITION p_mid VALUES LESS THAN (2024), \
               PARTITION p_new VALUES LESS THAN MAXVALUE\
             )",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO sales (id, year, amount) VALUES \
             (1, 2018, 100), (2, 2020, 200), (3, 2023, 300), (4, 2030, 400)",
        )
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM sales ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, year FROM sales WHERE year >= 2024 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(4), Value::Int(2030)]]);
}

#[test]
fn create_list_partitioned_table_routes_by_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE region_orders (\
             id BIGINT NOT NULL, \
             region INT NOT NULL\
             ) PARTITION BY LIST (region) (\
               PARTITION p_west VALUES IN (1, 2, 3), \
               PARTITION p_east VALUES IN (4, 5, 6)\
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO region_orders (id, region) VALUES (10, 1), (20, 5), (30, 3), (40, 6)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM region_orders ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 4);
}

#[test]
fn list_partitioned_table_rejects_value_outside_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE bucket_only (\
             id INT NOT NULL, kind INT NOT NULL\
             ) PARTITION BY LIST (kind) (\
               PARTITION pa VALUES IN (1), PARTITION pb VALUES IN (2)\
             )",
        )
        .unwrap();
    let err = engine
        .execute("INSERT INTO bucket_only VALUES (1, 99)")
        .unwrap_err();
    assert_eq!(err.code(), 1526);
}

#[test]
fn list_partition_default_value_is_rejected_before_create() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    let err = engine
        .execute(
            "CREATE TABLE t (id INT) PARTITION BY LIST(id) (\
               PARTITION p0 VALUES IN (1), PARTITION pd VALUES IN (DEFAULT))",
        )
        .expect_err("LIST DEFAULT must not become a catch-all partition");
    assert!(
        err.message().to_ascii_uppercase().contains("DEFAULT"),
        "unexpected error: {err:?}"
    );
    assert!(engine.database().table("test.t").is_err());
}

#[test]
fn create_hash_partitioned_table_distributes_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute("CREATE TABLE evts (id BIGINT NOT NULL) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    let rows_sql: Vec<String> = (0..40).map(|i| format!("({})", i)).collect();
    let sql = format!("INSERT INTO evts VALUES {}", rows_sql.join(", "));
    engine.execute(&sql).unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM evts").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(40));
}

#[test]
fn key_partitioned_table_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE keyed (\
             id BIGINT NOT NULL, \
             country VARCHAR(8) NOT NULL\
             ) PARTITION BY KEY (country) PARTITIONS 3",
        )
        .unwrap();
    engine
        .execute("INSERT INTO keyed VALUES (1, 'US'), (2, 'JP'), (3, 'DE'), (4, 'FR')")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM keyed").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(4));
}

#[test]
fn key_algorithms_drive_routing_and_pruning_consistently() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter, route_row,
    };

    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE key_v1 (id INT) \
             PARTITION BY KEY ALGORITHM=1 (id) PARTITIONS 17",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE key_v2 (id INT) \
             PARTITION BY KEY ALGORITHM=2 (id) PARTITIONS 17",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE key_default (id INT) PARTITION BY KEY(id) PARTITIONS 17")
        .unwrap();

    let v1 = engine.database().table("test.key_v1").unwrap();
    let v2 = engine.database().table("test.key_v2").unwrap();
    let default = engine.database().table("test.key_default").unwrap();
    let v1_part = v1.partitioning.as_ref().unwrap();
    let v2_part = v2.partitioning.as_ref().unwrap();
    let default_part = default.partitioning.as_ref().unwrap();
    assert_eq!(v1_part.key_algorithm, Some(1));
    assert_eq!(v2_part.key_algorithm, Some(2));
    assert_eq!(default_part.key_algorithm, Some(2));

    let mut algorithms_differ = false;
    for id in 0..256 {
        let row = [Value::Int(id)];
        let bucket_v1 = route_row(&v1, v1_part, &row).unwrap().index;
        let bucket_v2 = route_row(&v2, v2_part, &row).unwrap().index;
        let bucket_default = route_row(&default, default_part, &row).unwrap().index;
        algorithms_differ |= bucket_v1 != bucket_v2;
        assert_eq!(bucket_default, bucket_v2, "default KEY algorithm must be 2");
    }
    assert!(
        algorithms_differ,
        "ALGORITHM=1 and ALGORITHM=2 must not share one kernel"
    );
    drop((v1, v2, default));

    let rows = (0..256)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO key_v1 VALUES {rows}"))
        .unwrap();
    engine
        .execute(&format!("INSERT INTO key_v2 VALUES {rows}"))
        .unwrap();
    for table in ["key_v1", "key_v2"] {
        reset_partition_visit_counter();
        let QueryResponse::ResultSet(result) = engine
            .execute(&format!("SELECT id FROM {table} WHERE id = 97"))
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(result.rows, vec![vec![Value::Int(97)]]);
        assert_eq!(
            partition_visit_count(),
            1,
            "{table} pruning must use the same KEY algorithm as DML routing"
        );
    }
}

#[test]
fn key_string_hash_respects_column_collation_and_null() {
    use crate::engine::executor::partition_router::route_row;

    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (name VARCHAR(16) COLLATE utf8mb4_0900_ai_ci) \
             PARTITION BY KEY(name) PARTITIONS 16",
        )
        .unwrap();
    let table = engine.database().table("test.t").unwrap();
    let part = table.partitioning.as_ref().unwrap();
    let lower = route_row(&table, part, &[Value::String("cafe".into())])
        .unwrap()
        .index;
    let upper = route_row(&table, part, &[Value::String("CAFE".into())])
        .unwrap()
        .index;
    assert_eq!(
        lower, upper,
        "case-insensitive equal keys must share a bucket"
    );
    route_row(&table, part, &[Value::Null]).unwrap();
}

#[test]
fn range_partition_delete_removes_rows_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE\
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 7), (4, 20)")
        .unwrap();
    engine.execute("DELETE FROM t WHERE id = 3").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(4)]
        ]
    );
}

#[test]
fn range_partition_update_can_migrate_across_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE\
             )",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    engine.execute("UPDATE t SET y = 50 WHERE id = 1").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, y FROM t WHERE id = 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1), Value::Int(50)]]);
}

#[test]
fn partition_index_dml_keeps_partition_local_row_ids_disambiguated() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t_origin (id INT, y INT, v INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE\
             )",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX ix_id ON t_origin(id)")
        .unwrap();
    // Each fresh partition heap normally allocates the same first RowId.
    engine
        .execute("INSERT INTO t_origin VALUES (1, 5, 100), (11, 15, 200)")
        .unwrap();
    engine
        .execute("UPDATE t_origin SET v = v + 1 WHERE id = 11")
        .unwrap();
    engine.execute("DELETE FROM t_origin WHERE id = 1").unwrap();

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id, v FROM t_origin ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(11), Value::Int(201)]]);
}

#[test]
fn show_create_table_emits_partition_clause() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SHOW CREATE TABLE t").unwrap() else {
        panic!("expected result set");
    };
    let create_sql = match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {:?}", other),
    };
    assert!(create_sql.contains("PARTITION BY RANGE"), "{}", create_sql);
    assert!(create_sql.contains("PARTITION `p0`"), "{}", create_sql);
    assert!(create_sql.contains("MAXVALUE"), "{}", create_sql);
}

#[test]
fn information_schema_partitions_lists_each_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE p (id INT) PARTITION BY HASH(id) PARTITIONS 3")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT PARTITION_NAME, PARTITION_METHOD FROM information_schema.partitions \
             WHERE TABLE_NAME = 'p' ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 3);
    for row in &rs.rows {
        match &row[1] {
            Value::String(s) => assert_eq!(s, "HASH"),
            other => panic!("unexpected {:?}", other),
        }
    }
}

#[test]
fn alter_table_add_partition_extends_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (2020), \
                PARTITION p1 VALUES LESS THAN (2024))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 2010), (2, 2022)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (2030))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (3, 2026)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(3));
}

#[test]
fn alter_table_drop_partition_removes_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 50)")
        .unwrap();
    engine.execute("ALTER TABLE t DROP PARTITION p0").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn alter_table_truncate_partition_empties_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 50), (3, 7)")
        .unwrap();
    engine
        .execute("ALTER TABLE t TRUNCATE PARTITION p0")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
}

#[test]
fn range_partition_eq_filter_prunes_to_single_partition() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)")
        .unwrap();
    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE y = 5").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    let visits = partition_visit_count();
    assert_eq!(
        visits, 1,
        "expected exactly 1 partition visit, got {} (pruning may be off)",
        visits
    );
}

#[test]
fn hash_partition_eq_filter_prunes_to_single_partition() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    let rows_sql: Vec<String> = (0..20).map(|i| format!("({})", i)).collect();
    engine
        .execute(&format!("INSERT INTO t VALUES {}", rows_sql.join(", ")))
        .unwrap();
    reset_partition_visit_counter();
    let QueryResponse::ResultSet(_) = engine.execute("SELECT id FROM t WHERE id = 7").unwrap()
    else {
        panic!("expected result set");
    };
    let visits = partition_visit_count();
    assert_eq!(
        visits, 1,
        "HASH pruning should visit exactly 1 partition, got {}",
        visits
    );
}

#[test]
fn range_partition_lt_filter_prunes_to_prefix_partitions() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN (30), \
                PARTITION p3 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .unwrap();
    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE y < 12 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    let visits = partition_visit_count();
    assert!(
        visits <= 2,
        "expected at most 2 partition visits for y<12, got {}",
        visits
    );

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE y < 10 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    assert_eq!(
        partition_visit_count(),
        1,
        "an exclusive predicate on a RANGE boundary must not open the next partition"
    );
}

#[test]
fn contradictory_partition_filter_opens_no_heap_and_explains_empty_set() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
            PARTITION p0 VALUES LESS THAN (10), \
            PARTITION p1 VALUES LESS THAN (20), \
            PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)")
        .unwrap();

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE y < 10 AND y >= 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(rs.rows.is_empty());
    assert_eq!(
        partition_visit_count(),
        0,
        "an empty prune set must open no heap"
    );

    let QueryResponse::ResultSet(json) = engine
        .execute("EXPLAIN FORMAT=JSON SELECT id FROM t WHERE y < 10 AND y >= 20")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(json_text) = &json.rows[0][0] else {
        panic!("expected JSON text");
    };
    assert!(json_text.contains(r#""partitions": []"#), "{json_text}");
}

#[test]
fn range_partition_or_filter_prunes_union_partitions() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN (30), \
                PARTITION p3 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .unwrap();

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE y = 5 OR y = 25 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    let visits = partition_visit_count();
    assert_eq!(
        visits, 2,
        "OR pruning should visit p0 and p2 only, got {} visits",
        visits
    );
}

#[test]
fn range_partition_or_conjunct_prunes_inside_and_filter() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN (30), \
                PARTITION p3 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .unwrap();

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE (y = 5 OR y = 25) AND id > 0 ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    let visits = partition_visit_count();
    assert_eq!(
        visits, 2,
        "OR under AND should visit p0 and p2 only, got {} visits",
        visits
    );
}

#[test]
fn list_partition_is_null_filter_prunes_to_null_partition() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, kind INT) PARTITION BY LIST (kind) (\
                PARTITION p_null VALUES IN (NULL), \
                PARTITION p_one VALUES IN (1), \
                PARTITION p_two VALUES IN (2))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, NULL), (2, 1), (3, 2)")
        .unwrap();

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE kind IS NULL")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
    let visits = partition_visit_count();
    assert_eq!(
        visits, 1,
        "IS NULL pruning should visit the NULL partition only, got {} visits",
        visits
    );

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t WHERE kind <=> 1").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    assert_eq!(
        partition_visit_count(),
        1,
        "NULL-safe equality should use the same exact LIST pruning path"
    );
}

#[test]
fn explain_traditional_reports_pruned_partition_names() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("EXPLAIN SELECT * FROM t WHERE y = 15")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.columns[3].name, "partitions");
    assert_eq!(rs.rows[0][3], Value::String("p1".to_string()));
    assert_eq!(rs.rows[0][4], Value::String("ALL".to_string()));

    let QueryResponse::ResultSet(tree) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT * FROM t WHERE y = 15")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree_text) = &tree.rows[0][0] else {
        panic!("expected tree explain text");
    };
    assert!(
        tree_text.contains("partitions: p1"),
        "TREE explain should report p1, got {tree_text}"
    );

    let QueryResponse::ResultSet(json) = engine
        .execute("EXPLAIN FORMAT=JSON SELECT * FROM t WHERE y = 15")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(json_text) = &json.rows[0][0] else {
        panic!("expected json explain text");
    };
    assert!(
        json_text.contains(r#""partitions": ["p1"]"#),
        "JSON explain should report p1, got {json_text}"
    );
}

#[test]
fn explain_analyze_reports_actual_partition_visits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)")
        .unwrap();

    let QueryResponse::ResultSet(tree) = engine
        .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT * FROM t WHERE y = 15")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree_text) = &tree.rows[0][0] else {
        panic!("expected tree explain text");
    };
    assert!(
        tree_text.contains("partitions=1"),
        "TREE explain analyze should report one actual partition visit, got {tree_text}"
    );

    let QueryResponse::ResultSet(json) = engine
        .execute("EXPLAIN ANALYZE FORMAT=JSON SELECT * FROM t WHERE y = 15")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(json_text) = &json.rows[0][0] else {
        panic!("expected json explain text");
    };
    assert!(
        json_text.contains(r#""partitions": 1"#),
        "JSON explain analyze should report one actual partition visit, got {json_text}"
    );
}

#[test]
fn explain_analyze_parallel_partition_scan_counts_worker_partition_visits() {
    with_test_env(&[("MYDB_PARALLEL_DISABLE", None)], || {
        let dir = tempfile::tempdir().unwrap();
        let engine = open_partition_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                    PARTITION p0 VALUES LESS THAN (10), \
                    PARTITION p1 VALUES LESS THAN (20), \
                    PARTITION p2 VALUES LESS THAN (30), \
                    PARTITION p3 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
            .unwrap();

        let QueryResponse::ResultSet(tree) = engine
            .execute("EXPLAIN ANALYZE FORMAT=TREE SELECT /*+ PARALLEL(4) */ id FROM t WHERE y >= 0")
            .unwrap()
        else {
            panic!("expected result set");
        };
        let Value::String(tree_text) = &tree.rows[0][0] else {
            panic!("expected tree explain text");
        };
        assert!(
            tree_text.contains("Parallel table scan"),
            "PARALLEL hint should use partition-aware parallel scan, got {tree_text}"
        );
        assert!(
            tree_text.contains("partitions=4"),
            "parallel EXPLAIN ANALYZE should aggregate worker partition visits, got {tree_text}"
        );
    });
}

#[test]
fn subpartition_pruning_maps_parent_partition_to_leaf_storage_ids() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };

    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) \
             PARTITION BY RANGE (y) \
             SUBPARTITION BY HASH (id) SUBPARTITIONS 2 \
             (PARTITION p0 VALUES LESS THAN (10), \
              PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    for id in 0..20 {
        let y = if id < 10 { 5 } else { 15 };
        engine
            .execute(&format!("INSERT INTO t VALUES ({id}, {y})"))
            .unwrap();
    }

    reset_partition_visit_counter();
    let _ = engine.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(partition_visit_count(), 4);

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT COUNT(*) FROM t WHERE y = 5")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows[0][0], Value::Int(10));
    assert_eq!(
        partition_visit_count(),
        2,
        "RANGE pruning should visit p0's two leaf subpartitions"
    );

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT COUNT(*) FROM t WHERE y = 5 AND id = 4")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows[0][0], Value::Int(1));
    assert_eq!(
        partition_visit_count(),
        1,
        "parent RANGE and child HASH pruning should select one physical leaf"
    );
}

#[test]
fn key_subpartition_pruning_requires_the_complete_key_tuple() {
    use crate::engine::executor::partition_router::pruned_storage_ids_from_filter;
    use crate::parser::{BinaryOp, Expr};

    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL, region INT NOT NULL, y INT NOT NULL) \
             PARTITION BY RANGE (y) \
             SUBPARTITION BY KEY ALGORITHM=2 (id, region) SUBPARTITIONS 4 \
             (PARTITION p0 VALUES LESS THAN (10), \
              PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 2, 5), (1, 3, 5), (9, 2, 15)")
        .unwrap();

    let eq = |column: &str, value: i64| Expr::Binary {
        left: Box::new(Expr::Identifier(column.to_string())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::BoundValue(Value::Int(value))),
    };
    let and = |left: Expr, right: Expr| Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
    };
    let partial = and(eq("y", 5), eq("id", 1));
    let table = engine.database().table("test.t").unwrap();
    assert_eq!(
        pruned_storage_ids_from_filter(&table, Some(&partial))
            .unwrap()
            .len(),
        4,
        "a KEY prefix cannot select one subpartition"
    );
    let complete = and(partial, eq("region", 2));
    assert_eq!(
        pruned_storage_ids_from_filter(&table, Some(&complete))
            .unwrap()
            .len(),
        1,
        "a complete KEY tuple should select one subpartition leaf"
    );

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT COUNT(*) FROM t WHERE y = 5 AND id = 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows[0][0], Value::Int(2));

    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT COUNT(*) FROM t WHERE y = 5 AND id = 1 AND region = 2")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows[0][0], Value::Int(1));
}

#[test]
fn coalesce_partition_shrinks_hash_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    let rows_sql: Vec<String> = (0..16).map(|i| format!("({})", i)).collect();
    engine
        .execute(&format!("INSERT INTO t VALUES {}", rows_sql.join(",")))
        .unwrap();
    engine
        .execute("ALTER TABLE t COALESCE PARTITION 2")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(16));
    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = 't' ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(parts.rows.len(), 2);
}

#[test]
fn reorganize_partition_splits_range_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p_low VALUES LESS THAN (20), \
                PARTITION p_high VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)")
        .unwrap();
    engine
        .execute(
            "ALTER TABLE t REORGANIZE PARTITION p_low INTO (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20))",
        )
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, y FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 4);
    // Verify partition list is now 3 partitions.
    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = 't' ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<String> = parts
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            _ => String::new(),
        })
        .collect();
    assert_eq!(names, vec!["p0", "p1", "p_high"]);
}

#[test]
fn reorganize_parent_partition_preserves_subpartition_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL, region INT NOT NULL, PRIMARY KEY (id, region)) \
         PARTITION BY RANGE (id) SUBPARTITION BY HASH(region) SUBPARTITIONS 2 ( \
           PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    {
        let table = engine.database().table("test.t").unwrap();
        let part = table.partitioning.as_ref().unwrap();
        assert!(
            part.partitions.iter().all(|parent| parent.storage_id == 0),
            "catalog v42 logical parents must not own placeholder heaps"
        );
        assert_eq!(part.storage_ids().len(), 4);
    }
    engine
        .execute("INSERT INTO t VALUES (1,0), (2,1), (7,0), (16,1)")
        .unwrap();
    let before = engine.database().table("test.t").unwrap();
    let table_id = before.id;
    let live_rows_before =
        before.stats.row_count as i128 + engine.database().table_live_row_delta(table_id) as i128;
    let modified_before = before.stats.modified_rows_since_analyze
        + engine.database().table_modification_delta(table_id);
    let dml_inserts_before = engine.database().dml_stats_snapshot().insert_rows;
    engine
        .execute(
            "ALTER TABLE t REORGANIZE PARTITION p0 INTO ( \
           PARTITION q0 VALUES LESS THAN (5), PARTITION q1 VALUES LESS THAN (10))",
        )
        .unwrap();
    // Reorganization folds committed DML into the published statistics.
    // Check the logical total rather than requiring a stale sidecar to survive.
    let after = engine.database().table("test.t").unwrap();
    assert_eq!(
        after.stats.row_count as i128 + engine.database().table_live_row_delta(table_id) as i128,
        live_rows_before,
        "a physical shadow copy must not double-count logical live rows"
    );
    assert_eq!(
        after.stats.modified_rows_since_analyze
            + engine.database().table_modification_delta(table_id),
        modified_before,
        "a physical shadow copy must not mark the table modified again"
    );
    assert_eq!(
        engine.database().dml_stats_snapshot().insert_rows,
        dml_inserts_before,
        "a physical shadow copy must not publish user INSERT metrics"
    );

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, region FROM t ORDER BY id, region")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(7), Value::Int(0)],
            vec![Value::Int(16), Value::Int(1)],
        ]
    );
    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME, SUBPARTITION_NAME FROM information_schema.partitions \
         WHERE TABLE_NAME='t' ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        parts.rows.len(),
        6,
        "three parents must retain two leaves each"
    );
}

#[test]
fn remove_partitioning_collapses_to_single_heap() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 3")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    engine.execute("ALTER TABLE t REMOVE PARTITIONING").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(5));
    // Verify partition list is now empty for this table.
    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = 't'",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    // Unpartitioned tables emit one row with NULL partition name.
    assert_eq!(parts.rows.len(), 1);
    assert_eq!(parts.rows[0][0], Value::Null);
}

#[test]
fn remove_subpartitioning_collapses_all_physical_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL, region INT NOT NULL, PRIMARY KEY (id, region)) \
         PARTITION BY RANGE (id) SUBPARTITION BY HASH(region) SUBPARTITIONS 2 ( \
           PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,0), (2,1), (7,0), (16,1)")
        .unwrap();
    engine.execute("ALTER TABLE t REMOVE PARTITIONING").unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id, region FROM t ORDER BY id, region")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 4);
    assert_eq!(rs.rows[0], vec![Value::Int(1), Value::Int(0)]);
    assert_eq!(rs.rows[3], vec![Value::Int(16), Value::Int(1)]);
}

#[test]
fn exchange_partition_swaps_data_with_unpartitioned_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE staging (id INT, y INT)")
        .unwrap();
    engine
        .execute("INSERT INTO part_tbl VALUES (1, 5), (2, 50)")
        .unwrap();
    engine
        .execute("INSERT INTO staging VALUES (10, 7), (11, 8)")
        .unwrap();
    engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .unwrap();
    // part_tbl now has staging's old rows + the row from p1.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM part_tbl ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(10)],
            vec![Value::Int(11)]
        ]
    );
    // staging now has p0's old rows.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM staging ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn exchange_subpartition_swaps_one_leaf_and_rejects_parent() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine.execute(
        "CREATE TABLE part_tbl (id INT NOT NULL, region INT NOT NULL, PRIMARY KEY (id, region)) \
         PARTITION BY RANGE (id) SUBPARTITION BY HASH(region) SUBPARTITIONS 2 ( \
           PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)"
    ).unwrap();
    engine
        .execute(
            "CREATE TABLE staging (id INT NOT NULL, region INT NOT NULL, PRIMARY KEY (id, region))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO part_tbl VALUES (1,0), (2,1), (20,0)")
        .unwrap();
    engine.execute("INSERT INTO staging VALUES (7,0)").unwrap();

    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .expect_err("a subpartitioned parent cannot be exchanged as one heap");
    assert_eq!(
        err.code(),
        crate::error::catalog::ER_PARTITION_INSTEAD_OF_SUBPARTITION
            .code()
            .get()
    );

    engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0sp0 WITH TABLE staging")
        .unwrap();
    let QueryResponse::ResultSet(all_part) = engine
        .execute("SELECT id, region FROM part_tbl ORDER BY id, region")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        all_part.rows,
        vec![
            vec![Value::Int(2), Value::Int(1)],
            vec![Value::Int(7), Value::Int(0)],
            vec![Value::Int(20), Value::Int(0)],
        ]
    );
    let QueryResponse::ResultSet(leaf) = engine
        .execute("SELECT id, region FROM part_tbl PARTITION(p0sp0)")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(leaf.rows, vec![vec![Value::Int(7), Value::Int(0)]]);
    let QueryResponse::ResultSet(staging) =
        engine.execute("SELECT id, region FROM staging").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(staging.rows, vec![vec![Value::Int(1), Value::Int(0)]]);
}

#[test]
fn exchange_partition_with_validation_rejects_mismatched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE staging (id INT, y INT)")
        .unwrap();
    // staging row y=20 would route to p1, not p0 — must fail.
    engine
        .execute("INSERT INTO staging VALUES (1, 20)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging WITH VALIDATION")
        .unwrap_err();
    assert_eq!(err.code(), 1737);
}

// P2-D1 phase 2 — metadata-compatibility guards. MySQL 8.0 raises
// ER_TABLES_DIFFERENT_METADATA (1736) when EXCHANGE PARTITION's two
// tables disagree on column shape; mydb mirrors that across these
// dimensions:

#[test]
fn exchange_partition_rejects_column_name_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE staging (id INT, z INT)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .unwrap_err();
    assert_eq!(err.code(), 1736);
    assert!(
        err.message().contains("name mismatch"),
        "got: {}",
        err.message()
    );
}

#[test]
fn exchange_partition_rejects_column_type_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE staging (id INT, y BIGINT)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .unwrap_err();
    assert_eq!(err.code(), 1736);
    assert!(
        err.message().contains("data type"),
        "got: {}",
        err.message()
    );
}

#[test]
fn exchange_partition_rejects_index_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    // Identical columns + PK, but staging carries a secondary index the
    // partitioned table lacks. EXCHANGE swaps physical storage and pairs
    // cache invalidation / index-file renames by index name — a mismatched
    // index set would strand a stale cache entry and orphan a `.idx` file,
    // so mydb must reject with ER_TABLES_DIFFERENT_METADATA (1736).
    engine
        .execute("CREATE TABLE staging (id INT, y INT, KEY ky (y))")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .unwrap_err();
    assert_eq!(err.code(), 1736);
    assert!(
        err.message().contains("index"),
        "expected index-mismatch 1736, got: {}",
        err.message()
    );
}

#[test]
fn exchange_partition_rejects_nullability_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT NOT NULL, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE staging (id INT, y INT)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .unwrap_err();
    assert_eq!(err.code(), 1736);
    assert!(
        err.message().to_lowercase().contains("nullability") || err.message().contains("unsigned"),
        "got: {}",
        err.message()
    );
}

#[test]
fn exchange_partition_rejects_primary_key_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE part_tbl (id INT NOT NULL, y INT NOT NULL, PRIMARY KEY (id, y)) \
             PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    // staging declares (id) as PK, different shape from part_tbl's (id, y).
    // NOT NULL on both columns so the column-type / nullability check
    // doesn't fire first.
    engine
        .execute("CREATE TABLE staging (id INT NOT NULL PRIMARY KEY, y INT NOT NULL)")
        .unwrap();
    let err = engine
        .execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging")
        .unwrap_err();
    assert_eq!(err.code(), 1736);
    assert!(
        err.message().contains("primary key") || err.message().contains("PRIMARY KEY"),
        "got: {}",
        err.message()
    );
}

#[test]
fn load_data_into_partitioned_table_routes_rows_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("rows.csv");
    std::fs::write(&csv_path, "1,5\n2,15\n3,25\n4,35\n5,8\n6,18\n").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv_path)
    );
    engine.execute(&sql).unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(6));
    // y=5 and y=8 → p0 (2 rows)
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT COUNT(*) FROM t WHERE y < 10")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn load_data_partition_clause_rejects_row_outside_selected_partition() {
    let dir = tempfile::tempdir().unwrap();
    let csv_path = dir.path().join("rows.csv");
    std::fs::write(&csv_path, "1,5\n2,15\n").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();

    let sql = format!(
        "LOAD DATA INFILE '{}' INTO TABLE t PARTITION (p0) \
         FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&csv_path)
    );
    let err = engine.execute(&sql).unwrap_err();
    assert_eq!(err.code(), 1748, "unexpected error: {err:?}");
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(0));
}

#[test]
fn parallel_scan_over_partitions_returns_full_result() {
    // PARALLEL(4) on a 4-partition HASH table forces the
    // partition-aware parallel gather path. Verify the result row
    // count equals the inserted count regardless of which worker
    // handled which partition.
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT, v INT) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    let mut values = Vec::new();
    for i in 0..100 {
        values.push(format!("({},{})", i, i * 10));
    }
    engine
        .execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT /*+ PARALLEL(4) */ COUNT(*) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(100));
}

#[test]
fn partitioned_table_survives_engine_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                    PARTITION p0 VALUES LESS THAN (10), \
                    PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 5), (2, 50)")
            .unwrap();
    }
    // Reopen and verify the partition layout persisted.
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id, y FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1), Value::Int(5)],
            vec![Value::Int(2), Value::Int(50)]
        ]
    );
    let QueryResponse::ResultSet(rs) = engine
        .execute(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = 't' ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let names: Vec<String> = rs
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(names, vec!["p0", "p1"]);
}

#[test]
fn startup_validation_repairs_partition_indexes_from_lazy_headers() {
    let dir = tempfile::tempdir().unwrap();
    let paths;
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(32), INDEX v_idx (v)) \
             PARTITION BY HASH(id) PARTITIONS 2",
            )
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 'one'), (2, 'two')")
            .unwrap();
        let table = engine.db.table("test.t").unwrap();
        paths = table
            .storage_ids()
            .into_iter()
            .map(|storage_id| {
                crate::storage::naming::index_object_path_by_id(
                    dir.path(),
                    storage_id,
                    table.active_generation.max(1),
                    table
                        .indexes
                        .iter()
                        .find(|index| index.name == "v_idx")
                        .unwrap()
                        .index_id,
                    "idx",
                )
            })
            .collect::<Vec<_>>();
        engine.checkpoint().unwrap();
    }
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        let summary = engine.db.storage_status().last_recovery_summary;
        assert!(
            summary.contains("startup_validation_path=clean_fast"),
            "{summary}"
        );
        assert!(summary.contains("index_files_quick_checked=4"), "{summary}");
        assert!(summary.contains("indexes_repaired=0"), "{summary}");
        engine.checkpoint().unwrap();
    }
    std::fs::remove_file(&paths[0]).unwrap();
    std::fs::write(&paths[1], []).unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    let summary = engine.db.storage_status().last_recovery_summary;
    assert!(
        summary.contains("startup_validation_path=clean_fast"),
        "{summary}"
    );
    assert!(summary.contains("indexes_repaired=4"), "{summary}");
    for (value, id) in [("one", 1), ("two", 2)] {
        assert_eq!(
            partition_test_rows(
                &engine,
                &format!("SELECT id FROM t FORCE INDEX (v_idx) WHERE v = '{value}'")
            ),
            vec![vec![Value::Int(id)]],
        );
    }
}

#[test]
fn partitioned_table_with_index_routes_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, y INT, INDEX y_idx (y)) \
             PARTITION BY HASH(id) PARTITIONS 4",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (3, 300), (4, 400)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT y FROM t WHERE id = 2").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(200)]]);
}

#[test]
fn range_columns_multi_column_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (a INT, b INT) PARTITION BY RANGE COLUMNS (a, b) (\
                PARTITION p0 VALUES LESS THAN (5, 10), \
                PARTITION p1 VALUES LESS THAN (10, 20), \
                PARTITION p2 VALUES LESS THAN (MAXVALUE, MAXVALUE))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 5), (5, 15), (10, 30), (3, 100)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(4));
}

#[test]
fn list_columns_multi_column_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (region VARCHAR(8), kind INT) \
             PARTITION BY LIST COLUMNS (region, kind) (\
                PARTITION p_us VALUES IN (('US', 1), ('US', 2)), \
                PARTITION p_eu VALUES IN (('EU', 1), ('EU', 2)))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES ('US', 1), ('US', 2), ('EU', 1)")
        .unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES ('US', 99)")
        .unwrap_err();
    assert_eq!(err.code(), 1526);
}

#[test]
fn list_columns_runtime_hash_matches_pad_space_collation_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, label CHAR(8) COLLATE utf8mb4_general_ci) \
             PARTITION BY LIST COLUMNS(label) (\
               PARTITION p0 VALUES IN ('Cafe'), \
               PARTITION p1 VALUES IN ('Other'))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'cafe'), (2, 'Cafe   '), (3, 'OTHER')")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM t WHERE label = 'CAFE' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);

    engine
        .execute(
            "CREATE TABLE composite (id INT, label CHAR(8) COLLATE utf8mb4_general_ci, shard INT) \
             PARTITION BY LIST COLUMNS(label, shard) (\
               PARTITION p0 VALUES IN (('Cafe', 1)), \
               PARTITION p1 VALUES IN (('Other', 2)))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO composite VALUES (1, 'cafe', 1), (2, 'OTHER', 2)")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id FROM composite WHERE label = 'CAFE'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn list_runtime_is_invalidated_by_partition_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT) PARTITION BY LIST(id) (\
               PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (2))",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    engine
        .execute("ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES IN (3))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (3)").unwrap();
    engine.execute("ALTER TABLE t DROP PARTITION p0").unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES (1)")
        .expect_err("dropped LIST value must not survive in a stale runtime cache");
    assert_eq!(err.code(), 1526);
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(2)], vec![Value::Int(3)]]);
}

#[test]
fn list_columns_partial_eq_filter_prunes_matching_tuple_groups() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (region VARCHAR(8), kind INT, id INT) \
             PARTITION BY LIST COLUMNS (region, kind) (\
                PARTITION p_us VALUES IN (('US', 1), ('US', 2)), \
                PARTITION p_eu VALUES IN (('EU', 1), ('EU', 2)), \
                PARTITION p_ap VALUES IN (('AP', 1), ('AP', 2)))",
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO t VALUES \
             ('US', 1, 1), ('US', 2, 2), ('EU', 1, 3), ('AP', 2, 4)",
        )
        .unwrap();

    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE region = 'US' ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    let visits = partition_visit_count();
    assert_eq!(
        visits, 1,
        "LIST COLUMNS partial equality should visit p_us only, got {} visits",
        visits
    );
}

#[test]
fn analyze_partition_runs_table_analyze() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1), (2), (3), (4)")
        .unwrap();
    // Targeted ANALYZE visits and persists all selected physical leaves.
    engine
        .execute("ALTER TABLE t ANALYZE PARTITION ALL")
        .unwrap();
}

#[test]
fn check_partition_validates_routing_and_local_indexes_and_persists_status() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE checked_t (id INT, y INT, KEY y_idx(y)) \
             PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10), \
             PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO checked_t VALUES (1,100),(2,200),(20,300)")
        .unwrap();
    engine
        .execute("ALTER TABLE checked_t CHECK PARTITION p0")
        .unwrap();
    let table = engine.database().table("test.checked_t").unwrap();
    let part = table.partitioning.as_ref().unwrap();
    let p0 = part.storage_ids_for_name("p0").unwrap()[0];
    let p1 = part.storage_ids_for_name("p1").unwrap()[0];
    assert_eq!(table.stats.partition_stats[&p0].check_ok, Some(true));
    assert_eq!(table.stats.partition_stats[&p0].row_count, 2);
    assert!(!table.stats.partition_stats.contains_key(&p1));
}

#[test]
fn partition_wise_hash_join_requires_identical_topology_and_full_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for table in ["pwj_l", "pwj_r"] {
        engine
            .execute(&format!(
                "CREATE TABLE {table} (id INT, v INT) PARTITION BY RANGE COLUMNS(id) (\
                 PARTITION p0 VALUES LESS THAN (10),\
                 PARTITION p1 VALUES LESS THAN (MAXVALUE))"
            ))
            .unwrap();
    }
    engine
        .execute("INSERT INTO pwj_l VALUES (1,10),(2,20),(11,110),(12,120)")
        .unwrap();
    engine
        .execute("INSERT INTO pwj_r VALUES (1,1),(2,2),(12,12),(20,20)")
        .unwrap();

    let sql = "SELECT /*+ HASH_JOIN(pwj_l,pwj_r) */ pwj_l.id, pwj_l.v, pwj_r.v \
               FROM pwj_l JOIN pwj_r ON pwj_l.id=pwj_r.id ORDER BY pwj_l.id";
    let QueryResponse::ResultSet(rows) = engine.execute(sql).unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::Int(10), Value::Int(1)],
            vec![Value::Int(2), Value::Int(20), Value::Int(2)],
            vec![Value::Int(12), Value::Int(120), Value::Int(12)],
        ]
    );
    let QueryResponse::ResultSet(explain) = engine
        .execute(&format!("EXPLAIN FORMAT=TREE {sql}"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected TREE text");
    };
    assert!(tree.contains("partition_wise: true"), "{tree}");

    engine
        .execute(
            "CREATE TABLE pwj_bad (id INT, v INT) PARTITION BY RANGE COLUMNS(id) (\
             PARTITION p0 VALUES LESS THAN (5), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap();
    let QueryResponse::ResultSet(explain) = engine
        .execute(
            "EXPLAIN FORMAT=TREE SELECT /*+ HASH_JOIN(pwj_l,pwj_bad) */ * \
             FROM pwj_l JOIN pwj_bad ON pwj_l.id=pwj_bad.id",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected TREE text");
    };
    assert!(!tree.contains("partition_wise: true"), "{tree}");
}

#[test]
fn partition_wise_aggregate_concatenates_disjoint_leaf_groups() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE pwa (id INT, v INT) PARTITION BY RANGE COLUMNS(id) (\
             PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO pwa VALUES (1,10),(1,20),(11,3),(11,4),(20,8)")
        .unwrap();
    let QueryResponse::ResultSet(rows) = engine
        .execute("SELECT id, SUM(v) FROM pwa GROUP BY id ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rows.rows,
        vec![
            vec![Value::Int(1), Value::DecimalCompact(30, 0)],
            vec![Value::Int(11), Value::DecimalCompact(7, 0)],
            vec![Value::Int(20), Value::DecimalCompact(8, 0)],
        ]
    );
    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT id, SUM(v) FROM pwa GROUP BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected TREE text");
    };
    assert!(tree.contains("partition_wise: true"), "{tree}");

    let QueryResponse::ResultSet(explain) = engine
        .execute("EXPLAIN FORMAT=TREE SELECT v, SUM(id) FROM pwa GROUP BY v")
        .unwrap()
    else {
        panic!("expected result set");
    };
    let Value::String(tree) = &explain.rows[0][0] else {
        panic!("expected TREE text");
    };
    assert!(!tree.contains("partition_wise: true"), "{tree}");
}

#[test]
fn partition_maintenance_name_list_validates_targets() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();

    for op in ["CHECK", "ANALYZE", "OPTIMIZE", "REPAIR", "REBUILD"] {
        let err = engine
            .execute(&format!("ALTER TABLE t {op} PARTITION missing"))
            .unwrap_err();
        assert_eq!(err.code(), 1507, "{op}: {err:?}");
    }
}

#[test]
fn rebuild_partition_name_list_preserves_target_partition_index_access() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, v INT, KEY idx_v(v)) \
             PARTITION BY RANGE(id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 100), (2, 200), (11, 300)")
        .unwrap();

    engine
        .execute("ALTER TABLE t REBUILD PARTITION p0")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t PARTITION (p0) FORCE INDEX (idx_v) WHERE v = 200")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    let QueryResponse::ResultSet(all) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        all.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(11)]
        ]
    );
}

#[test]
fn partition_maintenance_rewrites_only_target_leaf_and_releases_governed_memory() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, v VARCHAR(128), KEY idx_v(v)) \
             PARTITION BY RANGE(id) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, REPEAT('a', 64)), (11, REPEAT('b', 64))")
        .unwrap();
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[4],
        0,
        "partition DML batch reservations must be released at statement end"
    );

    engine
        .execute("ALTER TABLE t ANALYZE PARTITION p0")
        .unwrap();
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[6],
        0,
        "partition statistics reservations must be released after ANALYZE"
    );
    let before = engine.database().table("test.t").unwrap();
    let before_ids = before.storage_ids();
    assert_eq!(before.stats.partition_stats[&before_ids[0]].row_count, 1);

    engine
        .execute("ALTER TABLE t REBUILD PARTITION p0")
        .unwrap();
    assert_eq!(
        crate::engine::executor::mem_budget::controlled_memory_snapshot().2[5],
        0,
        "partition DDL batch reservations must be released after rewrite"
    );
    let rebuilt = engine.database().table("test.t").unwrap();
    let rebuilt_ids = rebuilt.storage_ids();
    assert_ne!(rebuilt_ids[0], before_ids[0]);
    assert_eq!(rebuilt_ids[1], before_ids[1]);
    assert_eq!(rebuilt.stats.partition_stats[&rebuilt_ids[0]].row_count, 1);

    engine
        .execute("ALTER TABLE t OPTIMIZE PARTITION p1")
        .unwrap();
    let optimized = engine.database().table("test.t").unwrap();
    let optimized_ids = optimized.storage_ids();
    assert_eq!(optimized_ids[0], rebuilt_ids[0]);
    assert_ne!(optimized_ids[1], rebuilt_ids[1]);
    assert_eq!(
        optimized.stats.partition_stats[&optimized_ids[1]].row_count,
        1
    );
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(1)], vec![Value::Int(11)]]);
}

#[test]
fn drop_partitioned_table_removes_all_partition_files() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(id) PARTITIONS 3")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    engine.execute("DROP TABLE t").unwrap();
    // Recreating the same name must work (no leftover catalog row).
    engine.execute("CREATE TABLE t (id INT, y INT)").unwrap();
}

#[test]
fn mysql80_partition_unique_key_must_include_partition_column_on_create() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    let err = engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, y INT NOT NULL) \
             PARTITION BY HASH(y) PARTITIONS 2",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1503, "{}", err.message());
    // §2.7: PRIMARY KEY wording differs from a plain UNIQUE index.
    assert!(
        err.message().contains("PRIMARY KEY"),
        "expected PRIMARY KEY wording, got: {}",
        err.message()
    );

    engine
        .execute(
            "CREATE TABLE ok_t (id INT NOT NULL, y INT NOT NULL, PRIMARY KEY(id, y)) \
             PARTITION BY HASH(y) PARTITIONS 2",
        )
        .unwrap();
}

#[test]
fn mysql80_partition_unique_key_must_include_partition_column_on_add_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL, y INT NOT NULL) PARTITION BY HASH(y) PARTITIONS 2",
        )
        .unwrap();
    let err = engine
        .execute("CREATE UNIQUE INDEX ux_id ON t(id)")
        .unwrap_err();
    assert_eq!(err.code(), 1503, "{}", err.message());
    assert!(
        err.message().contains("UNIQUE INDEX"),
        "expected UNIQUE INDEX wording, got: {}",
        err.message()
    );

    engine
        .execute("CREATE UNIQUE INDEX ux_id_y ON t(id, y)")
        .unwrap();
}

#[test]
fn mysql80_partition_range_bounds_must_be_strictly_increasing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    let err = engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE(y) (\
                PARTITION p0 VALUES LESS THAN (20), \
                PARTITION p1 VALUES LESS THAN (10))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1493, "{}", err.message());
}

#[test]
fn mysql80_partition_list_values_must_not_repeat() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    let err = engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY LIST(y) (\
                PARTITION p0 VALUES IN (1, 2), \
                PARTITION p1 VALUES IN (2, 3))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1495, "{}", err.message());
}

#[test]
fn mysql80_partitioned_temporary_table_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    let err = engine
        .execute("CREATE TEMPORARY TABLE t (id INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap_err();
    assert_eq!(err.code(), 1562, "{}", err.message());
}

#[test]
fn partitioned_fulltext_and_hnsw_are_enabled_extensions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE docs (id INT, y INT, body TEXT, FULLTEXT KEY ft_body(body)) \
         PARTITION BY HASH(y) PARTITIONS 2",
        )
        .unwrap();
    engine
        .execute("INSERT INTO docs VALUES (1, 0, 'partition search')")
        .unwrap();

    engine
        .execute(
            "CREATE TABLE items (id INT, bucket INT, emb VECTOR(3) NOT NULL) \
             PARTITION BY HASH(bucket) PARTITIONS 2",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX idx_emb ON items(emb) USING HNSW")
        .unwrap();
}

#[test]
fn alter_table_partition_by_repartitions_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine.execute("CREATE TABLE t (id INT, y INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    engine
        .execute("ALTER TABLE t PARTITION BY HASH(y) PARTITIONS 4")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*), SUM(y) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(4));
    assert_eq!(crate::storage::value_to_string(&rs.rows[0][1]), "100");

    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = 't' ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(parts.rows.len(), 4);
}

#[test]
fn add_partition_partitions_grows_hash_partitioned_table() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(y) PARTITIONS 2")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD PARTITION PARTITIONS 2")
        .unwrap();

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*), SUM(y) FROM t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(4));
    assert_eq!(crate::storage::value_to_string(&rs.rows[0][1]), "100");

    let QueryResponse::ResultSet(parts) = engine
        .execute(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = 't' ORDER BY PARTITION_ORDINAL_POSITION",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(parts.rows.len(), 4);
}

// ---------------------------------------------------------------------------
// M1: partition column / expression type validation (§2.2).
// ---------------------------------------------------------------------------

/// Restores an env var when dropped so the strict-types escape hatch does
/// not leak into other tests sharing the process (belt-and-braces on top of
/// nextest's process-per-test isolation).
struct PartEnvRestore {
    key: &'static str,
    prior: Option<String>,
}

impl Drop for PartEnvRestore {
    fn drop(&mut self) {
        match self.prior.take() {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

fn set_part_env(key: &'static str, value: &str) -> PartEnvRestore {
    let prior = std::env::var(key).ok();
    std::env::set_var(key, value);
    PartEnvRestore { key, prior }
}

#[test]
fn mysql80_hash_on_non_integer_column_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // Bare VARCHAR column under HASH → 1659 ER_FIELD_TYPE_NOT_ALLOWED.
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, name VARCHAR(32) NOT NULL) PARTITION BY HASH(name) PARTITIONS 2",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1659, "{}", err.message());
    assert!(err.message().contains("name"), "{}", err.message());
}

#[test]
fn mysql80_range_on_non_integer_bare_column_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // A bare DATE column produces a non-integer result → 1659; MySQL
    // requires wrapping it in TO_DAYS()/YEAR() etc.
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, d DATE NOT NULL) PARTITION BY RANGE(d) (\
                PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1659, "{}", err.message());
}

#[test]
fn mysql80_range_year_function_over_date_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // Whitelisted YEAR() over a DATE column → integer result, accepted.
    engine
        .execute(
            "CREATE TABLE t (id INT, d DATE NOT NULL) PARTITION BY RANGE(YEAR(d)) (\
                PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, '2019-05-01'), (2, '2023-01-01')")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn mysql80_non_whitelisted_partition_function_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // ASCII() is not in MySQL's partition function whitelist → 1564.
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, name VARCHAR(32) NOT NULL) \
             PARTITION BY HASH(ASCII(name)) PARTITIONS 2",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1564, "{}", err.message());
}

#[test]
fn mysql80_partition_expression_allows_int_div_but_rejects_slash_and_bits() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE div_ok (id INT) PARTITION BY HASH(id DIV 4) PARTITIONS 4")
        .unwrap();
    let table = engine.database().table("test.div_ok").unwrap();
    assert!(matches!(
        table
            .partitioning
            .as_ref()
            .and_then(|partitioning| partitioning.expr_ast.as_ref()),
        Some(crate::parser::Expr::Binary {
            op: crate::parser::BinaryOp::IntDiv,
            ..
        })
    ));
    engine
        .execute("INSERT INTO div_ok VALUES (1), (4), (8), (15)")
        .unwrap();
    let QueryResponse::ResultSet(rows) =
        engine.execute("SELECT id FROM div_ok ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows.len(), 4);

    for (table, expr) in [
        ("slash_bad", "id / 4"),
        ("and_bad", "id & 3"),
        ("or_bad", "id | 3"),
        ("xor_bad", "id ^ 3"),
        ("shift_bad", "id << 1"),
        ("not_bad", "~id"),
    ] {
        let err = engine
            .execute(&format!(
                "CREATE TABLE {table} (id INT) PARTITION BY HASH({expr}) PARTITIONS 4"
            ))
            .expect_err("disallowed partition operator must fail before catalog mutation");
        assert!(
            matches!(err.code(), 1486 | 1491 | 1564 | 1697),
            "unexpected errno for {expr}: {err:?}"
        );
        assert!(engine.database().table(&format!("test.{table}")).is_err());
    }
}

#[test]
fn analyze_partition_persists_only_target_leaf_then_merges_totals() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE leaf_stats (id INT, y INT) PARTITION BY RANGE(y) (\
                 PARTITION p0 VALUES LESS THAN (10),\
                 PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO leaf_stats VALUES (1,1),(2,2),(3,11),(4,12)")
            .unwrap();
        let table = engine.database().table("test.leaf_stats").unwrap();
        let partitioning = table.partitioning.as_ref().unwrap();
        let p0 = partitioning.storage_ids_for_name("p0").unwrap()[0];
        let p1 = partitioning.storage_ids_for_name("p1").unwrap()[0];

        engine
            .execute("ALTER TABLE leaf_stats ANALYZE PARTITION p0")
            .unwrap();
        let table = engine.database().table("test.leaf_stats").unwrap();
        assert_eq!(table.stats.partition_stats.len(), 1);
        assert_eq!(table.stats.partition_stats[&p0].row_count, 2);
        assert!(!table.stats.partition_stats.contains_key(&p1));

        engine
            .execute("ALTER TABLE leaf_stats ANALYZE PARTITION p1")
            .unwrap();
        let table = engine.database().table("test.leaf_stats").unwrap();
        assert_eq!(table.stats.partition_stats.len(), 2);
        assert_eq!(table.stats.partition_stats[&p1].row_count, 2);
        assert_eq!(table.stats.row_count, 4);
    }
    let engine = open_partition_recovery_test_engine(dir.path());
    let table = engine.database().table("test.leaf_stats").unwrap();
    assert_eq!(table.stats.partition_stats.len(), 2);
    assert_eq!(table.stats.row_count, 4);
}

#[test]
fn partition_analyze_reconciles_live_delta_and_collects_joint_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ps_fix (id INT, a INT, b INT, KEY MyIdx(a,b)) \
         PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO ps_fix VALUES (1,1,1),(2,2,2),(101,1,1)")
        .unwrap();
    engine.execute("ANALYZE TABLE ps_fix").unwrap();
    engine
        .execute("INSERT INTO ps_fix VALUES (102,2,2)")
        .unwrap();
    engine
        .execute("ANALYZE TABLE ps_fix PARTITION (p0,p1)")
        .unwrap();
    let table = engine.database().table("test.ps_fix").unwrap();
    assert_eq!(table.stats.row_count, 4);
    assert_eq!(engine.database().table_live_row_delta(table.id), 0);
    assert_eq!(engine.database().table_modification_delta(table.id), 0);
    assert!(!table.stats.multi_column_stats.is_empty());
    assert!(table.stats.indexes["myidx"].leaf_page_count.is_some());
    assert_eq!(partition_test_rows(&engine,
        "SELECT TABLE_ROWS, MODIFIED_ROWS FROM information_schema.mydb_stats_health WHERE TABLE_NAME='ps_fix'"),
        vec![vec![Value::Int(4), Value::Int(0)]]);
    engine
        .execute("INSERT INTO ps_fix VALUES (3,3,3),(103,3,3)")
        .unwrap();
    engine
        .execute("ANALYZE TABLE ps_fix PARTITION (p0)")
        .unwrap();
    let table = engine.database().table("test.ps_fix").unwrap();
    assert_eq!(table.stats.row_count, 5);
    assert_eq!(engine.database().table_live_row_delta(table.id), 1);
    assert_eq!(partition_test_rows(&engine,
        "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA='test' AND TABLE_NAME='ps_fix'"),
        vec![vec![Value::Int(6)]]);
}

#[test]
fn partition_statistics_follow_drop_truncate_and_exchange_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE ps (id INT, y INT) PARTITION BY RANGE(y) (\
             PARTITION p0 VALUES LESS THAN (10),\
             PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE stage (id INT, y INT)")
        .unwrap();
    engine
        .execute("INSERT INTO ps VALUES (1,1),(2,2),(3,20)")
        .unwrap();
    engine
        .execute("INSERT INTO stage VALUES (10,5),(11,6),(12,7)")
        .unwrap();
    engine.execute("ANALYZE TABLE ps").unwrap();
    engine.execute("ANALYZE TABLE stage").unwrap();

    let before = engine.database().table("test.ps").unwrap();
    let p0_old = before
        .partitioning
        .as_ref()
        .unwrap()
        .storage_ids_for_name("p0")
        .unwrap()[0];
    assert_eq!(before.stats.partition_stats[&p0_old].row_count, 2);

    engine
        .execute("ALTER TABLE ps EXCHANGE PARTITION p0 WITH TABLE stage")
        .unwrap();
    let exchanged = engine.database().table("test.ps").unwrap();
    let p0_new = exchanged
        .partitioning
        .as_ref()
        .unwrap()
        .storage_ids_for_name("p0")
        .unwrap()[0];
    assert_ne!(p0_new, p0_old);
    assert_eq!(exchanged.stats.partition_stats[&p0_new].row_count, 3);
    assert_eq!(exchanged.stats.row_count, 4);
    let stage = engine.database().table("test.stage").unwrap();
    assert_eq!(stage.stats.row_count, 2);
    assert!(stage.stats.partition_stats.is_empty());

    engine
        .execute("ALTER TABLE ps TRUNCATE PARTITION p0")
        .unwrap();
    let truncated = engine.database().table("test.ps").unwrap();
    let p0_truncated = truncated
        .partitioning
        .as_ref()
        .unwrap()
        .storage_ids_for_name("p0")
        .unwrap()[0];
    assert_ne!(p0_truncated, p0_new);
    assert!(!truncated.stats.partition_stats.contains_key(&p0_new));
    assert_eq!(truncated.stats.partition_stats[&p0_truncated].row_count, 0);
    assert_eq!(truncated.stats.row_count, 1);

    engine.execute("ALTER TABLE ps DROP PARTITION p0").unwrap();
    let dropped = engine.database().table("test.ps").unwrap();
    assert!(!dropped.stats.partition_stats.contains_key(&p0_new));
    assert_eq!(dropped.stats.row_count, 1);
}

#[test]
fn streaming_repartition_rebinds_leaf_statistics_to_shadow_generation() {
    let dir = tempfile::tempdir().unwrap();
    let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE stats_reorg (id INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine
        .execute("INSERT INTO stats_reorg VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    engine.execute("ANALYZE TABLE stats_reorg").unwrap();
    let old_ids = engine
        .database()
        .table("test.stats_reorg")
        .unwrap()
        .storage_ids();
    engine
        .execute("ALTER TABLE stats_reorg ADD PARTITION PARTITIONS 2")
        .unwrap();
    let table = engine.database().table("test.stats_reorg").unwrap();
    let new_ids = table.storage_ids();
    assert!(old_ids.iter().all(|id| !new_ids.contains(id)));
    assert_eq!(table.stats.partition_stats.len(), 4);
    assert_eq!(
        table
            .stats
            .partition_stats
            .values()
            .map(|stats| stats.row_count)
            .sum::<u64>(),
        5
    );
    assert_eq!(table.stats.row_count, 5);

    engine
        .execute("ALTER TABLE stats_reorg REMOVE PARTITIONING")
        .unwrap();
    let table = engine.database().table("test.stats_reorg").unwrap();
    assert!(table.stats.partition_stats.is_empty());
    assert_eq!(table.stats.row_count, 5);
}

#[test]
fn mysql80_range_columns_rejects_float_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // FLOAT/DOUBLE/DECIMAL are not allowed as RANGE COLUMNS columns → 1659.
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, f DOUBLE NOT NULL) PARTITION BY RANGE COLUMNS(f) (\
                PARTITION p0 VALUES LESS THAN (1), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1659, "{}", err.message());
}

#[test]
fn mysql80_range_columns_allows_varchar_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // CHAR/VARCHAR are allowed under COLUMNS.
    engine
        .execute(
            "CREATE TABLE t (id INT, region VARCHAR(8) NOT NULL) PARTITION BY RANGE COLUMNS(region) (\
                PARTITION p0 VALUES LESS THAN ('m'), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1, 'apac'), (2, 'zulu')")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(2));
}

#[test]
fn mysql80_key_partition_rejects_blob_and_json_but_allows_others() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // JSON → 1659 in a KEY partition function.
    let err = engine
        .execute(
            "CREATE TABLE t2 (id INT NOT NULL, doc JSON NOT NULL) PARTITION BY KEY(doc) PARTITIONS 2",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1659, "{}", err.message());

    // DECIMAL is allowed for KEY (unlike COLUMNS).
    engine
        .execute(
            "CREATE TABLE t3 (id INT NOT NULL, amt DECIMAL(10,2) NOT NULL) \
             PARTITION BY KEY(amt) PARTITIONS 2",
        )
        .unwrap();
}

#[test]
fn mysql80_range_non_integer_values_literal_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // Integer expression but a string VALUES literal → 1697.
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, y INT NOT NULL) PARTITION BY RANGE(y) (\
                PARTITION p0 VALUES LESS THAN ('abc'), PARTITION p1 VALUES LESS THAN (MAXVALUE))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1697, "{}", err.message());
}

#[test]
fn mysql80_partition_strict_types_cannot_be_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    // A process-local setting must not be able to change persisted routing.
    let _guard = set_part_env("MYDB_PARTITION_STRICT_TYPES", "0");
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, name VARCHAR(32) NOT NULL) PARTITION BY HASH(name) PARTITIONS 2",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1659, "{}", err.message());
}

// ---------------------------------------------------------------------------
// M2: crash-safe streaming repartition (§2.1).
// ---------------------------------------------------------------------------

/// Sorted (id, y) pairs of a two-column table, for differential compares.
fn collect_id_y(engine: &StorageQueryEngine, table: &str) -> Vec<(i64, i64)> {
    let QueryResponse::ResultSet(rs) = engine
        .execute(&format!("SELECT id, y FROM {table} ORDER BY id"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    rs.rows
        .iter()
        .map(|r| {
            let id = match &r[0] {
                Value::Int(v) => *v,
                other => panic!("unexpected id {other:?}"),
            };
            let y = match &r[1] {
                Value::Int(v) => *v,
                other => panic!("unexpected y {other:?}"),
            };
            (id, y)
        })
        .collect()
}

fn partition_names(engine: &StorageQueryEngine, table: &str) -> Vec<String> {
    let QueryResponse::ResultSet(rs) = engine
        .execute(&format!(
            "SELECT PARTITION_NAME FROM information_schema.partitions \
             WHERE TABLE_NAME = '{table}' ORDER BY PARTITION_ORDINAL_POSITION"
        ))
        .unwrap()
    else {
        panic!("expected result set");
    };
    rs.rows
        .iter()
        .filter_map(|r| match &r[0] {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

/// Streaming (default) and legacy full-memory paths must produce
/// identical data after a COALESCE.
#[test]
fn partition_reorg_streaming_matches_legacy_coalesce() {
    let run = |streaming: bool| -> Vec<(i64, i64)> {
        let dir = tempfile::tempdir().unwrap();
        let _g = set_part_env(
            "MYDB_PARTITION_REORG_STREAMING",
            if streaming { "1" } else { "0" },
        );
        let engine = open_partition_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(y) PARTITIONS 6")
            .unwrap();
        let rows: Vec<String> = (0..64).map(|i| format!("({i}, {})", i * 7 % 97)).collect();
        engine
            .execute(&format!("INSERT INTO t VALUES {}", rows.join(", ")))
            .unwrap();
        engine
            .execute("ALTER TABLE t COALESCE PARTITION 2")
            .unwrap();
        collect_id_y(&engine, "t")
    };
    let streamed = run(true);
    let legacy = run(false);
    assert_eq!(streamed.len(), 64);
    assert_eq!(streamed, legacy);
}

/// The streaming path holds only O(batch) rows in memory at once, never
/// the whole table.
#[test]
fn partition_reorg_streaming_bounds_peak_batch_memory() {
    let dir = tempfile::tempdir().unwrap();
    let _batch = set_part_env("MYDB_PARTITION_REORG_BATCH_PAGES", "1");
    let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(y) PARTITIONS 2")
        .unwrap();
    // Enough rows to span many heap pages so a 1-page batch is a small
    // fraction of the total.
    let total = 1000;
    let rows: Vec<String> = (0..total).map(|i| format!("({i}, {i})")).collect();
    engine
        .execute(&format!("INSERT INTO t VALUES {}", rows.join(", ")))
        .unwrap();
    engine
        .execute("ALTER TABLE t ADD PARTITION PARTITIONS 2")
        .unwrap();

    let peak = crate::storage::reorg_peak_batch_rows();
    assert!(peak > 0, "streaming path should have recorded batches");
    assert!(
        (peak as i64) < total / 2,
        "peak batch {peak} must be far below the {total}-row table"
    );

    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(total));
}

/// A crash *before* the catalog swap must roll back: the source data is
/// intact and the partitioning is unchanged.
#[test]
fn partition_reorg_crash_before_swap_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Crash injection only fires on the streaming path; pin it on
        // so this test is correct regardless of the ambient flag.
        let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(y) PARTITIONS 4")
            .unwrap();
        let rows: Vec<String> = (0..100).map(|i| format!("({i}, {})", i * 3)).collect();
        engine
            .execute(&format!("INSERT INTO t VALUES {}", rows.join(", ")))
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "before_swap");
            // Simulated crash mid-repartition (returns before the swap).
            let _ = engine.execute("ALTER TABLE t ADD PARTITION PARTITIONS 4");
        }
        drop(engine);
    }
    // Reopen — recovery must roll the reorg back.
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(100), "no rows may be lost");
    // Partitioning unchanged: still 4 partitions.
    assert_eq!(partition_names(&engine, "t").len(), 4);
}

#[test]
fn partition_ddl_job_is_persistent_observable_and_cancellable_before_swap() {
    let dir = tempfile::tempdir().unwrap();
    {
        let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute("CREATE TABLE job_t (id INT) PARTITION BY HASH(id) PARTITIONS 2")
            .unwrap();
        engine
            .execute("INSERT INTO job_t VALUES (1),(2),(3),(4)")
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "before_swap");
            engine
                .execute("ALTER TABLE job_t ADD PARTITION PARTITIONS 2, LOCK=SHARED")
                .unwrap();
        }
        let QueryResponse::ResultSet(jobs) = engine
            .execute(
                "SELECT JOB_ID, STATE FROM information_schema.mydb_ddl_jobs \
                 WHERE TABLE_NAME='job_t'",
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(jobs.rows.len(), 1);
        let Value::Int(job_id) = jobs.rows[0][0] else {
            panic!("expected job id");
        };
        assert_eq!(jobs.rows[0][1], Value::String("CATCH_UP".to_string()));
        engine.execute(&format!("CANCEL DDL JOB {job_id}")).unwrap();
        let QueryResponse::ResultSet(jobs) = engine
            .execute(
                "SELECT STATE FROM information_schema.mydb_ddl_jobs \
                 WHERE TABLE_NAME='job_t'",
            )
            .unwrap()
        else {
            panic!("expected result set");
        };
        assert_eq!(
            jobs.rows[0][0],
            Value::String("CANCEL_REQUESTED".to_string())
        );
    }
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rows) = engine.execute("SELECT COUNT(*) FROM job_t").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rows.rows[0][0], Value::Int(4));
    assert_eq!(partition_names(&engine, "job_t").len(), 2);
    let QueryResponse::ResultSet(jobs) = engine
        .execute("SELECT * FROM information_schema.mydb_ddl_jobs WHERE TABLE_NAME='job_t'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(jobs.rows.is_empty());
}

/// A crash *after* the catalog swap must roll forward: the new layout is
/// live, the data is complete, and the source files are cleaned up.
#[test]
fn partition_reorg_crash_after_swap_rolls_forward() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Crash injection only fires on the streaming path; pin it on
        // so this test is correct regardless of the ambient flag.
        let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(y) PARTITIONS 4")
            .unwrap();
        let rows: Vec<String> = (0..100).map(|i| format!("({i}, {})", i * 3)).collect();
        engine
            .execute(&format!("INSERT INTO t VALUES {}", rows.join(", ")))
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "after_swap");
            let _ = engine.execute("ALTER TABLE t ADD PARTITION PARTITIONS 4");
        }
        drop(engine);
    }
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(100), "no rows may be lost");
    // New layout is live: 8 partitions.
    assert_eq!(partition_names(&engine, "t").len(), 8);
    // A follow-up write still routes correctly.
    engine.execute("INSERT INTO t VALUES (1000, 7)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(101));
}

/// REMOVE PARTITIONING is crash-safe under streaming too.
#[test]
fn partition_reorg_remove_partitioning_crash_before_swap_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Crash injection only fires on the streaming path; pin it on
        // so this test is correct regardless of the ambient flag.
        let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE(y) (\
                    PARTITION p0 VALUES LESS THAN (100), \
                    PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 150), (3, 90), (4, 200)")
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "before_swap");
            let _ = engine.execute("ALTER TABLE t REMOVE PARTITIONING");
        }
        drop(engine);
    }
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0], Value::Int(4));
    // Still partitioned (rollback).
    assert_eq!(partition_names(&engine, "t").len(), 2);
}

/// Recovery must use physical subpartition leaves as the source generation
/// when REMOVE PARTITIONING has already swapped the catalog. Logical parent
/// storage ids are empty placeholders and must never be copied or restored.
#[test]
fn remove_subpartitioning_crash_after_swap_rolls_forward_without_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    {
        let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t (id INT, y INT) \
                 PARTITION BY RANGE(y) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (\
                   PARTITION p0 VALUES LESS THAN (100), \
                   PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 110), (4, 120)")
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "after_swap");
            let _ = engine.execute("ALTER TABLE t REMOVE PARTITIONING");
        }
        drop(engine);
    }

    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)],
        ]
    );
    assert!(
        engine
            .database()
            .table("test.t")
            .unwrap()
            .partitioning
            .is_none(),
        "post-swap recovery must retain the unpartitioned catalog"
    );
    engine.execute("INSERT INTO t VALUES (5, 500)").unwrap();
}

/// A parent REORGANIZE on a subpartitioned table rebuilds and journals every
/// physical leaf below that parent. Recovery after the catalog swap must not
/// mistake the empty parent storage id for the source generation.
#[test]
fn reorganize_subpartition_parent_crash_after_swap_preserves_all_rows() {
    let dir = tempfile::tempdir().unwrap();
    {
        let _stream = set_part_env("MYDB_PARTITION_REORG_STREAMING", "1");
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE t (id INT, y INT) \
                 PARTITION BY RANGE(y) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (\
                   PARTITION p0 VALUES LESS THAN (100), \
                   PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("INSERT INTO t VALUES (1, 10), (2, 60), (3, 110), (4, 120)")
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "after_swap");
            let _ = engine.execute(
                "ALTER TABLE t REORGANIZE PARTITION p0 INTO (\
                   PARTITION p00 VALUES LESS THAN (50), \
                   PARTITION p01 VALUES LESS THAN (100))",
            );
        }
        drop(engine);
    }

    let engine = open_partition_recovery_test_engine(dir.path());
    assert_eq!(
        collect_id_y(&engine, "t"),
        vec![(1, 10), (2, 60), (3, 110), (4, 120)]
    );
    let table = engine.database().table("test.t").unwrap();
    let part = table.partitioning.as_ref().unwrap();
    assert_eq!(
        part.partitions
            .iter()
            .map(|parent| parent.name.as_str())
            .collect::<Vec<_>>(),
        vec!["p00", "p01", "p1"]
    );
    assert!(part
        .partitions
        .iter()
        .all(|parent| parent.subpartitions.len() == 2));
}

/// EXCHANGE PARTITION: a crash after the committed catalog swap but before
/// the file renames finish must be completed by recovery (§2.1 EXCHANGE
/// hardening), leaving both tables' content correctly swapped.
#[test]
fn exchange_partition_crash_after_swap_is_completed_by_recovery() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE part_tbl (id INT, y INT) PARTITION BY RANGE (y) (\
                    PARTITION p0 VALUES LESS THAN (10), \
                    PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("CREATE TABLE staging (id INT, y INT)")
            .unwrap();
        engine
            .execute("INSERT INTO part_tbl VALUES (1, 5), (2, 50)")
            .unwrap();
        engine
            .execute("INSERT INTO staging VALUES (10, 7), (11, 8)")
            .unwrap();
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "exchange_after_swap");
            let _ = engine.execute("ALTER TABLE part_tbl EXCHANGE PARTITION p0 WITH TABLE staging");
        }
        drop(engine);
    }
    // Reopen — recovery completes the pending renames.
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM part_tbl ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows,
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(10)],
            vec![Value::Int(11)]
        ]
    );
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM staging ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

/// EXCHANGE recovery records the concrete subpartition ordinal. Replaying an
/// exchange must swap that leaf, not the logical parent placeholder.
#[test]
fn exchange_subpartition_crash_after_swap_recovers_exact_leaf() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE part_tbl (id INT, y INT) \
                 PARTITION BY RANGE(y) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (\
                   PARTITION p0 VALUES LESS THAN (10), \
                   PARTITION p1 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        engine
            .execute("CREATE TABLE staging (id INT, y INT)")
            .unwrap();
        engine
            .execute("INSERT INTO part_tbl VALUES (1, 5), (2, 5), (3, 50)")
            .unwrap();
        engine
            .execute("INSERT INTO staging VALUES (11, 5)")
            .unwrap();

        let table = engine.database().table("test.part_tbl").unwrap();
        let partitioning = table.partitioning.as_ref().unwrap();
        let p0 = &partitioning.partitions[0];
        let target_name = p0
            .subpartitions
            .iter()
            .find(|leaf| {
                crate::engine::executor::partition_router::route_row(
                    &table,
                    partitioning,
                    &[Value::Int(11), Value::Int(5)],
                )
                .is_ok_and(|routed| routed.storage_id == leaf.storage_id)
            })
            .unwrap()
            .name
            .clone();
        drop(table);
        {
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "exchange_after_swap");
            let _ = engine.execute(&format!(
                "ALTER TABLE part_tbl EXCHANGE PARTITION {target_name} WITH TABLE staging"
            ));
        }
        drop(engine);
    }

    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM part_tbl ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert!(rs.rows.contains(&vec![Value::Int(11)]));
    assert!(!rs.rows.contains(&vec![Value::Int(1)]) || !rs.rows.contains(&vec![Value::Int(2)]));
    let QueryResponse::ResultSet(staging) = engine.execute("SELECT id FROM staging").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(staging.rows.len(), 1, "only the selected leaf is exchanged");
}

// ---------------------------------------------------------------------------
// M3: per-partition physical options persistence + echo (§2.3).
// ---------------------------------------------------------------------------

fn show_create(engine: &StorageQueryEngine, table: &str) -> String {
    let QueryResponse::ResultSet(rs) = engine
        .execute(&format!("SHOW CREATE TABLE {table}"))
        .unwrap()
    else {
        panic!("expected result set");
    };
    match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected value {:?}", other),
    }
}

#[test]
fn partition_unsupported_physical_options_are_rejected_before_create() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (name, option) in [
        ("data_dir", "DATA DIRECTORY = '/var/lib/p0'"),
        ("index_dir", "INDEX DIRECTORY = '/var/lib/p0'"),
        ("tablespace", "TABLESPACE = ts1"),
        ("max_rows", "MAX_ROWS = 1000"),
        ("min_rows", "MIN_ROWS = 1"),
        ("nodegroup", "NODEGROUP = 2"),
    ] {
        let sql = format!(
            "CREATE TABLE {name} (id INT, y INT) PARTITION BY RANGE (y) (\
            PARTITION p0 VALUES LESS THAN (10) {option}, \
            PARTITION p1 VALUES LESS THAN MAXVALUE)"
        );
        let err = engine.execute(&sql).unwrap_err();
        assert_eq!(err.code(), 1235, "{option}: {}", err.message());
        assert!(engine.database().table(&format!("test.{name}")).is_err());
    }
    engine
        .execute(
            "CREATE TABLE allowed_engine (id INT, y INT) PARTITION BY HASH(y) \
         (PARTITION p0 ENGINE=InnoDB)",
        )
        .unwrap();
    let show = show_create(&engine, "allowed_engine");
    assert!(!show.contains("PARTITION p0 ENGINE"), "{show}");
    let err = engine
        .execute(
            "CREATE TABLE bad_engine (id INT, y INT) PARTITION BY HASH(y) \
         (PARTITION p0 ENGINE=RocksDB)",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1286, "{}", err.message());
}

#[test]
fn partition_options_require_values_and_reject_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (suffix, option) in [("ts", "TABLESPACE"), ("max", "MAX_ROWS"), ("eng", "ENGINE")] {
        let sql = format!(
            "CREATE TABLE t_{suffix} (id INT) PARTITION BY HASH(id) \
            (PARTITION p0 {option})"
        );
        assert!(engine.execute(&sql).is_err(), "{option} needs a value");
    }
    assert!(engine
        .execute(
            "CREATE TABLE t_overflow (id INT) PARTITION BY HASH(id) \
         (PARTITION p0 NODEGROUP=4294967296)"
        )
        .is_err());
}

#[test]
fn physical_partition_limit_is_checked_before_catalog_id_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    let before = engine.database().catalog().next_table_id;
    let err = engine
        .execute("CREATE TABLE too_many (id INT) PARTITION BY HASH(id) PARTITIONS 8193")
        .expect_err("8193 physical leaves must exceed the hard limit");
    assert_eq!(err.code(), 1499, "unexpected error: {err:?}");
    assert_eq!(
        engine.database().catalog().next_table_id,
        before,
        "failed validation must not consume table/storage ids"
    );

    let err = engine
        .execute(
            "CREATE TABLE too_many_sub (id INT) \
             PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 4097 (\
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .expect_err("2*4097 physical leaves must exceed the hard limit");
    assert_eq!(err.code(), 1499, "unexpected error: {err:?}");
    assert_eq!(engine.database().catalog().next_table_id, before);
}

#[test]
fn unsupported_subpartition_options_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    let err = engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) \
         SUBPARTITION BY HASH(id) SUBPARTITIONS 1 (\
         PARTITION p0 VALUES LESS THAN MAXVALUE \
         (SUBPARTITION sp0 TABLESPACE=ts1))",
        )
        .unwrap_err();
    assert_eq!(err.code(), 1235, "{}", err.message());
}

// ---------------------------------------------------------------------------
// M4: RANGE COLUMNS prefix pruning + NULL routing semantics (§2.5).
// ---------------------------------------------------------------------------

#[test]
fn range_columns_prefix_range_prunes_leading_column() {
    use crate::engine::executor::partition_router::{
        partition_visit_count, reset_partition_visit_counter,
    };
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (a INT, b INT) PARTITION BY RANGE COLUMNS(a, b) (\
                PARTITION p0 VALUES LESS THAN (10, 100), \
                PARTITION p1 VALUES LESS THAN (20, 200), \
                PARTITION p2 VALUES LESS THAN (30, 300), \
                PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE))",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (5, 5), (15, 15), (25, 25), (35, 35)")
        .unwrap();
    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a FROM t WHERE a < 12 ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(5)]]);
    let visits = partition_visit_count();
    assert!(
        visits <= 2,
        "RANGE COLUMNS prefix a<12 should visit <= 2 partitions, got {visits}"
    );

    // A lower-bound prefix range prunes from the left.
    reset_partition_visit_counter();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT a FROM t WHERE a >= 22 ORDER BY a")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(25)], vec![Value::Int(35)]]);
    let visits = partition_visit_count();
    assert!(
        visits <= 2,
        "RANGE COLUMNS prefix a>=22 should visit <= 2 partitions, got {visits}"
    );
}

#[test]
fn range_partition_routes_null_to_leftmost_partition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    // MySQL: RANGE treats NULL as less than any value → leftmost partition.
    engine.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t PARTITION (p0)").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn list_partition_null_requires_explicit_values_in_null() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    // Without VALUES IN (NULL), a NULL has no partition → 1526.
    engine
        .execute(
            "CREATE TABLE t (id INT, k INT) PARTITION BY LIST (k) (\
                PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3, 4))",
        )
        .unwrap();
    let err = engine
        .execute("INSERT INTO t VALUES (1, NULL)")
        .unwrap_err();
    assert_eq!(err.code(), 1526, "{}", err.message());

    // With an explicit NULL bucket, the NULL routes there.
    engine
        .execute(
            "CREATE TABLE t2 (id INT, k INT) PARTITION BY LIST (k) (\
                PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3, NULL))",
        )
        .unwrap();
    engine.execute("INSERT INTO t2 VALUES (7, NULL)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t2 PARTITION (p1)").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(7)]]);
}

#[test]
fn hash_partition_routes_null_as_zero() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT, y INT) PARTITION BY HASH(y) PARTITIONS 4")
        .unwrap();
    // MySQL: HASH/KEY treat NULL as 0 → bucket 0 (partition p0).
    engine.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t PARTITION (p0)").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

// ---------- M2/P0-3: consistent RANGE COLUMNS(date) routing and pruning ----------

#[test]
fn range_columns_date_routes_and_prunes_consistently() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, d DATE) PARTITION BY RANGE COLUMNS(d) (\
                PARTITION p0 VALUES LESS THAN ('2020-01-01'), \
                PARTITION p1 VALUES LESS THAN ('2022-01-01'), \
                PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'2019-06-01'), (2,'2021-06-01'), (3,'2023-06-01')")
        .unwrap();

    // Rows must enter the correct partitions, not all incorrectly enter p2/MAXVALUE.
    let count_in = |p: &str| -> i64 {
        let QueryResponse::ResultSet(rs) = engine
            .execute(&format!("SELECT COUNT(*) FROM t PARTITION({p})"))
            .unwrap()
        else {
            panic!("expected result set");
        };
        match rs.rows[0][0] {
            Value::Int(n) => n,
            Value::UInt(n) => n as i64,
            ref other => panic!("count not int: {other:?}"),
        }
    };
    assert_eq!(count_in("p0"), 1, "2019-06-01 belongs in p0");
    assert_eq!(count_in("p1"), 1, "2021-06-01 belongs in p1");
    assert_eq!(count_in("p2"), 1, "2023-06-01 belongs in p2");

    // WHERE pruning must agree with routing; previously pruning chose p0 while data sat in p2.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM t WHERE d = '2019-06-01'")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);
}

// ---------- M2/P0-4: vector scans preserve YEAR values instead of making them NULL ----------

#[test]
fn year_column_survives_vectorized_scan() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, y YEAR)")
        .unwrap();
    // Insert enough rows to trigger the vector scan threshold.
    let vals: Vec<String> = (1..=500)
        .map(|i| format!("({}, {})", i, 2000 + (i % 50)))
        .collect();
    engine
        .execute(&format!("INSERT INTO t VALUES {}", vals.join(",")))
        .unwrap();
    // If the bridge nullifies YEAR, COUNT(y) falls below 500 and MAX(y) becomes NULL.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT COUNT(y), MIN(y), MAX(y) FROM t")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0],
        Value::Int(500),
        "no YEAR value should be NULLed"
    );
    assert_eq!(rs.rows[0][1].to_display_string(), "2000");
    assert_eq!(rs.rows[0][2].to_display_string(), "2049");
}

// ---------- Second DDL partition audit: docs/ddl-audit-2026-07-07-followup.md ----------

// N8: ADD PARTITION validates the post-append layout and rejects nonmonotonic RANGE bounds.
#[test]
fn n8_add_partition_rejects_non_monotonic_range() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL) PARTITION BY RANGE (id) (\
               PARTITION p0 VALUES LESS THAN (10)\
             )",
        )
        .unwrap();
    // Appending LESS THAN below the existing boundary must fail, not silently succeed.
    let err = engine
        .execute("ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (5))")
        .expect_err("non-monotonic ADD PARTITION must be rejected");
    assert_eq!(
        err.code(),
        1493,
        "non-monotonic RANGE bound must be 1493: {err:?}"
    );
    // A valid monotonically increasing append still succeeds.
    engine
        .execute("ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (20))")
        .expect("monotonic ADD PARTITION should succeed");
}

// N9: EXCHANGE PARTITION on tables with foreign keys returns 1740.
#[test]
fn n9_exchange_partition_rejects_foreign_key_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE parent (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    // Partitioned table on the left.
    engine
        .execute(
            "CREATE TABLE pt (id INT NOT NULL PRIMARY KEY, pid INT) \
             PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100))",
        )
        .unwrap();
    // Staging table with an FK referencing parent.
    engine
        .execute(
            "CREATE TABLE st (id INT NOT NULL PRIMARY KEY, pid INT, \
             CONSTRAINT fk_st FOREIGN KEY (pid) REFERENCES parent (id))",
        )
        .unwrap();
    let err = engine
        .execute("ALTER TABLE pt EXCHANGE PARTITION p0 WITH TABLE st")
        .expect_err("EXCHANGE with an FK table must be rejected");
    assert_eq!(err.code(), 1740, "EXCHANGE with FK must be 1740: {err:?}");
}

// N4: EXCHANGE PARTITION migrates the .lob sidecar so external LOBs remain readable.
#[test]
fn n4_exchange_partition_migrates_lob_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE pt (id INT NOT NULL PRIMARY KEY, body TEXT) \
             PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100))",
        )
        .unwrap();
    engine
        .execute("CREATE TABLE st (id INT NOT NULL PRIMARY KEY, body TEXT)")
        .unwrap();
    // Text above the default 4096-byte inline threshold externalizes into .lob.
    let big = "x".repeat(9000);
    engine
        .execute(&format!("INSERT INTO st (id, body) VALUES (7, '{big}')"))
        .unwrap();
    engine
        .execute("ALTER TABLE pt EXCHANGE PARTITION p0 WITH TABLE st")
        .expect("EXCHANGE should succeed");
    // After exchange, pt's partition must return the complete external LOB.
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT LENGTH(body) FROM pt WHERE id = 7")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0].to_display_string(),
        "9000",
        "externalized LOB must survive EXCHANGE (sidecar migrated)"
    );
}

// N7: DROP PARTITION on subpartitioned tables succeeds and preserves other partitions;
// previously physical deletion used the parent placeholder storage_id and orphaned leaf files.
#[test]
fn n7_drop_partition_subpartitioned_keeps_other_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT NOT NULL, k INT NOT NULL) \
             PARTITION BY RANGE (id) SUBPARTITION BY HASH (k) SUBPARTITIONS 2 (\
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (20)\
             )",
        )
        .unwrap();
    engine
        .execute("INSERT INTO t (id, k) VALUES (1, 1), (2, 2), (11, 3), (12, 4)")
        .unwrap();
    engine
        .execute("ALTER TABLE t DROP PARTITION p0")
        .expect("DROP PARTITION on subpartitioned table should succeed");
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM t").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0].to_display_string(),
        "2",
        "p1 rows must survive"
    );
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM t ORDER BY id").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "11");
    assert_eq!(rs.rows[1][0].to_display_string(), "12");
}

#[test]
fn create_table_like_copies_range_partitioning_and_routes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE src (id INT NOT NULL, y INT NOT NULL) PARTITION BY RANGE (y) (\
                PARTITION p0 VALUES LESS THAN (10), \
                PARTITION p1 VALUES LESS THAN (20), \
                PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    // A row in the source must NOT be copied by LIKE.
    engine
        .execute("INSERT INTO src (id, y) VALUES (1, 5)")
        .unwrap();

    engine.execute("CREATE TABLE dst LIKE src").unwrap();

    // Partition definition round-trips through SHOW CREATE.
    let QueryResponse::ResultSet(create) = engine.execute("SHOW CREATE TABLE dst").unwrap() else {
        panic!("expected result set");
    };
    let create_sql = create.rows[0][1].to_display_string();
    assert!(create_sql.contains("PARTITION BY RANGE"), "{create_sql}");
    assert!(create_sql.contains("p0"), "{create_sql}");
    assert!(create_sql.contains("p1"), "{create_sql}");
    assert!(create_sql.contains("p2"), "{create_sql}");

    // dst starts empty even though src has a row.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM dst").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "0");

    // Rows route into the right partition heaps — which must all exist,
    // otherwise the INSERT into that partition would fail.
    engine
        .execute("INSERT INTO dst (id, y) VALUES (100, 5), (200, 15), (300, 25)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM dst PARTITION (p0) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM dst PARTITION (p1) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(200)]]);
    let QueryResponse::ResultSet(rs) = engine
        .execute("SELECT id FROM dst PARTITION (p2) ORDER BY id")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows, vec![vec![Value::Int(300)]]);

    // src storage is independent of dst writes.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM src").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "1");
}

#[test]
fn create_table_like_copies_hash_partitioning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute("CREATE TABLE src (id INT NOT NULL) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    engine.execute("CREATE TABLE dst LIKE src").unwrap();

    let QueryResponse::ResultSet(create) = engine.execute("SHOW CREATE TABLE dst").unwrap() else {
        panic!("expected result set");
    };
    let create_sql = create.rows[0][1].to_display_string();
    assert!(create_sql.contains("PARTITION BY HASH"), "{create_sql}");
    assert!(create_sql.contains("PARTITIONS 4"), "{create_sql}");

    engine
        .execute("INSERT INTO dst (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8)")
        .unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM dst").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "8");
}

#[test]
fn create_table_like_copies_subpartitioning() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());

    engine
        .execute(
            "CREATE TABLE src (id INT, y INT) \
             PARTITION BY RANGE (y) \
             SUBPARTITION BY HASH (id) SUBPARTITIONS 2 \
             (PARTITION p0 VALUES LESS THAN (10), \
              PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine.execute("CREATE TABLE dst LIKE src").unwrap();

    let QueryResponse::ResultSet(create) = engine.execute("SHOW CREATE TABLE dst").unwrap() else {
        panic!("expected result set");
    };
    let create_sql = create.rows[0][1].to_display_string();
    assert!(create_sql.contains("PARTITION BY RANGE"), "{create_sql}");
    assert!(create_sql.contains("SUBPARTITION BY HASH"), "{create_sql}");

    // Every leaf subpartition heap must exist so all four buckets accept
    // rows and read back.
    for id in 0..20 {
        let y = if id < 10 { 5 } else { 15 };
        engine
            .execute(&format!("INSERT INTO dst VALUES ({id}, {y})"))
            .unwrap();
    }
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM dst").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows[0][0].to_display_string(), "20");
}

// -- S1: RENAME TABLE on a partitioned table must move partition files -----

/// Regression for S1 (alter-table-review-2026-07): `RENAME TABLE` on a
/// partitioned table used to rename only the logical `table_id` sidecars,
/// leaving every partition heap (named by its `storage_id`) at the old path.
/// Within the process a stale cache masked the loss; after reopen the new
/// name resolved to non-existent partition heaps and the data vanished.
#[test]
fn rename_partitioned_table_preserves_data_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine
            .execute(
                "CREATE TABLE p (id INT, y INT, KEY k (y)) \
                 PARTITION BY RANGE (id) ( \
                   PARTITION p0 VALUES LESS THAN (10), \
                   PARTITION p1 VALUES LESS THAN (100), \
                   PARTITION p2 VALUES LESS THAN MAXVALUE)",
            )
            .unwrap();
        for i in [1, 5, 42, 77, 500, 999] {
            engine
                .execute(&format!("INSERT INTO p VALUES ({i}, {i})"))
                .unwrap();
        }
        engine.execute("RENAME TABLE p TO q").unwrap();
        // In-process the rename must already report the rows under the new
        // name (and the old name must be gone).
        let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM q").unwrap() else {
            panic!("expected result set");
        };
        assert_eq!(
            rs.rows[0][0],
            Value::Int(6),
            "in-process row count after rename"
        );
        assert!(
            engine.execute("SELECT COUNT(*) FROM p").is_err(),
            "old name must be gone"
        );
        drop(engine);
    }
    // Reopen: the partition heaps must have moved with the table so the
    // rows survive a restart (this is what regressed before the fix).
    let engine = open_partition_recovery_test_engine(dir.path());
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT COUNT(*) FROM q").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(
        rs.rows[0][0],
        Value::Int(6),
        "rows must survive reopen after rename"
    );
    // Partition-routed predicate still finds the row in p1.
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT id FROM q WHERE id = 42").unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(rs.rows.len(), 1);
    assert_eq!(partition_names(&engine, "q").len(), 3);
}

/// Regression for S2 (alter-table-review-2026-07): REORGANIZE PARTITION must
/// validate the resulting layout. Splicing a new bound that overshoots a
/// retained successor partition (non-monotonic RANGE sequence) must be
/// rejected (1493), not silently accepted and then mis-routed.
#[test]
fn reorganize_partition_rejects_non_monotonic_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT) PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (100), \
               PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    // b < 200 overshoots the retained p1 (< 100): the resulting sequence
    // a<5, b<200, p1<100 is non-monotonic and must be rejected.
    let err = engine
        .execute(
            "ALTER TABLE t REORGANIZE PARTITION p0 INTO ( \
               PARTITION a VALUES LESS THAN (5), \
               PARTITION b VALUES LESS THAN (200))",
        )
        .unwrap_err();
    assert_eq!(
        err.code(),
        1493,
        "expected non-monotonic RANGE rejection: {err:?}"
    );
    // The original layout must be untouched.
    assert_eq!(partition_names(&engine, "t"), vec!["p0", "p1", "p2"]);

    // A valid split within the reorganized range still works.
    engine
        .execute(
            "ALTER TABLE t REORGANIZE PARTITION p0 INTO ( \
               PARTITION a VALUES LESS THAN (5), \
               PARTITION b VALUES LESS THAN (10))",
        )
        .unwrap();
    assert_eq!(partition_names(&engine, "t"), vec!["a", "b", "p1", "p2"]);
}

/// Regression for S12 (alter-table-review-2026-07): DROP PARTITION used to
/// delete only the `.idx` file, leaking a vector index's `.hvec`/`.hnsw`
/// files for the removed partition. Count the vector index files before and
/// after the drop: the dropped partition's file must be gone.
#[test]
fn drop_partition_removes_vector_index_files() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE v (id INT, emb VECTOR(4) NOT NULL) PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (20), \
               PARTITION p2 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX vx ON v (emb) USING HNSW")
        .unwrap();
    // Populate p0 and p1 so each builds a per-partition graph file.
    for id in [1, 5, 12, 15] {
        engine
            .execute(&format!(
                "INSERT INTO v VALUES ({id}, STRING_TO_VECTOR('[{}, 0, 0, 0]'))",
                id
            ))
            .unwrap();
    }
    // Force the graphs to materialize via a KNN query on each partition.
    let _ = engine.execute(
        "SELECT id FROM v ORDER BY DISTANCE(emb, STRING_TO_VECTOR('[1,0,0,0]'), 'L2') LIMIT 2",
    );

    let count_vec_files = || {
        object_file_names(dir.path())
            .into_iter()
            .filter(|name| name.ends_with(".hvec") || name.ends_with(".hnsw"))
            .count()
    };
    let before = count_vec_files();
    assert!(
        before >= 2,
        "expected per-partition vector files, found {before}"
    );

    engine.execute("ALTER TABLE v DROP PARTITION p0").unwrap();

    let after = count_vec_files();
    assert!(
        after < before,
        "DROP PARTITION must remove the dropped partition's vector index files \
         (before={before}, after={after}) — leftover files indicate the leak"
    );
}

#[test]
fn reorganize_partition_replaces_vector_files_without_orphans() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE v (id INT, emb VECTOR(3) NOT NULL) PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX vx ON v(emb) USING HNSW")
        .unwrap();
    engine
        .execute("INSERT INTO v VALUES (1, '[1,0,0]'), (7, '[7,0,0]'), (20, '[20,0,0]')")
        .unwrap();
    let before = engine.database().table("test.v").unwrap().storage_ids();
    let old_sid = before[0];
    let names = || object_file_names(dir.path());
    assert!(names()
        .iter()
        .any(|n| n.contains(&format!("indexes/{old_sid}/")) && n.ends_with(".hvec")));

    engine
        .execute(
            "ALTER TABLE v REORGANIZE PARTITION p0 INTO ( \
               PARTITION p0a VALUES LESS THAN (5), \
               PARTITION p0b VALUES LESS THAN (10))",
        )
        .unwrap();
    let after = engine.database().table("test.v").unwrap().storage_ids();
    assert!(!after.contains(&old_sid));
    let after_names = names();
    assert!(
        !after_names
            .iter()
            .any(|n| n.contains(&format!("indexes/{old_sid}/")) && n.ends_with(".hvec")),
        "old partition vector file leaked: {after_names:?}"
    );
    for sid in after {
        assert!(
            after_names
                .iter()
                .any(|n| n.contains(&format!("indexes/{sid}/")) && n.ends_with(".hvec")),
            "new/retained partition {sid} was not warmed: {after_names:?}"
        );
    }
    let QueryResponse::ResultSet(result) = engine.execute("SELECT COUNT(*) FROM v").unwrap() else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn add_partition_eagerly_finalizes_empty_vector_graph() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE v (id INT, emb VECTOR(2) NOT NULL) PARTITION BY RANGE (id) ( \
               PARTITION p0 VALUES LESS THAN (10))",
        )
        .unwrap();
    engine
        .execute("CREATE INDEX vx ON v(emb) USING HNSW")
        .unwrap();
    engine
        .execute("ALTER TABLE v ADD PARTITION (PARTITION p1 VALUES LESS THAN (20))")
        .unwrap();
    let table = engine.database().table("test.v").unwrap();
    let new_sid = *table.storage_ids().last().unwrap();
    let found = object_file_names(dir.path())
        .into_iter()
        .any(|n| n.contains(&format!("indexes/{new_sid}/")) && n.ends_with(".hvec"));
    assert!(
        found,
        "ADD PARTITION must warm/finalize its empty HNSW graph"
    );
    engine
        .execute("INSERT INTO v VALUES (12, '[12,0]')")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute("SELECT id FROM v ORDER BY DISTANCE(emb, '[12,0]') LIMIT 1")
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(result.rows[0][0], Value::Int(12));
}

/// P0-8: DROP COLUMN on a column used by PARTITION BY LIST COLUMNS must be
/// rejected. Previously the drop silently succeeded and any subsequent DML
/// broke with "unknown partition column".
#[test]
fn p0_8_drop_column_used_by_list_columns_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, region VARCHAR(10)) \
             PARTITION BY LIST COLUMNS(region) ( \
               PARTITION p0 VALUES IN ('a'), \
               PARTITION p1 VALUES IN ('b') \
             )",
        )
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,'a')").unwrap();
    let err = engine
        .execute("ALTER TABLE t DROP COLUMN region")
        .expect_err("DROP COLUMN on partition column must be rejected");
    assert_eq!(err.code(), 1054, "wrong error code: {err:?}");
    // Table still usable: another INSERT routes correctly.
    engine.execute("INSERT INTO t VALUES (2,'b')").unwrap();
}

/// P0-8: same guard covers RANGE(expr) partitioning where the expr refers
/// to the dropped column.
#[test]
fn p0_8_drop_column_used_by_range_expr_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) \
             PARTITION BY RANGE (y) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (20) \
             )",
        )
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t DROP COLUMN y")
        .expect_err("DROP COLUMN on partition-expression column must be rejected");
    assert_eq!(err.code(), 1054, "wrong error code: {err:?}");
}

/// P1-11: RENAME COLUMN must propagate the new name into
/// partitioning.columns / expr_sql so SHOW CREATE TABLE and the runtime
/// router stay coherent.
#[test]
fn p1_11_rename_column_updates_list_columns_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, region VARCHAR(10)) \
             PARTITION BY LIST COLUMNS(region) ( \
               PARTITION p0 VALUES IN ('a'), \
               PARTITION p1 VALUES IN ('b') \
             )",
        )
        .unwrap();
    engine
        .execute("ALTER TABLE t RENAME COLUMN region TO area")
        .unwrap();
    // Insert must route by the new name.
    engine.execute("INSERT INTO t VALUES (1,'a')").unwrap();
    engine.execute("INSERT INTO t VALUES (2,'b')").unwrap();
    // SHOW CREATE TABLE must reflect the new name.
    let response = engine.execute("SHOW CREATE TABLE t").unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected SHOW CREATE result set");
    };
    let create_sql = match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected type: {other:?}"),
    };
    assert!(
        create_sql.to_ascii_lowercase().contains("area"),
        "SHOW CREATE must use new column name: {create_sql}"
    );
    assert!(
        !create_sql.to_ascii_lowercase().contains("(region)"),
        "SHOW CREATE must not reference old column name: {create_sql}"
    );
}

/// P1-11: rename also propagates for RANGE(expr) partitioning.
#[test]
fn p1_11_rename_column_updates_range_expr_definition() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute(
            "CREATE TABLE t (id INT, y INT) \
             PARTITION BY RANGE (y) ( \
               PARTITION p0 VALUES LESS THAN (10), \
               PARTITION p1 VALUES LESS THAN (20) \
             )",
        )
        .unwrap();
    engine
        .execute("ALTER TABLE t RENAME COLUMN y TO bucket")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1, 5)").unwrap();
    engine.execute("INSERT INTO t VALUES (2, 15)").unwrap();
    let response = engine.execute("SHOW CREATE TABLE t").unwrap();
    let QueryResponse::ResultSet(rs) = response else {
        panic!("expected SHOW CREATE result set");
    };
    let create_sql = match &rs.rows[0][1] {
        Value::String(s) => s.clone(),
        other => panic!("unexpected type: {other:?}"),
    };
    assert!(
        create_sql.to_ascii_lowercase().contains("bucket"),
        "SHOW CREATE must use new column name in expr: {create_sql}"
    );
}
#[test]
fn audit_repro_partitioned_select_for_update_really_locks_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut holder = SessionContext::default();
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(
            &mut holder,
            "CREATE TABLE t (id INT, y INT, PRIMARY KEY(id,y)) PARTITION BY RANGE (y) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut holder, "INSERT INTO t VALUES (1, 1)")
        .unwrap();
    engine.execute_with_session(&mut holder, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut holder, "SELECT * FROM t WHERE id=1 AND y=1 FOR UPDATE")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "SET SESSION innodb_lock_wait_timeout=1")
        .unwrap();
    let blocked = engine
        .execute_with_session(&mut writer, "UPDATE t SET id=2 WHERE id=1 AND y=1")
        .unwrap_err();
    assert_eq!(
        blocked.code(),
        1205,
        "expected row-lock timeout: {blocked:?}"
    );
    engine
        .execute_with_session(&mut holder, "ROLLBACK")
        .unwrap();
}

#[test]
fn audit_repro_partitioned_secondary_update_rollback_restores_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT, y INT, k INT, PRIMARY KEY(id,y), KEY k_idx(k)) PARTITION BY RANGE (y) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 1, 10)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "UPDATE t SET k=11 WHERE id=1 AND y=1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT id,y,k FROM t FORCE INDEX(k_idx) WHERE k=10",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1), Value::Int(1), Value::Int(10)]]
    );
}

#[test]
fn partitioned_secondary_delete_rollback_restores_local_index() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE t (id INT, y INT, k INT, PRIMARY KEY(id,y), KEY k_idx(k)) PARTITION BY RANGE (y) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO t VALUES (1, 1, 10)")
        .unwrap();
    engine.execute_with_session(&mut session, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut session, "DELETE FROM t WHERE id=1 AND y=1")
        .unwrap();
    engine
        .execute_with_session(&mut session, "ROLLBACK")
        .unwrap();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(
            &mut session,
            "SELECT id,y,k FROM t FORCE INDEX(k_idx) WHERE k=10",
        )
        .unwrap()
    else {
        panic!("expected result set");
    };
    assert_eq!(
        result.rows,
        vec![vec![Value::Int(1), Value::Int(1), Value::Int(10)]]
    );
}

#[test]
fn partition_ddl_rejects_unsafe_layout_changes_before_writes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN(20), PARTITION p2 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1),(8),(12),(22)")
        .unwrap();
    let before = partition_test_rows(&engine, "SELECT id FROM t ORDER BY id");
    for sql in [
        "ALTER TABLE t REORGANIZE PARTITION p0 INTO (PARTITION q VALUES LESS THAN(5))",
        "ALTER TABLE t REORGANIZE PARTITION p0 INTO (PARTITION q VALUES LESS THAN(15))",
        "ALTER TABLE t DROP PARTITION p0,p1,p2",
    ] {
        assert!(engine.execute(sql).is_err(), "must reject: {sql}");
        assert_eq!(
            partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
            before
        );
        assert_eq!(partition_names(&engine, "t"), vec!["p0", "p1", "p2"]);
    }
}

#[test]
fn partition_ddl_rejects_duplicate_list_sources() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT) PARTITION BY LIST(id) (PARTITION p0 VALUES IN(1,2), PARTITION p1 VALUES IN(3,4))").unwrap();
    engine.execute("INSERT INTO t VALUES(1),(3)").unwrap();
    assert!(engine
        .execute("ALTER TABLE t REORGANIZE PARTITION p0,P0 INTO (PARTITION q VALUES IN(1,2))")
        .is_err());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(3)]]
    );
}

#[test]
fn partition_ddl_truncate_all_requires_partitioned_table() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT)").unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    assert!(engine
        .execute("ALTER TABLE t TRUNCATE PARTITION ALL")
        .is_err());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn partition_ddl_shared_build_allows_reads_and_blocks_writes() {
    use std::sync::Arc;
    for (definition, action) in [
        ("PARTITION BY HASH(id) PARTITIONS 3", "ADD PARTITION PARTITIONS 1"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "COALESCE PARTITION 1"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "REMOVE PARTITIONING"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "PARTITION BY KEY(id) PARTITIONS 5"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "REBUILD PARTITION ALL"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "TRUNCATE PARTITION ALL"),
        ("PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)",
         "REORGANIZE PARTITION p0 INTO(PARTITION q0 VALUES LESS THAN(5), PARTITION q1 VALUES LESS THAN(10))"),
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = Arc::new(open_partition_test_engine(dir.path()));
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY) {definition}")).unwrap();
        engine.execute("INSERT INTO t VALUES(1),(8),(12)").unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let sql = format!("ALTER TABLE t {action}, LOCK=SHARED");
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8101;
            worker.execute_with_session(&mut session, &sql)
        });
        let reached = hook.wait_until_reached(Duration::from_secs(10));
        let mut reader = SessionContext::default();
        reader.connection_id = 8102;
        reader.sysvars.lock_wait_timeout = Some(0);
        let read = engine.execute_with_session(&mut reader, "SELECT id FROM t ORDER BY id")
            .and_then(|r| r.materialize());
        let mut writer = SessionContext::default();
        writer.connection_id = 8103;
        writer.sysvars.lock_wait_timeout = Some(0);
        let write = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(7)");
        let other = engine.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let competing = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8104;
            session.sysvars.lock_wait_timeout = Some(0);
            tx.send(other.execute_with_session(&mut session,
                "ALTER TABLE t REBUILD PARTITION ALL, LOCK=SHARED")).unwrap();
        });
        let other_result = rx.recv_timeout(Duration::from_secs(2));
        hook.release();
        competing.join().unwrap();
        assert_eq!(other_result.expect("competing DDL must fail without entering build")
            .unwrap_err().code(), 1205);
        let result = ddl.join().unwrap();
        assert!(reached, "build hook not reached: {action}: {result:?}");
        let QueryResponse::ResultSet(rows) = read.expect("reader must complete during build") else { panic!("rows") };
        assert_eq!(rows.rows.len(), 3, "{action}");
        assert_eq!(write.unwrap_err().code(), 1205, "writer must wait: {action}");
        result.unwrap();
        let expected = if action.starts_with("TRUNCATE") { 0 } else { 3 };
        assert_eq!(partition_test_rows(&engine, "SELECT id FROM t").len(), expected);
        engine.execute("INSERT INTO t VALUES(7)").unwrap();
    }
}

#[test]
fn partition_ddl_cutover_timeout_preserves_source_and_releases_locks() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let original = engine.database().table("test.t").unwrap().storage_ids();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8201;
        session.sysvars.lock_wait_timeout = Some(0);
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t REBUILD PARTITION ALL, LOCK=SHARED",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut reader = SessionContext::default();
    reader.connection_id = 8202;
    reader.sysvars.lock_wait_timeout = Some(0);
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let read = engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .and_then(|r| r.materialize());
    hook.release();
    let result = ddl.join().unwrap();
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert!(reached, "{result:?}");
    read.unwrap();
    assert_eq!(result.unwrap_err().code(), 1205);
    assert_eq!(
        engine.database().table("test.t").unwrap().storage_ids(),
        original
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    engine.execute("INSERT INTO t VALUES(3)").unwrap();
    engine
        .execute("ALTER TABLE t REBUILD PARTITION ALL, LOCK=SHARED")
        .unwrap();
}

#[test]
fn partition_ddl_truncate_generations_recover_atomically() {
    for crash in ["before_swap", "after_swap"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let original;
        {
            let engine = open_partition_recovery_test_engine(dir.path());
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY, payload TEXT, INDEX ix(payload(12))) PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,REPEAT('a',9000)),(2,'b'),(11,'c')")
                .unwrap();
            original = engine.database().table("test.t").unwrap().storage_ids();
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", crash);
            engine
                .execute("ALTER TABLE t TRUNCATE PARTITION p0")
                .unwrap();
            assert_eq!(
                crate::storage::reorg_peak_batch_rows(),
                0,
                "truncate must not scan source rows"
            );
        }
        let engine = open_partition_recovery_test_engine(dir.path());
        let ids = partition_test_rows(&engine, "SELECT id FROM t ORDER BY id");
        let current = engine.database().table("test.t").unwrap().storage_ids();
        if crash == "before_swap" {
            assert_eq!(current, original);
            assert_eq!(ids.len(), 3);
            assert_eq!(
                partition_test_rows(&engine, "SELECT LENGTH(payload) FROM t WHERE id=1"),
                vec![vec![Value::Int(9000)]]
            );
        } else {
            assert_ne!(&current[..2], &original[..2]);
            assert_eq!(&current[2..], &original[2..]);
            assert_eq!(ids, vec![vec![Value::Int(11)]]);
        }
        engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
        engine.execute("INSERT INTO t VALUES(3,'new')").unwrap();
    }
}

#[test]
fn partition_ddl_range_columns_reorg_preserves_upper_bound() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE t(a INT, b INT) PARTITION BY RANGE COLUMNS(a,b) (PARTITION p0 VALUES LESS THAN(10,10), PARTITION p1 VALUES LESS THAN(20,20))").unwrap();
    engine.execute("INSERT INTO t VALUES(1,1),(11,11)").unwrap();
    assert!(engine
        .execute("ALTER TABLE t REORGANIZE PARTITION p0 INTO(PARTITION q VALUES LESS THAN(10,5))")
        .is_err());
    engine
        .execute("ALTER TABLE t REORGANIZE PARTITION p1 INTO(PARTITION q VALUES LESS THAN(30,30))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(25,25)").unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT a FROM t ORDER BY a"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(11)],
            vec![Value::Int(25)]
        ]
    );
    engine
        .execute("ALTER TABLE t REBUILD PARTITION ALL, LOCK=NONE")
        .unwrap();
    assert!(engine
        .execute("ALTER TABLE t TRUNCATE PARTITION ALL, LOCK=NONE")
        .is_err());
}

#[test]
fn partition_ddl_drop_recovery_cleans_only_committed_sources() {
    for crash in ["before_swap", "after_swap"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let original_files;
        {
            let engine = open_partition_recovery_test_engine(dir.path());
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY, payload TEXT) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
            engine
                .execute("INSERT INTO t VALUES(1,REPEAT('x',9000)),(11,'retained')")
                .unwrap();
            original_files = object_file_names(dir.path());
            let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", crash);
            engine.execute("ALTER TABLE t DROP PARTITION p0").unwrap();
        }
        let engine = open_partition_recovery_test_engine(dir.path());
        let ids = partition_test_rows(&engine, "SELECT id FROM t ORDER BY id");
        if crash == "before_swap" {
            assert_eq!(ids.len(), 2);
            assert_eq!(partition_names(&engine, "t").len(), 2);
        } else {
            assert_eq!(ids, vec![vec![Value::Int(11)]]);
            assert_eq!(partition_names(&engine, "t"), vec!["p1"]);
            let files = object_file_names(dir.path());
            assert!(
                original_files
                    .iter()
                    .filter(|file| file.ends_with(".heap"))
                    .count()
                    > files.iter().filter(|file| file.ends_with(".heap")).count()
            );
        }
        engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    }
}

#[test]
fn partition_ddl_internal_read_transactions_do_not_leak() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let baseline = engine.database().storage_status().active_transactions;
    for sql in [
        "ALTER TABLE t REBUILD PARTITION ALL",
        "ALTER TABLE t CHECK PARTITION ALL",
        "ALTER TABLE t REPAIR PARTITION ALL",
        "ALTER TABLE t TRUNCATE PARTITION ALL",
    ] {
        engine.execute(sql).unwrap();
        assert_eq!(
            engine.database().storage_status().active_transactions,
            baseline,
            "{sql}"
        );
    }
}

#[test]
fn partition_ddl_truncate_all_preserves_auto_increment_and_rejects_drop_all() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT AUTO_INCREMENT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("INSERT INTO t VALUES(50)").unwrap();
    assert!(engine
        .execute("ALTER TABLE t DROP PARTITION p0,p1")
        .is_err());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t"),
        vec![vec![Value::Int(50)]]
    );
    engine
        .execute("ALTER TABLE t TRUNCATE PARTITION ALL")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(NULL)").unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t"),
        vec![vec![Value::Int(51)]]
    );
    drop(engine);
    let engine = open_partition_recovery_test_engine(dir.path());
    engine.execute("INSERT INTO t VALUES(NULL)").unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![vec![Value::Int(51)], vec![Value::Int(52)]]
    );
}

#[test]
fn partition_ddl_cancel_waiting_cutover_preserves_source() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let original = engine.database().table("test.t").unwrap().storage_ids();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8301;
        session.sysvars.lock_wait_timeout = Some(5);
        tx.send(worker.execute_with_session(
            &mut session,
            "ALTER TABLE t REBUILD PARTITION ALL, LOCK=SHARED",
        ))
        .unwrap();
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut reader = SessionContext::default();
    reader.connection_id = 8302;
    reader.sysvars.lock_wait_timeout = Some(0);
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let read = engine
        .execute_with_session(&mut reader, "SELECT * FROM t")
        .and_then(|r| r.materialize());
    hook.release();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let job = loop {
        let jobs = engine.database().partition_ddl_job_snapshots();
        if let Some(job) = jobs.into_iter().find(|job| job.state == "QUIESCE") {
            break Some(job);
        }
        if std::time::Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let cancelled = job.map(|job| engine.database().cancel_partition_ddl_job(job.job_id));
    let result = rx.recv_timeout(Duration::from_secs(3));
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    ddl.join().unwrap();
    assert!(reached);
    read.unwrap();
    assert_eq!(cancelled.unwrap().unwrap(), true);
    assert_eq!(
        result
            .expect("cancellation must not wait for reader commit")
            .unwrap_err()
            .code(),
        1317
    );
    assert_eq!(
        engine.database().table("test.t").unwrap().storage_ids(),
        original
    );
    assert_eq!(partition_test_rows(&engine, "SELECT id FROM t").len(), 2);
    engine.execute("INSERT INTO t VALUES(3)").unwrap();
}

#[test]
fn partition_ddl_releases_exclusive_lock_before_source_cleanup() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let original = engine.database().table("test.t").unwrap().storage_ids();
    let build = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8401;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PARTITION PARTITIONS 1, LOCK=SHARED",
        )
    });
    let built = build.wait_until_reached(Duration::from_secs(10));
    let cleanup = engine.database().install_ddl_backfill_test_hook();
    build.release();
    let published = cleanup.wait_until_reached(Duration::from_secs(10));
    let current = engine.database().table("test.t").unwrap().storage_ids();
    let late_cancel = engine
        .database()
        .partition_ddl_job_snapshots()
        .first()
        .map(|job| engine.database().cancel_partition_ddl_job(job.job_id));
    let mut writer = SessionContext::default();
    writer.connection_id = 8402;
    writer.sysvars.lock_wait_timeout = Some(0);
    let writes = [
        "INSERT INTO t VALUES(3)",
        "UPDATE t SET id=4 WHERE id=1",
        "DELETE FROM t WHERE id=2",
    ]
    .map(|sql| engine.execute_with_session(&mut writer, sql));
    let read = engine
        .execute_with_session(&mut writer, "SELECT id FROM t ORDER BY id")
        .and_then(|r| r.materialize());
    cleanup.release();
    let result = ddl.join().unwrap();
    assert_eq!(
        late_cancel
            .expect("publication journal")
            .unwrap_err()
            .code(),
        1235,
        "published DDL must reject cancellation"
    );
    assert!(built && published, "{result:?}");
    assert_ne!(current, original);
    for write in writes {
        write.expect("new-layout DML must not wait for source cleanup");
    }
    let QueryResponse::ResultSet(rows) = read.unwrap() else {
        panic!("rows")
    };
    assert_eq!(rows.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);
    result.unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
        rows.rows
    );
}

#[test]
fn partition_ddl_ambiguous_catalog_failure_requires_recovery() {
    for action in [
        "REBUILD PARTITION ALL",
        "TRUNCATE PARTITION ALL",
        "DROP PARTITION p0",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        {
            let engine = open_partition_recovery_test_engine(dir.path());
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
            engine.execute("INSERT INTO t VALUES(1),(11)").unwrap();
            engine.database().fail_next_catalog_commit_for_test();
            assert!(engine.execute(&format!("ALTER TABLE t {action}")).is_err());
            assert!(
                engine.database().ensure_operational().is_err(),
                "ambiguous publication must reject subsequent writes: {action}"
            );
        }
        let engine = open_partition_recovery_test_engine(dir.path());
        assert_eq!(
            partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(11)]]
        );
        engine.execute("INSERT INTO t VALUES(2)").unwrap();
    }
}

#[test]
fn partition_online_reorg_replays_cross_leaf_writes_and_rollback() {
    for (definition, action) in [
        ("PARTITION BY HASH(id) PARTITIONS 3", "ADD PARTITION PARTITIONS 1"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "COALESCE PARTITION 1"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "REMOVE PARTITIONING"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "PARTITION BY KEY(id) PARTITIONS 5"),
        ("PARTITION BY HASH(id) PARTITIONS 3", "REBUILD PARTITION ALL"),
        ("PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)", "REBUILD PARTITION p0"),
        ("PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)", "REORGANIZE PARTITION p0 INTO(PARTITION q0 VALUES LESS THAN(5), PARTITION q1 VALUES LESS THAN(10))"),
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = std::sync::Arc::new(open_partition_recovery_test_engine(dir.path()));
        engine.execute(&format!("CREATE TABLE t(id INT PRIMARY KEY, v INT, INDEX iv(v)) {definition}")).unwrap();
        engine.execute("INSERT INTO t VALUES(1,10),(8,80),(12,120)").unwrap();
        let source = engine.database().table("test.t").unwrap();
        let hook = engine.database().install_ddl_backfill_test_hook();
        let worker = engine.clone();
        let sql = format!("ALTER TABLE t {action}, LOCK=NONE");
        let ddl = std::thread::spawn(move || {
            let mut session = SessionContext::default();
            session.connection_id = 8501;
            worker.execute_with_session(&mut session, &sql)
        });
        let reached = hook.wait_until_reached(Duration::from_secs(5));
        if !reached {
            hook.release();
            panic!("online build not reached: {action}: {:?}", ddl.join().unwrap());
        }
        let mut writer = SessionContext::default();
        writer.connection_id = 8502;
        writer.sysvars.lock_wait_timeout = Some(0);
        let results = ["BEGIN", "UPDATE t SET id=11,v=110 WHERE id=1", "UPDATE t SET id=2,v=20 WHERE id=12", "DELETE FROM t WHERE id=8", "INSERT INTO t VALUES(3,30)", "SAVEPOINT s", "INSERT INTO t VALUES(7,70)", "ROLLBACK TO s", "COMMIT", "BEGIN", "UPDATE t SET v=999", "ROLLBACK"]
            .map(|sql| engine.execute_with_session(&mut writer, sql));
        hook.release();
        let result = ddl.join().unwrap();
        for write in results { write.expect("DML must run during online partition build"); }
        result.unwrap();
        assert_eq!(partition_test_rows(&engine, "SELECT id,v FROM t ORDER BY id"), vec![vec![Value::Int(2),Value::Int(20)],vec![Value::Int(3),Value::Int(30)],vec![Value::Int(11),Value::Int(110)]]);
        assert_eq!(partition_test_rows(&engine, "SELECT id FROM t FORCE INDEX(iv) WHERE v>=20 ORDER BY id"), vec![vec![Value::Int(2)],vec![Value::Int(3)],vec![Value::Int(11)]]);
        if action.ends_with("p0") || action.starts_with("REORGANIZE") {
            let target = engine.database().table("test.t").unwrap();
            assert_eq!(source.partitioning.as_ref().unwrap().storage_ids_for_name("p1"), target.partitioning.as_ref().unwrap().storage_ids_for_name("p1"), "retained partition must not be rebuilt");
        }
        drop(engine);
        let engine = open_partition_recovery_test_engine(dir.path());
        assert_eq!(partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"), vec![vec![Value::Int(2)],vec![Value::Int(3)],vec![Value::Int(11)]]);
    }
}

#[test]
fn partition_cleanup_failure_keeps_durable_intent_for_retry() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    // Fault injection renames a live heap path. Speculative prefetch could
    // recreate that path between rename and create_dir, racing the fixture.
    let engine = open_strict_cold_metadata_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine.execute("INSERT INTO t VALUES(1),(11)").unwrap();
    let table = engine.database().table("test.t").unwrap();
    let sid = table
        .partitioning
        .as_ref()
        .unwrap()
        .storage_ids_for_name("p0")
        .unwrap()[0];
    {
        let _crash = set_part_env("MYDB_PARTITION_REORG_CRASH_POINT", "after_swap");
        engine.execute("ALTER TABLE t DROP PARTITION p0").unwrap();
    }
    let path = crate::storage::naming::table_object_generation_path(
        dir.path(),
        sid,
        table.active_generation.max(1),
        "heap",
    );
    let saved = path.with_extension("held");
    std::fs::rename(&path, &saved).unwrap();
    // Lazy file opens may recreate this obsolete heap after the rename. Move
    // a pre-created poison directory into place atomically, retaining the
    // original inode in `saved` for the cleanup retry below.
    let poison = path.with_extension("poison");
    std::fs::create_dir(&poison).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        match std::fs::rename(&poison, &path) {
            Ok(()) => break,
            Err(err) if path.is_file() && std::time::Instant::now() < deadline => {
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(removed) if removed.kind() == std::io::ErrorKind::NotFound => {}
                    Err(removed) => panic!("remove recreated obsolete heap: {removed}"),
                }
                let _ = err;
            }
            Err(err) => panic!("install cleanup failure fixture: {err}"),
        }
    }
    let failure = engine.database().recover_partition_reorgs();
    assert!(
        failure.is_err(),
        "cleanup must not silently discard a failed unlink"
    );
    assert_eq!(engine.database().partition_ddl_job_snapshots().len(), 1);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(&saved, &path).unwrap();
    engine.database().recover_partition_reorgs().unwrap();
    assert!(engine.database().partition_ddl_job_snapshots().is_empty());
    assert!(!path.exists());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t"),
        vec![vec![Value::Int(11)]]
    );
}

#[test]
fn partition_online_reorg_replays_final_tail_with_lobs_and_expression_indexes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_recovery_test_engine(dir.path()));
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY, s VARCHAR(100), d DECIMAL(12,2), payload TEXT, INDEX ix(s(8)), INDEX ie((LOWER(s)))) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,'First',1.25,REPEAT('x',12000)),(2,'Second',2.50,'short')")
        .unwrap();
    let build = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8601;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PARTITION PARTITIONS 3, LOCK=NONE",
        )
    });
    let built = build.wait_until_reached(Duration::from_secs(10));
    let tail = engine.database().install_ddl_backfill_test_hook();
    build.release();
    let caught_up = tail.wait_until_reached(Duration::from_secs(10));
    let mut session = SessionContext::default();
    session.connection_id = 8602;
    session.sysvars.lock_wait_timeout = Some(0);
    let writes = [
        "UPDATE t SET id=11,s='CHANGED',d=3.75,payload=REPEAT('z',15000) WHERE id=1",
        "DELETE FROM t WHERE id=2",
        "INSERT INTO t VALUES(3,NULL,NULL,'new')",
    ]
    .map(|sql| engine.execute_with_session(&mut session, sql));
    let jobs = partition_test_rows(
        &engine,
        "SELECT JOB_ID FROM information_schema.mydb_ddl_jobs WHERE TABLE_NAME='t'",
    );
    tail.release();
    let result = ddl.join().unwrap();
    assert!(built && caught_up, "{result:?}");
    assert_eq!(
        jobs.len(),
        1,
        "one visible job per online partition rewrite"
    );
    for write in writes {
        write.unwrap();
    }
    result.unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t WHERE LOWER(s)='changed'"),
        vec![vec![Value::Int(11)]]
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT id,LENGTH(payload) FROM t ORDER BY id"),
        vec![
            vec![Value::Int(3), Value::Int(3)],
            vec![Value::Int(11), Value::Int(15000)]
        ]
    );
    engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    drop(engine);
    let engine = open_partition_recovery_test_engine(dir.path());
    assert_eq!(
        partition_test_rows(
            &engine,
            "SELECT id FROM t FORCE INDEX(ix) WHERE s='CHANGED'"
        ),
        vec![vec![Value::Int(11)]]
    );
}

#[test]
fn partition_online_reorg_cancel_keeps_committed_concurrent_writes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2)").unwrap();
    let ids = engine.database().table("test.t").unwrap().storage_ids();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8701;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t REBUILD PARTITION ALL, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 8702;
    writer.sysvars.lock_wait_timeout = Some(0);
    let write = engine.execute_with_session(&mut writer, "INSERT INTO t VALUES(3)");
    let cancelled = engine
        .database()
        .alter_copy_job_snapshots()
        .first()
        .map(|job| engine.database().cancel_ddl_job(job.job_id));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    write.unwrap();
    cancelled.unwrap().unwrap();
    assert_eq!(result.unwrap_err().code(), 1317);
    assert_eq!(
        engine.database().table("test.t").unwrap().storage_ids(),
        ids
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
}

#[test]
fn partition_ddl_process_crash_child() {
    let Ok(path) = std::env::var("MYDB_PARTITION_CRASH_CHILD_DIR") else {
        return;
    };
    let engine = open_partition_recovery_test_engine(std::path::Path::new(&path));
    engine.execute(if std::env::var("MYDB_PARTITION_CRASH_KIND").as_deref() == Ok("add") {
            "CREATE TABLE t(id INT PRIMARY KEY, payload TEXT, INDEX ip(payload(8))) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10))"
        } else { "CREATE TABLE t(id INT PRIMARY KEY, payload TEXT, INDEX ip(payload(8))) PARTITION BY HASH(id) PARTITIONS 2" }).unwrap();
    engine
        .execute("INSERT INTO t VALUES(1,REPEAT('a',9000)),(2,REPEAT('b',9000))")
        .unwrap();
    engine
        .execute(
            if std::env::var("MYDB_PARTITION_CRASH_KIND").as_deref() == Ok("add") {
                "ALTER TABLE t ADD PARTITION(PARTITION p1 VALUES LESS THAN(20)), LOCK=NONE"
            } else {
                "ALTER TABLE t ADD PARTITION PARTITIONS 2, LOCK=NONE"
            },
        )
        .unwrap();
    panic!("crash injection was not reached");
}

#[test]
fn partition_online_reorg_recovers_after_abrupt_process_exit() {
    for point in ["abort_before_swap", "abort_after_swap"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::partitioning::partition_ddl_process_crash_child",
                "--nocapture",
            ])
            .env("MYDB_PARTITION_CRASH_CHILD_DIR", dir.path())
            .env("MYDB_PARTITION_REORG_CRASH_POINT", point)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(86),
            "{}",
            std::fs::read_to_string(dir.path().join("child.log")).unwrap()
        );
        let engine = open_partition_recovery_test_engine(dir.path());
        assert_eq!(
            partition_names(&engine, "t").len(),
            if point == "abort_before_swap" { 2 } else { 4 }
        );
        assert_eq!(
            partition_test_rows(&engine, "SELECT id,LENGTH(payload) FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(9000)],
                vec![Value::Int(2), Value::Int(9000)]
            ]
        );
        engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
        engine
            .execute("INSERT INTO t VALUES(3,'recovered')")
            .unwrap();
    }
}

#[test]
fn partition_deferred_cleanup_survives_followup_topology_changes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine.database().set_partition_cleanup_background(true);
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(2),(3)").unwrap();
    for sql in [
        "ALTER TABLE t ADD PARTITION PARTITIONS 2, LOCK=NONE",
        "ALTER TABLE t REMOVE PARTITIONING, LOCK=NONE",
        "ALTER TABLE t PARTITION BY HASH(id) PARTITIONS 3",
        "ALTER TABLE t REMOVE PARTITIONING, LOCK=NONE",
    ] {
        engine.execute(sql).unwrap();
    }
    assert_eq!(engine.database().partition_ddl_job_snapshots().len(), 4);
    assert_eq!(
        engine
            .database()
            .maintain_partition_cleanup_until(|| false)
            .unwrap(),
        4
    );
    assert!(engine.database().partition_ddl_job_snapshots().is_empty());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)]
        ]
    );
    drop(engine);
    let engine = open_partition_recovery_test_engine(dir.path());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t ORDER BY id").len(),
        3
    );
}

#[test]
fn partition_deferred_cleanup_preserves_exchanged_target_after_table_drop() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine.database().set_partition_cleanup_background(true);
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10), PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    engine
        .execute("CREATE TABLE stage(id INT PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1),(11)").unwrap();
    engine.execute("INSERT INTO stage VALUES(4)").unwrap();
    engine
        .execute("ALTER TABLE t REBUILD PARTITION p0, LOCK=NONE")
        .unwrap();
    engine
        .execute("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE stage")
        .unwrap();
    engine.execute("DROP TABLE t").unwrap();
    assert_eq!(
        engine
            .database()
            .maintain_partition_cleanup_until(|| false)
            .unwrap(),
        1
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM stage"),
        vec![vec![Value::Int(1)]]
    );
    drop(engine);
    let engine = open_partition_recovery_test_engine(dir.path());
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM stage"),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn partition_check_spills_and_maintenance_releases_memory() {
    use crate::engine::executor::mem_budget;
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id INT PRIMARY KEY, s VARCHAR(300), INDEX ix(s)) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
    insert_fixture_rows(
        &engine,
        "test.t",
        (0..12_000i64).map(|i| {
            vec![
                Value::Int(i),
                Value::String(format!("{}{i}", "x".repeat(250))),
            ]
        }),
    );
    struct ResetBudget(usize);
    impl Drop for ResetBudget {
        fn drop(&mut self) {
            let _ = mem_budget::set_query_mem_limit_mb(self.0);
        }
    }
    let _restore = ResetBudget(mem_budget::query_mem_limit_bytes() / (1024 * 1024));
    mem_budget::set_query_mem_limit_mb(8).unwrap();
    let before = engine.database().storage_status().spill_bytes_written;
    engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    assert!(
        engine.database().storage_status().spill_bytes_written > before,
        "CHECK must use governed external runs"
    );
    engine
        .execute("ALTER TABLE t ANALYZE PARTITION ALL")
        .unwrap();
    engine
        .execute("ALTER TABLE t REPAIR PARTITION ALL")
        .unwrap();
    engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(12000)]]
    );
    assert_eq!(mem_budget::controlled_memory_snapshot().2[5], 0);
    assert_eq!(mem_budget::controlled_memory_snapshot().2[6], 0);
}

#[test]
fn partition_online_vectors_replay_after_private_graph_build() {
    for backend in ["memory", "paged"] {
        let _storage = set_part_env("MYDB_VECTOR_STORAGE", backend);
        let _brute = set_part_env("MYDB_HNSW_BRUTE_THRESHOLD", "0");
        let _filter = set_part_env("MYDB_HNSW_BRUTE_FILTER_ROWS", "0");
        for algo in ["HNSW", "IVF"] {
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = std::sync::Arc::new(open_partition_recovery_test_engine(dir.path()));
            engine.execute("CREATE TABLE t(id INT PRIMARY KEY, emb VECTOR(3) NOT NULL) PARTITION BY HASH(id) PARTITIONS 2").unwrap();
            // Setup precedes vector-index creation; tail transactions below
            // retain their original boundaries and recovery assertions.
            let values = (1..=32)
                .map(|i| format!("({i},'[{i},0,0]')"))
                .collect::<Vec<_>>()
                .join(",");
            engine
                .execute(&format!("INSERT INTO t VALUES {values}"))
                .unwrap();
            engine
                .execute(&format!("CREATE INDEX emb_idx ON t(emb) USING {algo}"))
                .unwrap();
            let build = engine.database().install_ddl_backfill_test_hook();
            let worker = engine.clone();
            let ddl = std::thread::spawn(move || {
                let mut session = SessionContext::default();
                session.connection_id = 8801;
                worker
                    .execute_with_session(&mut session, "ALTER TABLE t ADD PARTITION PARTITIONS 2")
            });
            let reached = build.wait_until_reached(Duration::from_secs(10));
            let tail = engine.database().install_ddl_backfill_test_hook();
            build.release();
            let caught_up = tail.wait_until_reached(Duration::from_secs(10));
            let mut writer = SessionContext::default();
            writer.connection_id = 8802;
            writer.sysvars.lock_wait_timeout = Some(0);
            let writes = [
                "UPDATE t SET emb='[100,0,0]' WHERE id=1",
                "DELETE FROM t WHERE id=2",
                "INSERT INTO t VALUES(33,'[0,0,0]')",
            ]
            .map(|sql| engine.execute_with_session(&mut writer, sql));
            tail.release();
            let result = ddl.join().unwrap();
            assert!(reached && caught_up, "{backend}/{algo}: {result:?}");
            for write in writes {
                write.unwrap();
            }
            result.unwrap();
            let query = "SELECT id FROM t ORDER BY DISTANCE(emb,'[0,0,0]') LIMIT 1";
            assert_eq!(
                partition_test_rows(&engine, query),
                vec![vec![Value::Int(33)]],
                "{backend}/{algo}"
            );
            drop(engine);
            let engine = open_partition_recovery_test_engine(dir.path());
            assert_eq!(
                partition_test_rows(&engine, query),
                vec![vec![Value::Int(33)]],
                "reopen {backend}/{algo}"
            );
        }
    }
}

#[test]
fn partition_online_reorg_pins_snapshot_against_zero_retention_purge() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let mut config = memory_test_storage_config();
    config.undo_retention = Duration::ZERO;
    let engine = std::sync::Arc::new(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .unwrap(),
    );
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,0),(2,0)").unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8901;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PARTITION PARTITIONS 2, LOCK=NONE",
        )
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 8902;
    writer.sysvars.lock_wait_timeout = Some(0);
    let mut errors = Vec::new();
    for _ in 0..50 {
        if let Err(err) = engine.execute_with_session(&mut writer, "UPDATE t SET v=v+1 WHERE id=1")
        {
            errors.push(err);
        }
        engine.database().purge_obsolete_undo();
    }
    hook.release();
    let result = ddl.join().unwrap();
    assert!(reached, "{result:?}");
    assert!(errors.is_empty(), "{errors:?}");
    result.unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id,v FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(50)],
            vec![Value::Int(2), Value::Int(0)]
        ]
    );
    assert_eq!(engine.database().active_snapshot_count(), 0);
}

#[test]
fn partition_parallel_index_finish_matches_serial_and_releases_workers() {
    for degree in ["1", "4"] {
        let _degree = set_part_env("MYDB_PARALLEL_DEGREE", degree);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_partition_recovery_test_engine(dir.path());
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY,v INT,s VARCHAR(40),INDEX iv(v,id),INDEX ip(s(8)),INDEX ie((LOWER(s)))) PARTITION BY HASH(id) PARTITIONS 4").unwrap();
        let values = (0..500)
            .map(|id| format!("({id},{},'Text{id}')", id % 13))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES{values}"))
            .unwrap();
        let before = partition_test_rows(&engine, "SELECT id,v,s FROM t ORDER BY id");
        for mode in ["SHARED", "NONE"] {
            engine
                .execute(&format!("ALTER TABLE t REBUILD PARTITION ALL, LOCK={mode}"))
                .unwrap();
            engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
            assert_eq!(
                partition_test_rows(&engine, "SELECT id,v,s FROM t ORDER BY id"),
                before
            );
            assert_eq!(
                engine.database().partition_index_finish_workers_for_test(),
                0
            );
        }
    }
}

#[test]
fn partition_parallel_index_failure_preserves_source() {
    let _degree = set_part_env("MYDB_PARALLEL_DEGREE", "4");
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_recovery_test_engine(dir.path());
    engine.execute("CREATE TABLE t(id VARCHAR(20) PRIMARY KEY,v INT,INDEX iv(v)) PARTITION BY KEY(id) PARTITIONS 2").unwrap();
    engine
        .execute("INSERT INTO t VALUES('01',1),('1',2)")
        .unwrap();
    let before = partition_test_rows(&engine, "SELECT id,v FROM t ORDER BY id");
    assert_eq!(
        engine
            .execute("ALTER TABLE t MODIFY id INT, LOCK=NONE")
            .unwrap_err()
            .code(),
        1062
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT id,v FROM t ORDER BY id"),
        before
    );
    assert_eq!(
        engine.database().partition_index_finish_workers_for_test(),
        0
    );
    engine.execute("INSERT INTO t VALUES('2',3)").unwrap();
}

#[test]
fn partition_online_prepare_includes_preexisting_writer_commit() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY,v INT) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1,0)").unwrap();
    let mut writer = SessionContext::default();
    writer.connection_id = 8951;
    engine.execute_with_session(&mut writer, "BEGIN").unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE t SET v=99 WHERE id=1")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 8952;
        worker.execute_with_session(
            &mut session,
            "ALTER TABLE t ADD PARTITION PARTITIONS 2, LOCK=NONE",
        )
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut registered = false;
    while std::time::Instant::now() < deadline {
        if !engine.database().alter_copy_job_snapshots().is_empty() {
            registered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    hook.release();
    let result = ddl.join().unwrap();
    assert!(registered && reached, "{result:?}");
    result.unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT id,v FROM t"),
        vec![vec![Value::Int(1), Value::Int(99)]]
    );
}

#[test]
fn partition_compiled_select_revalidates_definition_under_mdl() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(1)").unwrap();
    let mut session = SessionContext::default();
    session.connection_id = 9903;
    let parsed = std::sync::Arc::new(
        crate::parser::parse_statement("SELECT id FROM test.t WHERE id=1").unwrap(),
    );
    let memo = super::super::compiled_select::compile_prepared_select(
        &engine.db,
        &session,
        &parsed,
        &[],
        engine.plan_cache(),
    )
    .unwrap();
    engine
        .execute("ALTER TABLE t REBUILD PARTITION ALL")
        .unwrap();
    let (txn, _snapshot) = engine
        .db
        .read_only_statement_view_with_guard(crate::storage::IsolationLevel::ReadCommitted, None);
    let result = super::super::compiled_select::run_compiled_select(
        &engine.db,
        &session,
        &memo,
        txn,
        session.connection_id as u64,
    );
    assert_eq!(result.unwrap_err().code(), 1615);
    engine.execute("DROP TABLE t").unwrap();
}

#[test]
fn partition_streaming_reader_survives_concurrent_rebuild() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT,v INT) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    for batch in 0..20 {
        let values = (batch * 1000..(batch + 1) * 1000)
            .map(|id| format!("({id},{})", id * 3))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES{values}"))
            .unwrap();
    }
    let mut reader = SessionContext::default();
    reader.connection_id = 9001;
    let response = engine
        .execute_streaming_with_session_for_test(&mut reader, "SELECT id,v FROM t")
        .unwrap();
    assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
    let worker = engine.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 9002;
        session.sysvars.lock_wait_timeout = Some(5);
        tx.send(worker.execute_with_session(&mut session, "ALTER TABLE t REBUILD PARTITION ALL"))
            .unwrap();
    });
    let early = rx.recv_timeout(Duration::from_millis(300));
    let follow_up = engine
        .execute_with_session(&mut reader, "SELECT COUNT(*) FROM t")
        .and_then(QueryResponse::materialize);
    let rows = response.materialize();
    let result = early.unwrap_or_else(|_| rx.recv_timeout(Duration::from_secs(10)).unwrap());
    ddl.join().unwrap();
    result.unwrap();
    follow_up.expect("an existing cursor owner must not wait behind its own queued DDL waiter");
    let QueryResponse::ResultSet(rows) = rows.unwrap() else {
        panic!("rows")
    };
    let mut ids = rows
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int(id) => id,
            ref value => panic!("{value:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids.len(),
        20000,
        "DDL must not remove a streaming producer's unread leaves"
    );
    assert!(
        ids.into_iter().eq(0..20000),
        "streamed row identities must be preserved"
    );
}

#[test]
fn partition_join_stream_mdl_releases_on_early_close() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT,v INT) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    engine
        .execute("CREATE TABLE a(id INT PRIMARY KEY)")
        .unwrap();
    for batch in 0..10 {
        let values = (batch * 1000..(batch + 1) * 1000)
            .map(|id| format!("({id},{id})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES{values}"))
            .unwrap();
        let values = (batch * 1000..(batch + 1) * 1000)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO a VALUES{values}"))
            .unwrap();
    }
    let mut reader = SessionContext::default();
    reader.connection_id = 9101;
    let response = engine
        .execute_streaming_with_session_for_test(
            &mut reader,
            "SELECT t.id FROM t JOIN a ON t.id=a.id",
        )
        .unwrap();
    assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
    assert!(engine
        .database()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.source == "sql/streaming-result" && lock.owner_thread_id == Some(9101)));
    drop(response);
    assert!(!engine
        .database()
        .metadata_lock_snapshot()
        .iter()
        .any(|lock| lock.source == "sql/streaming-result"));
    engine
        .execute("ALTER TABLE t REBUILD PARTITION ALL")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Int(10000)]]
    );
    let internal = crate::storage::mdl::allocate_internal_owner().unwrap();
    assert!(internal > u32::MAX as u64 && internal < (1u64 << 63));
}

#[test]
fn partition_streaming_reader_retains_mdl_after_transaction_commit() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_partition_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE t(id INT,v INT) PARTITION BY HASH(id) PARTITIONS 4")
        .unwrap();
    for batch in 0..20 {
        let values = (batch * 1000..(batch + 1) * 1000)
            .map(|id| format!("({id},{})", id * 3))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO t VALUES{values}"))
            .unwrap();
    }
    let mut reader = SessionContext::default();
    reader.connection_id = 9001;
    engine.execute_with_session(&mut reader, "BEGIN").unwrap();
    let response = engine
        .execute_streaming_with_session_for_test(&mut reader, "SELECT id,v FROM t")
        .unwrap();
    assert!(matches!(response, QueryResponse::StreamingResultSet(_)));
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    let worker = engine.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 9002;
        session.sysvars.lock_wait_timeout = Some(5);
        tx.send(worker.execute_with_session(&mut session, "ALTER TABLE t REBUILD PARTITION ALL"))
            .unwrap();
    });
    let early = rx.recv_timeout(Duration::from_millis(300));
    let rows = response.materialize();
    let result = early.unwrap_or_else(|_| rx.recv_timeout(Duration::from_secs(10)).unwrap());
    ddl.join().unwrap();
    result.unwrap();
    let QueryResponse::ResultSet(rows) = rows.unwrap() else {
        panic!("rows")
    };
    let mut ids = rows
        .rows
        .iter()
        .map(|row| match row[0] {
            Value::Int(id) => id,
            ref value => panic!("{value:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids.len(),
        20000,
        "DDL must not remove a streaming producer's unread leaves"
    );
    assert!(
        ids.into_iter().eq(0..20000),
        "streamed row identities must be preserved"
    );
}

#[test]
fn partition_add_failure_is_recoverable_and_rejects_followup_writes() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    {
        let engine = open_partition_recovery_test_engine(dir.path());
        engine.execute("CREATE TABLE t(id INT PRIMARY KEY) PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN(10))").unwrap();
        engine.execute("INSERT INTO t VALUES(1)").unwrap();
        engine.database().fail_next_catalog_commit_for_test();
        assert!(engine
            .execute("ALTER TABLE t ADD PARTITION(PARTITION p1 VALUES LESS THAN(20))")
            .is_err());
        assert!(
            engine.database().ensure_operational().is_err(),
            "failed ADD publication must not accept writes into an uncommitted partition"
        );
    }
    let engine = open_partition_recovery_test_engine(dir.path());
    assert_eq!(partition_names(&engine, "t"), vec!["p0"]);
    assert_eq!(
        partition_test_rows(&engine, "SELECT id FROM t"),
        vec![vec![Value::Int(1)]]
    );
    engine
        .execute("ALTER TABLE t ADD PARTITION(PARTITION p1 VALUES LESS THAN(20))")
        .unwrap();
    engine.execute("INSERT INTO t VALUES(11)").unwrap();
}

#[test]
fn partition_add_recovers_after_abrupt_process_exit() {
    for point in ["abort_before_swap", "abort_after_swap"] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let log = std::fs::File::create(dir.path().join("child.log")).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::partitioning::partition_ddl_process_crash_child",
                "--nocapture",
            ])
            .env("MYDB_PARTITION_CRASH_CHILD_DIR", dir.path())
            .env("MYDB_PARTITION_CRASH_KIND", "add")
            .env("MYDB_PARTITION_REORG_CRASH_POINT", point)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(86),
            "{}",
            std::fs::read_to_string(dir.path().join("child.log")).unwrap()
        );
        let engine = open_partition_recovery_test_engine(dir.path());
        assert_eq!(
            partition_names(&engine, "t").len(),
            if point == "abort_before_swap" { 1 } else { 2 }
        );
        assert_eq!(
            partition_test_rows(&engine, "SELECT id,LENGTH(payload) FROM t ORDER BY id"),
            vec![
                vec![Value::Int(1), Value::Int(9000)],
                vec![Value::Int(2), Value::Int(9000)]
            ]
        );
        engine.execute("ALTER TABLE t CHECK PARTITION ALL").unwrap();
        engine
            .execute("INSERT INTO t VALUES(3,'recovered')")
            .unwrap();
    }
}

#[test]
fn partition_audit_partial_prefix_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE audit_prefix(a INT,b VARCHAR(8),c INT,v INT, PRIMARY KEY(a,b,c)) PARTITION BY RANGE COLUMNS(a,b,c) (PARTITION p0 VALUES LESS THAN(1,'m',5),PARTITION p1 VALUES LESS THAN(5,'m',5),PARTITION p2 VALUES LESS THAN(5,'z',5),PARTITION p3 VALUES LESS THAN(10,'m',5),PARTITION p4 VALUES LESS THAN(MAXVALUE,MAXVALUE,MAXVALUE))").unwrap();
    engine
        .execute("INSERT INTO audit_prefix VALUES(5,'a',1,0),(5,'m',9,0),(5,'z',9,0),(9,'a',1,0)")
        .unwrap();
    for predicate in [
        "a=5",
        "a=5 AND c=9",
        "a=5 AND b='m'",
        "a=5 AND b='z' AND c=9",
    ] {
        let actual = partition_test_rows(
            &engine,
            &format!("SELECT b,c FROM audit_prefix WHERE {predicate} ORDER BY b,c"),
        );
        let expected = match predicate {
            "a=5" => 3,
            "a=5 AND c=9" => 2,
            _ => 1,
        };
        assert_eq!(actual.len(), expected, "{predicate}");
    }
    assert_eq!(
        partition_test_rows(
            &engine,
            "SELECT b FROM audit_prefix PARTITION(p3) WHERE a=5"
        )
        .len(),
        1
    );
    engine
        .execute("UPDATE audit_prefix SET v=v+1 WHERE a=5")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT v FROM audit_prefix WHERE v=1").len(),
        3
    );
    engine
        .execute("DELETE FROM audit_prefix WHERE a=5")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT a FROM audit_prefix"),
        vec![vec![Value::Int(9)]]
    );
}

#[test]
fn partition_audit_odku_unique_migration() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for key in ["PRIMARY KEY(a)", "UNIQUE KEY uk(a,b)"] {
        engine.execute(&format!("CREATE TABLE audit_unique(a INT,b INT,{key}) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)")).unwrap();
        engine
            .execute("INSERT INTO audit_unique VALUES(1,7),(100,7)")
            .unwrap();
        let err = engine
            .execute("INSERT INTO audit_unique VALUES(1,7) ON DUPLICATE KEY UPDATE a=100")
            .unwrap_err();
        assert_eq!(err.code(), 1062);
        assert_eq!(
            partition_test_rows(&engine, "SELECT a FROM audit_unique ORDER BY a"),
            vec![vec![Value::Int(1)], vec![Value::Int(100)]]
        );
        let mut session = SessionContext::default();
        engine.execute_with_session(&mut session, "BEGIN").unwrap();
        engine
            .execute_with_session(
                &mut session,
                "INSERT INTO audit_unique VALUES(1,7) ON DUPLICATE KEY UPDATE a=101",
            )
            .unwrap();
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        assert_eq!(
            partition_test_rows(&engine, "SELECT a FROM audit_unique ORDER BY a"),
            vec![vec![Value::Int(1)], vec![Value::Int(100)]]
        );
        engine.execute("DROP TABLE audit_unique").unwrap();
    }
}

#[test]
fn partition_audit_constant_bounds_and_comments() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    engine.execute("CREATE TABLE audit_func(d DATE) PARTITION BY RANGE(TO_DAYS(d)) (PARTITION p0 VALUES LESS THAN(TO_DAYS('2025-01-01')) COMMENT 'old')").unwrap();
    engine.execute("ALTER TABLE audit_func ADD PARTITION (PARTITION p1 VALUES LESS THAN(TO_DAYS('2026-01-01')) COMMENT = 'new')").unwrap();
    engine
        .execute("INSERT INTO audit_func VALUES('2024-12-31'),('2025-01-01')")
        .unwrap();
    assert_eq!(
        partition_test_rows(&engine, "SELECT d FROM audit_func PARTITION(p0)").len(),
        1
    );
    assert_eq!(
        partition_test_rows(&engine, "SELECT d FROM audit_func PARTITION(p1)").len(),
        1
    );
    for bound in [
        "TO_DAYS(d)",
        "RAND()",
        "ABS(TO_DAYS(d))",
        "UNIX_TIMESTAMP()",
    ] {
        assert!(engine.execute(&format!("CREATE TABLE audit_bad(d INT) PARTITION BY RANGE(d) (PARTITION p0 VALUES LESS THAN({bound}))")).is_err(),"{bound}");
    }
    engine.execute("CREATE TABLE audit_sub(a INT) PARTITION BY RANGE(a) SUBPARTITION BY HASH(a) (PARTITION p0 VALUES LESS THAN(ABS(-10)) COMMENT 'parent' (SUBPARTITION s0 COMMENT 'child',SUBPARTITION s1 COMMENT = 'child2'))").unwrap();
    for sql in [
        "ALTER TABLE audit_func ADD PARTITION",
        "ALTER TABLE audit_func ADD PARTITION ()",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1064);
    }
}

#[test]
fn partition_audit_typed_bounds_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (kind, bounds, values) in [
        ("BINARY(4)", "'abc'", "'abc'"),
        ("VARBINARY(4)", "'abc'", "'abc'"),
        ("DATE", "'2025-01-01'", "'2025-01-01'"),
        ("DATETIME", "'2025-01-01 12:34:56'", "'2025-01-01 12:34:56'"),
    ] {
        engine.execute(&format!("CREATE TABLE audit_typed(k {kind}) PARTITION BY LIST COLUMNS(k) (PARTITION p0 VALUES IN({bounds}))")).unwrap();
        engine
            .execute(&format!("INSERT INTO audit_typed VALUES({values})"))
            .unwrap();
        assert_eq!(
            partition_test_rows(&engine, "SELECT k FROM audit_typed PARTITION(p0)").len(),
            1
        );
        let rows = partition_test_rows(&engine, "SHOW CREATE TABLE audit_typed");
        let Value::String(ddl) = &rows[0][1] else {
            panic!("expected DDL")
        };
        let ddl = ddl.replace("`audit_typed`", "`audit_copy`");
        engine.execute(&ddl).unwrap();
        engine
            .execute(&format!("INSERT INTO audit_copy VALUES({values})"))
            .unwrap();
        engine.execute("DROP TABLE audit_typed,audit_copy").unwrap();
    }
    for kind in ["DATE", "DATETIME"] {
        for method in ["RANGE", "LIST"] {
            let clause = if method == "RANGE" { "LESS THAN" } else { "IN" };
            assert!(engine.execute(&format!("CREATE TABLE audit_bad(k {kind}) PARTITION BY {method} COLUMNS(k) (PARTITION p0 VALUES {clause}('2020-13-45'))")).is_err());
        }
    }
}

#[test]
fn partition_audit_signed_hash() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for (method, n) in [("HASH", 4), ("LINEAR HASH", 4), ("LINEAR HASH", 5)] {
        engine
            .execute(&format!(
                "CREATE TABLE audit_hash(a BIGINT) PARTITION BY {method}(a) PARTITIONS {n}"
            ))
            .unwrap();
        for v in [-1i64, -2, -7, i64::MIN, 0, 1, 17] {
            engine
                .execute(&format!("INSERT INTO audit_hash VALUES({v})"))
                .unwrap();
            let expected = if method == "HASH" {
                v.unsigned_abs() % n
            } else {
                let mask = n.next_power_of_two() - 1;
                let mut bucket = (v as u64) & mask;
                if bucket >= n {
                    bucket &= (mask + 1) / 2 - 1;
                }
                bucket
            };
            assert_eq!(
                partition_test_rows(
                    &engine,
                    &format!("SELECT a FROM audit_hash PARTITION(p{expected}) WHERE a={v}")
                )
                .len(),
                1,
                "{method} {n} {v}"
            );
        }
        engine.execute("DROP TABLE audit_hash").unwrap();
    }
}

#[test]
fn partition_audit_maintenance_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_partition_test_engine(dir.path());
    for analyzed in [false, true] {
        for (operation, lock) in [
            ("REBUILD", "SHARED"),
            ("REBUILD", "NONE"),
            ("OPTIMIZE", "SHARED"),
            ("OPTIMIZE", "NONE"),
            ("REPAIR", "SHARED"),
            ("TRUNCATE", "SHARED"),
        ] {
            engine.execute("CREATE TABLE audit_stats(a INT) PARTITION BY RANGE(a) (PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
            engine
                .execute("INSERT INTO audit_stats VALUES(1),(2),(20)")
                .unwrap();
            if analyzed {
                engine.execute("ANALYZE TABLE audit_stats").unwrap();
            }
            engine
                .execute("INSERT INTO audit_stats VALUES(21)")
                .unwrap();
            engine
                .execute(&format!(
                    "ALTER TABLE audit_stats {operation} PARTITION p0, LOCK={lock}"
                ))
                .unwrap();
            let rows=partition_test_rows(&engine,"SELECT TABLE_ROWS FROM information_schema.PARTITIONS WHERE TABLE_NAME='audit_stats' ORDER BY PARTITION_NAME");
            let counts: Vec<_> = rows.iter().map(|r| r[0].to_display_string()).collect();
            assert_eq!(
                counts,
                vec![
                    if operation == "TRUNCATE" { "0" } else { "2" }.to_string(),
                    "2".to_string()
                ],
                "{operation}, analyzed={analyzed}"
            );
            engine.execute("DROP TABLE audit_stats").unwrap();
        }
    }
}
