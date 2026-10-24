use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap() else {
        panic!("expected rows for {sql}");
    };
    rs.rows
}

#[test]
fn load_escape_null_marker_preserves_surrounding_bytes_on_all_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let file = dir.path().join("escaped.tsv");
    std::fs::write(&file, b"1|lead\\Ny\n2|tail\\N\n3|\\N\n4|\\\\N\n5|\\t\n").unwrap();
    for suffix in ["", " SET id=id", " (id,@s) SET s=@s"] {
        engine.execute("DROP TABLE IF EXISTS t").unwrap();
        engine
            .execute("CREATE TABLE t (id INT, s VARCHAR(64))")
            .unwrap();
        engine
            .execute(&format!(
                "LOAD DATA INFILE '{}' INTO TABLE t FIELDS TERMINATED BY '|'{suffix}",
                crate::util::file::sql_path(&file)
            ))
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT s FROM t ORDER BY id"),
            vec![
                vec![Value::String("leadNy".into())],
                vec![Value::String("tailN".into())],
                vec![Value::Null],
                vec![Value::String("\\N".into())],
                vec![Value::String("\t".into())]
            ],
            "{suffix}"
        );
    }
}

#[test]
fn load_ignore_lines_respects_overlapping_and_quoted_record_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (s VARCHAR(64))").unwrap();
    for (bytes, options) in [
        (
            b"h\r\r\nA\r\nB\r\n".as_slice(),
            "LINES TERMINATED BY '\\r\\n'",
        ),
        (
            b"habaababAababBabab".as_slice(),
            "LINES TERMINATED BY 'abab'",
        ),
        (
            b"\"h\nheader\"\nA\nB\n".as_slice(),
            "FIELDS ENCLOSED BY '\"'",
        ),
    ] {
        let file = dir.path().join("lines.txt");
        std::fs::write(&file, bytes).unwrap();
        engine.execute("DELETE FROM t").unwrap();
        engine
            .execute(&format!(
                "LOAD DATA INFILE '{}' INTO TABLE t {options} IGNORE 1 LINES",
                crate::util::file::sql_path(&file)
            ))
            .unwrap();
        assert_eq!(
            rows(&engine, "SELECT s FROM t ORDER BY s"),
            vec![
                vec![Value::String("A".into())],
                vec![Value::String("B".into())]
            ],
            "{options}"
        );
    }
}

#[test]
fn fk_parent_update_uses_collation_equivalence_and_composite_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for (n, collation, action) in [
        (1, "utf8mb4_0900_ai_ci", "RESTRICT"),
        (2, "utf8mb4_0900_ai_ci", "CASCADE"),
        (3, "utf8mb4_bin", "RESTRICT"),
    ] {
        engine
            .execute(&format!(
                "CREATE TABLE p{n} (k VARCHAR(20), n INT, PRIMARY KEY(k,n)) COLLATE {collation}"
            ))
            .unwrap();
        engine.execute(&format!("CREATE TABLE c{n} (id INT PRIMARY KEY, k VARCHAR(20), n INT, FOREIGN KEY(k,n) REFERENCES p{n}(k,n) ON UPDATE {action}) COLLATE {collation}")).unwrap();
        engine
            .execute(&format!("INSERT INTO p{n} VALUES ('café',1)"))
            .unwrap();
        engine
            .execute(&format!("INSERT INTO c{n} VALUES (1,'café',1)"))
            .unwrap();
        let result = engine.execute(&format!("UPDATE p{n} SET k='CAFE'"));
        if n == 3 {
            assert_eq!(result.unwrap_err().code(), 1451);
        } else {
            result.unwrap();
            assert_eq!(
                rows(&engine, &format!("SELECT k FROM c{n}")),
                vec![vec![Value::String("café".into())]]
            );
            let changed = engine.execute(&format!("UPDATE p{n} SET n=2"));
            if n == 1 {
                assert_eq!(changed.unwrap_err().code(), 1451);
            } else {
                changed.unwrap();
                assert_eq!(rows(&engine, "SELECT n FROM c2"), vec![vec![Value::Int(2)]]);
            }
        }
    }
}

#[test]
fn load_replace_runs_delete_only_triggers_on_fast_and_general_paths() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    engine
        .execute("CREATE TABLE log (timing INT,v INT)")
        .unwrap();
    engine
        .execute(
            "CREATE TRIGGER bd BEFORE DELETE ON t FOR EACH ROW INSERT INTO log VALUES (1,OLD.v)",
        )
        .unwrap();
    engine
        .execute(
            "CREATE TRIGGER ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log VALUES (2,OLD.v)",
        )
        .unwrap();
    let file = dir.path().join("replace.tsv");
    std::fs::write(&file, b"1\t99\n").unwrap();
    let mut session = SessionContext::default();
    for suffix in ["", " SET v=v"] {
        engine.execute("DELETE FROM t").unwrap();
        engine.execute("DELETE FROM log").unwrap();
        engine.execute("INSERT INTO t VALUES (1,5)").unwrap();
        assert_eq!(
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                        "LOAD DATA INFILE '{}' REPLACE INTO TABLE t{suffix}",
                        crate::util::file::sql_path(&file)
                    )
                )
                .unwrap(),
            QueryResponse::OkAffected { affected_rows: 2 }
        );
        assert_eq!(
            session.pending_statement_info.as_deref(),
            Some("Records: 1 Deleted: 1 Skipped: 0 Warnings: 0")
        );
        assert_eq!(
            rows(&engine, "SELECT timing,v FROM log ORDER BY timing"),
            vec![
                vec![Value::Int(1), Value::Int(5)],
                vec![Value::Int(2), Value::Int(5)]
            ]
        );
    }
}

#[test]
fn multi_delete_ignore_warns_and_preserves_referenced_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE p (id INT PRIMARY KEY)",
        "CREATE TABLE j (id INT PRIMARY KEY)",
        "CREATE TABLE c (id INT PRIMARY KEY, pid INT, FOREIGN KEY(pid) REFERENCES p(id))",
        "INSERT INTO p VALUES (1),(2)",
        "INSERT INTO j VALUES (1),(2)",
        "INSERT INTO c VALUES (1,1)",
    ] {
        engine.execute(sql).unwrap();
    }
    let mut session = SessionContext::default();
    assert_eq!(
        engine
            .execute_with_session(&mut session, "DELETE IGNORE p FROM p JOIN j ON p.id=j.id")
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 1 }
    );
    assert_eq!(session.warning_count(), 1);
    assert_eq!(rows(&engine, "SELECT id FROM p"), vec![vec![Value::Int(1)]]);
    engine.execute("INSERT INTO p VALUES (2)").unwrap();
    assert_eq!(
        engine
            .execute("DELETE p FROM p JOIN j ON p.id=j.id")
            .unwrap_err()
            .code(),
        1451
    );
    assert_eq!(
        rows(&engine, "SELECT COUNT(*) FROM p"),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn multi_dml_rejects_forward_on_columns_before_writing_even_on_empty_tables() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY,v INT)",
        "CREATE TABLE b (id INT PRIMARY KEY)",
        "CREATE TABLE c (future INT)",
        "INSERT INTO a VALUES (1,0)",
        "INSERT INTO b VALUES (1)",
    ] {
        engine.execute(sql).unwrap();
    }
    for sql in [
        "UPDATE a JOIN b ON a.id=c.future JOIN c ON b.id=c.future SET a.v=99",
        "DELETE a FROM a JOIN b ON a.id=c.future JOIN c ON b.id=c.future",
        "UPDATE a JOIN b ON a.id=COALESCE(future,0) JOIN c ON b.id=c.future SET a.v=99",
    ] {
        assert_eq!(engine.execute(sql).unwrap_err().code(), 1054, "{sql}");
    }
    assert_eq!(rows(&engine, "SELECT v FROM a"), vec![vec![Value::Int(0)]]);
    engine.execute("INSERT INTO c VALUES (1)").unwrap();
    engine
        .execute("UPDATE a JOIN b ON a.id=b.id JOIN c ON b.id=c.future SET a.v=7")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT v FROM a"), vec![vec![Value::Int(7)]]);
}

#[test]
fn load_rejects_generated_column_targets_and_accepts_explicit_discard() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (a INT, g INT AS (a+1) STORED)")
        .unwrap();
    let file = dir.path().join("generated.tsv");
    std::fs::write(&file, b"5\t123\n").unwrap();
    for suffix in [" (a,g)", " (a,g) SET a=a", " (a,@v) SET g=@v"] {
        assert_eq!(
            engine
                .execute(&format!(
                    "LOAD DATA INFILE '{}' INTO TABLE t{suffix}",
                    crate::util::file::sql_path(&file)
                ))
                .unwrap_err()
                .code(),
            3105,
            "{suffix}"
        );
        assert!(rows(&engine, "SELECT * FROM t").is_empty());
    }
    engine
        .execute(&format!(
            "LOAD DATA INFILE '{}' INTO TABLE t (a,@discard)",
            crate::util::file::sql_path(&file)
        ))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT * FROM t"),
        vec![vec![Value::Int(5), Value::Int(6)]]
    );
}

#[test]
fn returning_column_names_preserve_image_qualifiers_and_explicit_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,5)").unwrap();
    let QueryResponse::ResultSet(rs) = engine
        .execute("UPDATE t SET v=v+1 RETURNING old.v, new.v, old.v AS prior, id")
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        rs.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["old.v", "new.v", "prior", "id"]
    );
    assert_eq!(
        rs.columns
            .iter()
            .map(|c| c.source.org_name.as_str())
            .collect::<Vec<_>>(),
        vec!["v", "v", "v", "id"]
    );
    assert_eq!(
        rs.rows,
        vec![vec![
            Value::Int(5),
            Value::Int(6),
            Value::Int(5),
            Value::Int(1)
        ]]
    );
}

#[test]
fn self_join_multi_update_writes_each_physical_row_once_in_table_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("CREATE TABLE log (v INT)").unwrap();
    engine
        .execute("CREATE TRIGGER au AFTER UPDATE ON t FOR EACH ROW INSERT INTO log VALUES (NEW.v)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,0)").unwrap();
    for _ in 0..4 {
        engine.execute("DELETE FROM log").unwrap();
        assert_eq!(
            engine
                .execute("UPDATE t AS a JOIN t AS b ON a.id=b.id SET a.v=a.v+1,b.v=b.v+10")
                .unwrap(),
            QueryResponse::OkAffected { affected_rows: 1 }
        );
        assert_eq!(
            rows(&engine, "SELECT COUNT(*) FROM log"),
            vec![vec![Value::Int(1)]]
        );
    }
    assert_eq!(rows(&engine, "SELECT v FROM t"), vec![vec![Value::Int(4)]]);
}

#[test]
fn odku_ok_id_reports_updated_auto_increment_key_and_insert_takes_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY,u INT UNIQUE,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (7,1,0)").unwrap();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (u,v) VALUES (1,5) ON DUPLICATE KEY UPDATE v=VALUES(v)",
        )
        .unwrap();
    assert_eq!(session.statement_insert_id, 7);
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (u,v) VALUES (1,6) ON DUPLICATE KEY UPDATE id=11,v=VALUES(v)",
        )
        .unwrap();
    assert_eq!(session.statement_insert_id, 11);
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (u,v) VALUES (1,6) ON DUPLICATE KEY UPDATE v=VALUES(v)",
        )
        .unwrap();
    assert_eq!(
        session.statement_insert_id, 0,
        "a matched no-op has no successful write"
    );
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (u,v) VALUES (1,9),(2,9) ON DUPLICATE KEY UPDATE v=VALUES(v)",
        )
        .unwrap();
    let inserted = rows(&engine, "SELECT id FROM t WHERE u=2")[0][0]
        .to_display_string()
        .parse::<u64>()
        .unwrap();
    // SELECT clears the statement's OK metadata, so inspect the session result first below.
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (u,v) VALUES (1,10),(3,10) ON DUPLICATE KEY UPDATE v=VALUES(v)",
        )
        .unwrap();
    let ok_id = session.statement_insert_id;
    assert!(ok_id > inserted);
    assert_eq!(
        rows(&engine, "SELECT id FROM t WHERE u=3")[0][0]
            .to_display_string()
            .parse::<u64>()
            .unwrap(),
        ok_id
    );
    engine
        .execute("CREATE TABLE large (id INT AUTO_INCREMENT PRIMARY KEY,u INT UNIQUE,v INT)")
        .unwrap();
    let values = (1..=130)
        .map(|i| format!("({i},{i},0)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO large VALUES {values}"))
        .unwrap();
    let values = (1..=130)
        .map(|i| format!("({i},{i},1)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute_with_session(
            &mut session,
            &format!("INSERT INTO large VALUES {values} ON DUPLICATE KEY UPDATE v=VALUES(v)"),
        )
        .unwrap();
    assert_eq!(session.statement_insert_id, 130);
    for (table, data_type, id, expected) in [
        ("signed_key", "BIGINT", "-7", u64::MAX - 6),
        (
            "unsigned_key",
            "BIGINT UNSIGNED",
            "18446744073709551615",
            u64::MAX,
        ),
    ] {
        engine.execute(&format!("CREATE TABLE {table} (id {data_type} AUTO_INCREMENT PRIMARY KEY,u INT UNIQUE,v INT)")).unwrap();
        engine
            .execute(&format!("INSERT INTO {table} VALUES ({id},1,0)"))
            .unwrap();
        engine
            .execute_with_session(
                &mut session,
                &format!(
                    "INSERT INTO {table} VALUES ({id},1,1) ON DUPLICATE KEY UPDATE v=VALUES(v)"
                ),
            )
            .unwrap();
        assert_eq!(session.statement_insert_id, expected);
    }
}

#[test]
fn on_update_datetime_uses_declared_precision_in_text_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (id INT PRIMARY KEY,d DATETIME(3) ON UPDATE CURRENT_TIMESTAMP(3),v INT)").unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,'2020-01-01 00:00:00.000',0)")
        .unwrap();
    engine.execute("UPDATE t SET v=1").unwrap();
    let QueryResponse::ResultSet(rs) = engine.execute("SELECT d FROM t").unwrap() else {
        panic!()
    };
    assert_eq!(rs.columns[0].source.decimals, Some(3));
    let Value::DateTime(micros) = rs.rows[0][0] else {
        panic!("expected native datetime")
    };
    assert_eq!(micros % 1000, 0);
    let mut packet = Vec::new();
    crate::mysql::protocol::text_row_packet_into_with_columns(
        &rs.rows[0],
        &mut packet,
        None,
        Some(&rs.columns),
    );
    assert_eq!(packet[0], 23);
    assert_eq!(packet.len(), 24);
}

#[test]
fn homogeneous_prepared_batch_rechecks_schema_under_mdl_before_any_write() {
    use crate::engine::api::PreparedDmlState;
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,v INT)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1,0),(2,0)").unwrap();
    let mut session = SessionContext::default();
    let sql = "UPDATE t SET v=? WHERE id=?";
    let parsed = engine.prepare_cached(&mut session, sql, 2).unwrap();
    let mut state = PreparedDmlState::default();
    engine
        .execute_prepared_dml(
            &mut session,
            &mut state,
            &parsed,
            sql,
            &[Value::Int(0), Value::Int(1)],
        )
        .unwrap()
        .unwrap();
    let PreparedDmlState::Ready(plan) = state else {
        panic!()
    };
    // Model publication after optimistic validation but before the MDL grant.
    engine
        .execute("ALTER TABLE t ADD COLUMN extra INT FIRST")
        .unwrap();
    session.autocommit = false;
    let params = [
        vec![Value::Int(8), Value::Int(1)],
        vec![Value::Int(9), Value::Int(2)],
    ];
    let result = super::super::prepared_dml::execute_homogeneous_point_update_batch(
        engine.database(),
        &mut session,
        &plan,
        &[&params[0], &params[1]],
        None,
    );
    assert_eq!(result.unwrap_err().code(), 1615);
    assert!(
        session.current_txn.is_none(),
        "reprepare must happen before opening a transaction"
    );
    assert_eq!(
        rows(&engine, "SELECT id,v FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(0)],
            vec![Value::Int(2), Value::Int(0)]
        ]
    );
    engine
        .execute("ALTER TABLE t ADD COLUMN later INT")
        .unwrap();
}

#[test]
fn load_ignore_skips_null_json_rows_with_warning_and_keeps_valid_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,j JSON NOT NULL)")
        .unwrap();
    let file = dir.path().join("null-json.tsv");
    std::fs::write(&file, b"1\t{}\n2\t\\N\n3\t{}\n").unwrap();
    let mut session = SessionContext::default();
    for suffix in ["", " SET id=id"] {
        engine.execute("DELETE FROM t").unwrap();
        assert_eq!(
            engine
                .execute_with_session(
                    &mut session,
                    &format!(
                        "LOAD DATA INFILE '{}' IGNORE INTO TABLE t{suffix}",
                        crate::util::file::sql_path(&file)
                    )
                )
                .unwrap(),
            QueryResponse::OkAffected { affected_rows: 2 }
        );
        assert_eq!(session.warning_count(), 1);
        assert_eq!(
            rows(&engine, "SELECT id FROM t ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(3)]]
        );
    }
    engine.execute("DELETE FROM t").unwrap();
    assert_eq!(
        engine
            .execute(&format!(
                "LOAD DATA INFILE '{}' INTO TABLE t",
                crate::util::file::sql_path(&file)
            ))
            .unwrap_err()
            .code(),
        1048
    );
    assert!(rows(&engine, "SELECT * FROM t").is_empty());
}

#[test]
fn load_xml_rejects_invalid_utf8_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE t (s VARCHAR(64))").unwrap();
    let file = dir.path().join("invalid.xml");
    std::fs::write(&file, b"<rows><row s='valid'/><row s='\xff'/></rows>").unwrap();
    assert_eq!(
        engine
            .execute(&format!(
                "LOAD XML INFILE '{}' INTO TABLE t",
                crate::util::file::sql_path(&file)
            ))
            .unwrap_err()
            .code(),
        1300
    );
    assert!(rows(&engine, "SELECT * FROM t").is_empty());
    std::fs::write(&file, b"<rows><row s='caf\xe9'/></rows>").unwrap();
    engine
        .execute(&format!(
            "LOAD XML INFILE '{}' INTO TABLE t CHARACTER SET latin1",
            crate::util::file::sql_path(&file)
        ))
        .unwrap();
    assert_eq!(
        rows(&engine, "SELECT s FROM t"),
        vec![vec![Value::String("café".into())]]
    );
}

#[test]
fn self_join_update_merges_disjoint_columns_and_first_alias_owns_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY,x INT,y INT)")
        .unwrap();
    engine
        .execute("INSERT INTO t VALUES (1,0,0),(2,0,0)")
        .unwrap();
    assert_eq!(
        engine
            .execute("UPDATE t a JOIN t b ON a.id<>b.id SET a.x=a.x+1,b.y=b.y+10,b.x=b.x+10")
            .unwrap(),
        QueryResponse::OkAffected { affected_rows: 2 }
    );
    assert_eq!(
        rows(&engine, "SELECT x,y FROM t ORDER BY id"),
        vec![
            vec![Value::Int(1), Value::Int(10)],
            vec![Value::Int(1), Value::Int(10)]
        ]
    );
}

#[test]
fn multi_dml_on_unqualified_names_use_only_the_visible_join_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in [
        "CREATE TABLE a (id INT PRIMARY KEY,v INT)",
        "CREATE TABLE b (bid INT)",
        "CREATE TABLE c (v INT)",
        "INSERT INTO a VALUES (1,1)",
        "INSERT INTO b VALUES (1)",
        "INSERT INTO c VALUES (1)",
    ] {
        engine.execute(sql).unwrap();
    }
    engine
        .execute("UPDATE a JOIN b ON v=bid JOIN c ON a.v=c.v SET a.v=5")
        .unwrap();
    assert_eq!(rows(&engine, "SELECT v FROM a"), vec![vec![Value::Int(5)]]);
}

#[test]
fn multi_delete_ignore_undoes_partial_fk_cascade_and_does_not_ignore_trigger_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    for sql in ["CREATE TABLE p (id INT PRIMARY KEY)", "CREATE TABLE j (id INT PRIMARY KEY)", "CREATE TABLE cascaded (id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id) ON DELETE CASCADE)", "CREATE TABLE restricted (id INT PRIMARY KEY,pid INT,FOREIGN KEY(pid) REFERENCES p(id))", "INSERT INTO p VALUES (1),(2)", "INSERT INTO j VALUES (1),(2)", "INSERT INTO cascaded VALUES (1,1),(2,2)", "INSERT INTO restricted VALUES (1,1)"] { engine.execute(sql).unwrap(); }
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "DELETE IGNORE p FROM p JOIN j ON p.id=j.id")
        .unwrap();
    assert_eq!(session.warning_count(), 1);
    assert_eq!(rows(&engine, "SELECT id FROM p"), vec![vec![Value::Int(1)]]);
    assert_eq!(
        rows(&engine, "SELECT id FROM cascaded"),
        vec![vec![Value::Int(1)]]
    );
    engine.execute("CREATE TRIGGER bd BEFORE DELETE ON p FOR EACH ROW SIGNAL SQLSTATE '23000' SET MYSQL_ERRNO=1451,MESSAGE_TEXT='trigger failure'").unwrap();
    assert_eq!(
        engine
            .execute("DELETE IGNORE p FROM p JOIN j ON p.id=j.id")
            .unwrap_err()
            .code(),
        1451
    );
    assert_eq!(rows(&engine, "SELECT id FROM p"), vec![vec![Value::Int(1)]]);
}

#[test]
fn odku_triggered_path_reports_final_auto_increment_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    engine
        .execute("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY,u INT UNIQUE,v INT)")
        .unwrap();
    engine
        .execute("CREATE TRIGGER bu BEFORE UPDATE ON t FOR EACH ROW SET NEW.id=19")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (7,1,0)").unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "INSERT INTO t (u,v) VALUES (1,5) ON DUPLICATE KEY UPDATE v=VALUES(v)",
        )
        .unwrap();
    assert_eq!(session.statement_insert_id, 19);
    assert_eq!(
        rows(&engine, "SELECT id,v FROM t"),
        vec![vec![Value::Int(19), Value::Int(5)]]
    );
}
