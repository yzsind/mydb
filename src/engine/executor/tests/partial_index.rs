use super::support::result_rows;
use super::*;

fn rows(engine: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = engine.execute(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    else {
        panic!("expected rows: {sql}")
    };
    rs.rows
}

#[test]
fn partial_index_membership_unique_transitions_and_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,extra INT)")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,7,0,1),(2,7,NULL,2),(3,7,1,3)")
        .unwrap();
    e.execute("CREATE UNIQUE INDEX pu ON pi(k) WHERE active=1")
        .unwrap();
    e.execute("CREATE INDEX pc ON pi(k) INCLUDE(extra) WHERE active=1")
        .unwrap();
    assert!(e.execute("UPDATE pi SET active=1 WHERE id=1").is_err());
    e.execute("UPDATE pi SET active=0 WHERE id=3").unwrap();
    e.execute("UPDATE pi SET active=1 WHERE id=1").unwrap();
    assert!(e.execute("INSERT INTO pi VALUES(4,7,1,4)").is_err());
    e.execute("INSERT INTO pi VALUES(4,7,0,4),(5,NULL,1,5),(6,NULL,1,6)")
        .unwrap();
    let expected = rows(
        &e,
        "SELECT id,k,extra FROM pi IGNORE INDEX(pu,pc) WHERE active=1 ORDER BY id",
    );
    assert_eq!(
        rows(
            &e,
            "SELECT id,k,extra FROM pi FORCE INDEX(pc) WHERE active=1 ORDER BY id"
        ),
        expected
    );
    let mut writer = SessionContext::default();
    e.execute_with_session(&mut writer, "START TRANSACTION")
        .unwrap();
    e.execute_with_session(&mut writer, "UPDATE pi SET active=NULL WHERE id=1")
        .unwrap();
    e.execute_with_session(&mut writer, "UPDATE pi SET active=1 WHERE id=2")
        .unwrap();
    e.execute_with_session(&mut writer, "ROLLBACK").unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT id,k,extra FROM pi FORCE INDEX(pc) WHERE active=1 ORDER BY id"
        ),
        expected
    );
    assert_eq!(
        rows(&e, "SELECT id FROM pi WHERE k=7 ORDER BY id"),
        rows(
            &e,
            "SELECT id FROM pi IGNORE INDEX(pu,pc) WHERE k=7 ORDER BY id"
        )
    );
}

#[test]
fn partial_index_inline_alter_show_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let e = open_strict_test_engine(dir.path());
        e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,INDEX px(k) WHERE active IN(1,2), INDEX py(k) WHERE (active>0 AND active<3))").unwrap();
        e.execute("ALTER TABLE pi ADD INDEX pz(k) WHERE active=1, ADD COLUMN v INT")
            .unwrap();
        e.execute("INSERT INTO pi VALUES(1,2,1,10),(2,2,0,20),(3,2,NULL,30)")
            .unwrap();
        let ddl = rows(&e, "SHOW CREATE TABLE pi");
        assert!(format!("{ddl:?}").contains("WHERE"));
        assert_eq!(
            rows(
                &e,
                "SELECT id FROM pi FORCE INDEX(pz) WHERE k=2 AND active=1"
            ),
            vec![vec![Value::Int(1)]]
        );
        assert!(e.execute("ALTER TABLE pi DROP COLUMN active").is_err());
        e.shutdown().unwrap();
    }
    let e = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM pi FORCE INDEX(pz) WHERE k=2 AND active=1"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn partial_index_invalid_predicates_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT,k INT,active INT)")
        .unwrap();
    for predicate in [
        "RAND()>0.5",
        "NOW()>0",
        "@x=1",
        "@@sql_mode='x'",
        "missing=1",
        "EXISTS(SELECT 1)",
        "SUM(k)>0",
        "RLS_CONTEXT('tenant')=1",
        "AI_GENERATE('missing_model','prompt')='text'",
        "KEYRING_KEY_GENERATE('key','AES',16)=1",
        "ICU_VERSION() IS NOT NULL",
    ] {
        assert!(
            e.execute(&format!("CREATE INDEX bad ON pi(k) WHERE {predicate}"))
                .is_err(),
            "{predicate}"
        );
    }
    e.execute("CREATE INDEX bad ON pi(k) WHERE active=1")
        .unwrap();
}

#[test]
fn partial_index_range_implication_and_or_safety() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT)")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,1,-1),(2,2,0),(3,3,1),(4,4,2),(5,5,3),(6,6,NULL)")
        .unwrap();
    e.execute("CREATE INDEX pp ON pi(active,k) WHERE active>0")
        .unwrap();
    for condition in [
        "active>1",
        "active>=2",
        "active=1",
        "active BETWEEN 1 AND 3",
        "active IN(1,2)",
        "active=1 OR active=3",
    ] {
        assert_eq!(
            rows(
                &e,
                &format!("SELECT id,k FROM pi FORCE INDEX(pp) WHERE {condition} ORDER BY id")
            ),
            rows(
                &e,
                &format!("SELECT id,k FROM pi IGNORE INDEX(pp) WHERE {condition} ORDER BY id")
            ),
            "{condition}"
        );
    }
    for condition in ["active>=0", "active IS NULL", "active=0 OR active=1", "k=1"] {
        assert_eq!(
            rows(
                &e,
                &format!("SELECT id,k FROM pi WHERE {condition} ORDER BY id")
            ),
            rows(
                &e,
                &format!("SELECT id,k FROM pi IGNORE INDEX(pp) WHERE {condition} ORDER BY id")
            ),
            "{condition}"
        );
    }
}

fn assert_physical_members(engine: &StorageQueryEngine, table_name: &str, index_name: &str) {
    let db = engine.db_for_tests();
    let qualified = if table_name.contains('.') {
        table_name.to_string()
    } else {
        format!("test.{table_name}")
    };
    let qualified = qualified.to_ascii_lowercase();
    let table_name = qualified.as_str();
    let table = db.table(table_name).unwrap();
    let index = table.indexes.iter().find(|i| i.name == index_name).unwrap();
    let txn =
        crate::storage::TxnView::read_only(u64::MAX, crate::storage::IsolationLevel::ReadCommitted);
    let mut expected = db
        .scan_rows(table_name, txn)
        .unwrap()
        .into_iter()
        .flat_map(|row| {
            table
                .index_keys(index, &row.values)
                .unwrap()
                .into_iter()
                .map(move |key| (key, row.row_id))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let actual = db.physical_index_entries_for_test(&table, index).unwrap();
    assert_eq!(actual, expected, "{table_name}.{index_name}");
}

#[test]
fn partial_index_physical_members_and_random_dml_match_heap() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,INDEX pp(k) WHERE active=1)")
        .unwrap();
    for id in 0..40 {
        e.execute(&format!(
            "INSERT INTO pi VALUES({id},{}, {})",
            id % 7,
            id % 3
        ))
        .unwrap();
    }
    assert_physical_members(&e, "pi", "pp");
    let mut seed = 0x5eed_u64;
    for step in 0..100 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (seed >> 32) % 40;
        let active = if step % 5 == 0 {
            "NULL".to_string()
        } else {
            (step % 3).to_string()
        };
        e.execute(&format!(
            "UPDATE pi SET active={active},k={} WHERE id={id}",
            step % 11
        ))
        .unwrap();
        if step % 10 == 0 {
            assert_physical_members(&e, "pi", "pp");
            assert_eq!(
                rows(
                    &e,
                    "SELECT id,k FROM pi FORCE INDEX(pp) WHERE active=1 ORDER BY id"
                ),
                rows(
                    &e,
                    "SELECT id,k FROM pi IGNORE INDEX(pp) WHERE active=1 ORDER BY id"
                )
            );
        }
    }
    e.execute("DELETE FROM pi WHERE id%2=0").unwrap();
    assert_physical_members(&e, "pi", "pp");
    e.execute("ANALYZE TABLE pi").unwrap();
    assert_physical_members(&e, "pi", "pp");
    e.execute("OPTIMIZE TABLE pi").unwrap();
    assert_physical_members(&e, "pi", "pp");
}

#[test]
fn partial_index_functions_rename_copy_and_constants() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k VARCHAR(32),active INT)")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,'ONE',1),(2,'two',0),(3,'Three',NULL)")
        .unwrap();
    e.execute(
        "CREATE INDEX pf ON pi((LOWER(k))) WHERE CASE WHEN active IS NULL THEN 0 ELSE active END=1",
    )
    .unwrap();
    e.execute("CREATE INDEX pe ON pi(id) WHERE FALSE").unwrap();
    e.execute("CREATE INDEX pa ON pi(id) WHERE TRUE").unwrap();
    for idx in ["pf", "pe", "pa"] {
        assert_physical_members(&e, "pi", idx);
    }
    e.execute("ALTER TABLE pi RENAME COLUMN active TO enabled")
        .unwrap();
    e.execute("UPDATE pi SET enabled=1 WHERE id=2").unwrap();
    assert_physical_members(&e, "pi", "pf");
    e.execute("ALTER TABLE pi ADD COLUMN payload VARCHAR(200), ALGORITHM=COPY")
        .unwrap();
    for idx in ["pf", "pe", "pa"] {
        assert_physical_members(&e, "pi", idx);
    }
    e.execute("CREATE TABLE pi_like LIKE pi").unwrap();
    e.execute("INSERT INTO pi_like(id,k,enabled) VALUES(4,'four',1),(5,'five',0)")
        .unwrap();
    assert_physical_members(&e, "pi_like", "pf");
}

#[test]
fn partial_index_fulltext_scores_use_whole_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,active INT,body TEXT,FULLTEXT KEY full_ft(body),FULLTEXT KEY partial_ft(body) WHERE active=1)").unwrap();
    e.execute("INSERT INTO pi VALUES(1,1,'alpha beta'),(2,0,'alpha beta gamma'),(3,1,'alpha gamma'),(4,NULL,'beta'),(5,1,NULL)").unwrap();
    assert_physical_members(&e, "pi", "partial_ft");
    e.execute(
        "CREATE TABLE full_only(id INT PRIMARY KEY,active INT,body TEXT,FULLTEXT KEY ft(body))",
    )
    .unwrap();
    e.execute("INSERT INTO full_only SELECT * FROM pi").unwrap();
    for query in [
        "'alpha'",
        "'+alpha -beta' IN BOOLEAN MODE",
        "'alpha*' IN BOOLEAN MODE",
        "'alpha beta gamma' IN BOOLEAN MODE",
        "'+alpha beta gam*' IN BOOLEAN MODE",
        "'\"alpha beta\"' IN BOOLEAN MODE",
    ] {
        let sql = format!(
            "SELECT id,MATCH(body) AGAINST({query}) AS s FROM pi WHERE active=1 ORDER BY id"
        );
        let expected = rows(&e, &sql);
        assert_eq!(
            expected,
            rows(&e, &sql.replace("FROM pi", "FROM full_only"))
        );
        let outside = rows(&e, &sql.replace("WHERE active=1", "WHERE active=0"));
        e.execute("ALTER TABLE pi ALTER INDEX full_ft INVISIBLE")
            .unwrap();
        assert_eq!(rows(&e, &sql), expected, "{query}");
        assert_eq!(
            rows(&e, &sql.replace("WHERE active=1", "WHERE active=0")),
            outside
        );
        e.execute("ALTER TABLE pi ALTER INDEX full_ft VISIBLE")
            .unwrap();
    }
    e.execute("UPDATE pi SET active=1 WHERE id=2").unwrap();
    e.execute("UPDATE pi SET active=0 WHERE id=1").unwrap();
    assert_physical_members(&e, "pi", "partial_ft");
    e.execute("DROP INDEX full_ft ON pi").unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM pi WHERE active=0 AND MATCH(body) AGAINST('alpha') ORDER BY id"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn partial_index_spatial_and_vector_filter_only_updates() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ps(id INT PRIMARY KEY,active INT,g GEOMETRY NOT NULL,SPATIAL KEY sp(g) WHERE active=1)").unwrap();
    e.execute("INSERT INTO ps VALUES(1,1,ST_GeomFromText('POINT(1 1)')),(2,0,ST_GeomFromText('POINT(2 2)'))").unwrap();
    assert_physical_members(&e, "ps", "sp");
    e.execute("UPDATE ps SET active=1-active").unwrap();
    assert_physical_members(&e, "ps", "sp");
    for algo in ["HNSW", "IVF"] {
        e.execute(&format!("CREATE TABLE pv_{algo}(id INT PRIMARY KEY,active INT,v VECTOR(2) NOT NULL,INDEX vi(v) USING {algo} WHERE active=1)")).unwrap();
        e.execute(&format!(
            "INSERT INTO pv_{algo} VALUES(1,0,'[0,0]'),(2,1,'[1,0]'),(3,1,'[2,0]')"
        ))
        .unwrap();
        let sql = format!(
            "SELECT id FROM pv_{algo} WHERE active=1 ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 1"
        );
        assert_physical_members(&e, &format!("pv_{algo}"), "vi");
        assert_eq!(rows(&e, &sql), vec![vec![Value::Int(2)]]);
        e.execute(&format!("UPDATE pv_{algo} SET active=1 WHERE id=1"))
            .unwrap();
        assert_physical_members(&e, &format!("pv_{algo}"), "vi");
        assert_eq!(rows(&e, &sql), vec![vec![Value::Int(1)]]);
        e.execute(&format!("UPDATE pv_{algo} SET active=0 WHERE id=1"))
            .unwrap();
        assert_eq!(rows(&e, &sql), vec![vec![Value::Int(2)]]);
        assert_physical_members(&e, &format!("pv_{algo}"), "vi");
        assert_eq!(
            rows(&e, &sql.replace("WHERE active=1", "")),
            vec![vec![Value::Int(1)]]
        );
    }
}

#[test]
fn partial_index_snapshot_savepoint_prepared_and_settings_guards() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,INDEX pp(k) WHERE active=1)")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,7,1),(2,7,0)").unwrap();
    let mut reader = SessionContext::default();
    reader.connection_id = 7211;
    let mut writer = SessionContext::default();
    writer.connection_id = 7212;
    e.execute_with_session(
        &mut reader,
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    )
    .unwrap();
    e.execute_with_session(&mut reader, "START TRANSACTION")
        .unwrap();
    let sql = "SELECT id FROM pi FORCE INDEX(pp) WHERE active=1 AND k=7 ORDER BY id";
    let before = result_rows(e.execute_with_session(&mut reader, sql).unwrap());
    e.execute_with_session(&mut writer, "UPDATE pi SET active=1-active")
        .unwrap();
    assert_eq!(
        result_rows(e.execute_with_session(&mut reader, sql).unwrap()),
        before
    );
    e.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert_eq!(rows(&e, sql), vec![vec![Value::Int(2)]]);
    e.execute_with_session(&mut writer, "BEGIN").unwrap();
    e.execute_with_session(&mut writer, "SAVEPOINT s").unwrap();
    e.execute_with_session(&mut writer, "UPDATE pi SET active=1-active")
        .unwrap();
    e.execute_with_session(&mut writer, "ROLLBACK TO SAVEPOINT s")
        .unwrap();
    e.execute_with_session(&mut writer, "COMMIT").unwrap();
    assert_eq!(rows(&e, sql), vec![vec![Value::Int(2)]]);
    for value in [1, 0, 1, 2] {
        e.execute_with_session(&mut writer, "SET @active=1")
            .unwrap();
        e.execute_with_session(
            &mut writer,
            "PREPARE pq FROM 'SELECT id FROM pi WHERE active=? AND k=7 ORDER BY id'",
        )
        .unwrap();
        e.execute_with_session(&mut writer, &format!("SET @active={value}"))
            .unwrap();
        let actual = result_rows(
            e.execute_with_session(&mut writer, "EXECUTE pq USING @active")
                .unwrap(),
        );
        assert_eq!(
            actual,
            rows(
                &e,
                &format!(
                    "SELECT id FROM pi IGNORE INDEX(pp) WHERE active={value} AND k=7 ORDER BY id"
                )
            )
        );
        e.execute_with_session(&mut writer, "DEALLOCATE PREPARE pq")
            .unwrap();
    }
    e.execute_with_session(&mut writer, "SET time_zone='+08:00'")
        .unwrap();
    assert!(e.execute_with_session(&mut writer, sql).is_err());
    assert!(e
        .execute("SELECT * FROM pi FORCE INDEX(pp) WHERE k=7")
        .is_err());
}

#[test]
fn partial_index_online_build_replays_filter_changes_and_undo() {
    let dir = tempfile::tempdir().unwrap();
    let engine = std::sync::Arc::new(open_relaxed_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT)")
        .unwrap();
    engine
        .execute("INSERT INTO pi VALUES(1,7,1),(2,7,0),(3,8,1)")
        .unwrap();
    let hook = engine.database().install_ddl_backfill_test_hook();
    let worker = engine.clone();
    let ddl = std::thread::spawn(move || {
        let mut session = SessionContext::default();
        session.connection_id = 7221;
        worker.execute_with_session(&mut session, "CREATE INDEX pp ON pi(k) WHERE active=1")
    });
    let reached = hook.wait_until_reached(Duration::from_secs(10));
    let mut writer = SessionContext::default();
    writer.connection_id = 7222;
    let writes = (|| -> Result<(), QueryError> {
        engine.execute_with_session(&mut writer, "UPDATE pi SET active=0 WHERE id=1")?;
        engine.execute_with_session(&mut writer, "UPDATE pi SET active=1 WHERE id=2")?;
        engine.execute_with_session(&mut writer, "BEGIN")?;
        engine.execute_with_session(&mut writer, "UPDATE pi SET active=0 WHERE id=3")?;
        engine.execute_with_session(&mut writer, "ROLLBACK")?;
        engine.execute_with_session(&mut writer, "INSERT INTO pi VALUES(4,9,1),(5,9,NULL)")?;
        Ok(())
    })();
    hook.release();
    let built = ddl.join().unwrap();
    assert!(reached, "backfill hook not reached: {built:?}");
    writes.unwrap();
    built.unwrap();
    assert_physical_members(&engine, "pi", "pp");
    assert_eq!(
        rows(
            &engine,
            "SELECT id FROM pi FORCE INDEX(pp) WHERE active=1 ORDER BY id"
        ),
        vec![
            vec![Value::Int(2)],
            vec![Value::Int(3)],
            vec![Value::Int(4)]
        ]
    );
}

#[test]
fn partial_index_foreign_keys_never_use_incomplete_keys() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE parent(id INT,active INT,UNIQUE KEY pu(id) WHERE active=1)")
        .unwrap();
    assert!(e
        .execute("CREATE TABLE child(pid INT,FOREIGN KEY(pid) REFERENCES parent(id))")
        .is_err());
    e.execute("CREATE TABLE whole_parent(id INT PRIMARY KEY)")
        .unwrap();
    e.execute("CREATE TABLE child(pid INT,active INT,KEY partial_child(pid) WHERE active=1,FOREIGN KEY(pid) REFERENCES whole_parent(id))").unwrap();
    let db = e.db_for_tests();
    let t = db.table("test.child").unwrap();
    let backing = &t.foreign_keys[0].backing_index;
    assert!(t
        .indexes
        .iter()
        .find(|i| &i.name == backing)
        .unwrap()
        .predicate
        .is_none());
}

#[test]
fn partial_index_prepared_memo_rechecks_bindings_and_hints() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine};
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,INDEX pp(k) WHERE active=1)")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,7,1),(2,7,0),(3,7,NULL)")
        .unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT id FROM pi WHERE active=? AND k=7 ORDER BY id";
    let parsed = e.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    for active in [1, 0, 2, 1, 0] {
        let response = e
            .execute_prepared_select(
                &mut session,
                &mut state,
                &parsed,
                sql,
                &[Value::Int(active)],
            )
            .expect("eligible prepared SELECT")
            .unwrap()
            .materialize()
            .unwrap();
        assert_eq!(
            result_rows(response),
            rows(
                &e,
                &format!(
                    "SELECT id FROM pi IGNORE INDEX(pp) WHERE active={active} AND k=7 ORDER BY id"
                )
            )
        );
    }
    let sql = "SELECT id FROM pi FORCE INDEX(pp) WHERE active=? AND k=7";
    let parsed = e.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let response = e
        .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(1)])
        .expect("eligible prepared SELECT")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(result_rows(response), vec![vec![Value::Int(1)]]);
    // A failed planning attempt may decline the compiled lane. The ordinary
    // statement path must also reject the unsafe forced index.
    if let Some(result) =
        e.execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(0)])
    {
        assert!(result.is_err());
    }
    e.execute("ALTER TABLE pi ALTER INDEX pp INVISIBLE")
        .unwrap();
    assert!(e
        .execute("SELECT id FROM pi FORCE INDEX(pp) WHERE active=1")
        .is_err());
}

#[test]
fn partial_index_load_replace_and_upsert_membership() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute(
        "CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,UNIQUE KEY pu(k) WHERE active=1)",
    )
    .unwrap();
    let input = dir.path().join("partial.csv");
    std::fs::write(&input, "1,7,1\n2,7,0\n3,8,1\n").unwrap();
    e.execute(&format!(
        "LOAD DATA INFILE '{}' INTO TABLE pi FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'",
        crate::util::file::sql_path(&input)
    ))
    .unwrap();
    assert_physical_members(&e, "pi", "pu");
    e.execute("INSERT INTO pi VALUES(4,7,0) ON DUPLICATE KEY UPDATE active=0")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT id FROM pi WHERE k=7 ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(4)]
        ]
    );
    e.execute("REPLACE INTO pi VALUES(5,7,0)").unwrap();
    assert_eq!(
        rows(&e, "SELECT id FROM pi WHERE k=7 ORDER BY id"),
        vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(4)],
            vec![Value::Int(5)]
        ]
    );
    e.execute("INSERT INTO pi VALUES(6,7,1) ON DUPLICATE KEY UPDATE active=0")
        .unwrap();
    assert_eq!(
        rows(&e, "SELECT id FROM pi WHERE active=1 ORDER BY id"),
        vec![vec![Value::Int(3)]]
    );
    e.execute("REPLACE INTO pi VALUES(7,7,1)").unwrap();
    assert_physical_members(&e, "pi", "pu");
}

#[test]
fn partial_index_crash_recovers_members_and_discards_uncommitted_writes() {
    const CHILD: &str = "MYDB_PARTIAL_INDEX_CRASH_DIR";
    const TEST: &str="engine::executor::tests::partial_index::partial_index_crash_recovers_members_and_discards_uncommitted_writes";
    if let Some(path) = std::env::var_os(CHILD) {
        let e = open_strict_test_engine(std::path::Path::new(&path));
        e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,body TEXT,UNIQUE KEY pu(k) WHERE active=1,FULLTEXT KEY pf(body) WHERE active=1)").unwrap();
        e.execute("INSERT INTO pi VALUES(1,7,1,'alpha beta'),(2,7,0,'alpha gamma')")
            .unwrap();
        e.execute("UPDATE pi SET active=0 WHERE id=1").unwrap();
        e.execute("UPDATE pi SET active=1 WHERE id=2").unwrap();
        let mut writer = SessionContext::default();
        writer.connection_id = 7241;
        e.execute_with_session(&mut writer, "BEGIN").unwrap();
        e.execute_with_session(
            &mut writer,
            "INSERT INTO pi VALUES(3,8,1,'uncommitted word')",
        )
        .unwrap();
        std::process::exit(91);
    }
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(TEST)
        .arg("--nocapture")
        .env(CHILD, dir.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(91),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let e = open_strict_test_engine(dir.path());
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM pi FORCE INDEX(pu) WHERE active=1 ORDER BY id"
        ),
        vec![vec![Value::Int(2)]]
    );
    assert_eq!(
        rows(&e, "SELECT id FROM pi ORDER BY id"),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
    assert_physical_members(&e, "pi", "pu");
    assert_physical_members(&e, "pi", "pf");
    assert!(e
        .execute("INSERT INTO pi VALUES(4,7,1,'duplicate')")
        .is_err());
    e.execute("INSERT INTO pi VALUES(4,7,0,'excluded')")
        .unwrap();
}

#[test]
fn partial_index_fulltext_tokenizer_is_independent_of_membership_proof() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT,active INT,body TEXT,FULLTEXT KEY partial_ft(body) WITH PARSER ngram WHERE active=1,FULLTEXT KEY whole_ft(body))").unwrap();
    e.execute("INSERT INTO pi VALUES(1,1,'database'),(2,0,'database')")
        .unwrap();
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM pi WHERE active=1 AND MATCH(body) AGAINST('dat') ORDER BY id"
        ),
        vec![vec![Value::Int(1)]]
    );
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM pi WHERE active=0 AND MATCH(body) AGAINST('dat') ORDER BY id"
        ),
        vec![vec![Value::Int(2)]]
    );
}

#[test]
fn partial_index_rejected_compound_predicate_does_not_evaluate_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(k INT,KEY base_idx(k))").unwrap();
    e.execute("INSERT INTO pi VALUES(1),(2)").unwrap();
    e.execute("CREATE SEQUENCE side_effect START WITH 1")
        .unwrap();
    assert!(e.execute("ALTER TABLE pi ADD UNIQUE INDEX bad(k) WHERE NEXTVAL('side_effect')>0, DROP INDEX base_idx").is_err());
    assert_eq!(
        rows(&e, "SELECT NEXTVAL('side_effect')"),
        vec![vec![Value::Int(1)]]
    );
    assert!(e
        .db_for_tests()
        .table("test.pi")
        .unwrap()
        .indexes
        .iter()
        .any(|index| index.name == "base_idx"));
}

#[test]
fn partial_index_json_multivalued_decimal_temporal_and_prefix_keys() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,active INT,d DECIMAL(20,4),dt DATETIME,s VARCHAR(32),j JSON,INDEX dx(d,dt) WHERE active=1,INDEX jx((CAST(j->'$.ids' AS UNSIGNED ARRAY))) WHERE active=1,INDEX sx(s(4) DESC) INCLUDE(d) WHERE JSON_UNQUOTE(JSON_EXTRACT(j,'$.enabled'))='yes')").unwrap();
    e.execute(r#"INSERT INTO pi VALUES(1,1,1.2500,'2026-01-01','first','{"ids":[1,2],"enabled":"yes"}'),(2,0,2.5000,'2026-02-01','second','{"ids":[2,3],"enabled":"yes"}'),(3,1,3.2500,'2028-01-01','third','{"ids":[4],"enabled":"no"}')"#).unwrap();
    for index in ["dx", "jx", "sx"] {
        assert_physical_members(&e, "pi", index);
    }
    assert_eq!(rows(&e,"SELECT id FROM pi FORCE INDEX(dx) WHERE active=1 AND d>=1.25 AND dt<'2027-01-01' ORDER BY id"),vec![vec![Value::Int(1)]]);
    assert_eq!(rows(&e,"SELECT id FROM pi FORCE INDEX(jx) WHERE active=1 AND 2 MEMBER OF(j->'$.ids') ORDER BY id"),vec![vec![Value::Int(1)]]);
    e.execute("UPDATE pi SET active=1 WHERE id=2").unwrap();
    assert_eq!(rows(&e,"SELECT id FROM pi FORCE INDEX(jx) WHERE active=1 AND 2 MEMBER OF(j->'$.ids') ORDER BY id"),vec![vec![Value::Int(1)],vec![Value::Int(2)]]);
    e.execute("UPDATE pi SET j=JSON_SET(j,'$.enabled','yes') WHERE id=3")
        .unwrap();
    for index in ["dx", "jx", "sx"] {
        assert_physical_members(&e, "pi", index);
    }
    assert_eq!(rows(&e,"SELECT id FROM pi FORCE INDEX(sx) WHERE JSON_UNQUOTE(JSON_EXTRACT(j,'$.enabled'))='yes' ORDER BY id"),vec![vec![Value::Int(1)],vec![Value::Int(2)],vec![Value::Int(3)]]);
}

#[test]
fn partial_index_membership_uses_creation_sql_mode() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,s VARCHAR(32))")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,'1junk')").unwrap();
    let mut creator = SessionContext::default();
    creator.connection_id = 7251;
    let mut writer = SessionContext::default();
    writer.connection_id = 7252;
    e.execute_with_session(&mut creator, "SET sql_mode=''")
        .unwrap();
    e.execute_with_session(
        &mut creator,
        "CREATE INDEX pc ON pi(id) WHERE CAST(s AS SIGNED)>0",
    )
    .unwrap();
    e.execute_with_session(
        &mut writer,
        "SET sql_mode='STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO'",
    )
    .unwrap();
    e.execute_with_session(&mut writer, "INSERT INTO pi VALUES(2,'2junk')")
        .unwrap();
    assert_physical_members(&e, "pi", "pc");
    assert_eq!(
        result_rows(
            e.execute_with_session(
                &mut creator,
                "SELECT id FROM pi FORCE INDEX(pc) WHERE CAST(s AS SIGNED)>0 ORDER BY id"
            )
            .unwrap()
        ),
        vec![vec![Value::Int(1)], vec![Value::Int(2)]]
    );
}

#[test]
fn partial_index_convert_tz_membership_is_stable_across_writer_zones() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,dt DATETIME)")
        .unwrap();
    let mut creator = SessionContext::default();
    creator.connection_id = 7261;
    let mut writer = SessionContext::default();
    writer.connection_id = 7262;
    e.execute_with_session(&mut creator, "SET time_zone='+08:00'")
        .unwrap();
    e.execute_with_session(
        &mut creator,
        "CREATE INDEX pt ON pi(id) WHERE CONVERT_TZ(dt,'+08:00','+00:00')='2026-01-01 00:00:00'",
    )
    .unwrap();
    e.execute_with_session(
        &mut creator,
        "INSERT INTO pi VALUES(1,'2026-01-01 08:00:00')",
    )
    .unwrap();
    e.execute_with_session(&mut writer, "SET time_zone='-05:00'")
        .unwrap();
    e.execute_with_session(
        &mut writer,
        "INSERT INTO pi VALUES(2,'2026-01-01 08:00:00'),(3,'2026-01-01 09:00:00')",
    )
    .unwrap();
    assert_physical_members(&e, "pi", "pt");
    assert_eq!(result_rows(e.execute_with_session(&mut creator,"SELECT id FROM pi FORCE INDEX(pt) WHERE CONVERT_TZ(dt,'+08:00','+00:00')='2026-01-01 00:00:00' ORDER BY id").unwrap()),vec![vec![Value::Int(1)],vec![Value::Int(2)]]);
}

#[test]
fn partial_index_standalone_vector_options_precede_where() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,active INT,v VECTOR(2) NOT NULL,g GEOMETRY NOT NULL,body TEXT)").unwrap();
    e.execute("INSERT INTO pi VALUES(1,1,'[1,0]',ST_GeomFromText('POINT(1 1)'),'alpha beta'),(2,0,'[0,1]',ST_GeomFromText('POINT(2 2)'),'beta gamma')").unwrap();
    for sql in [
        "CREATE INDEX vh ON pi(v) USING HNSW WITH(metric=l2,m=4,ef_construction=16) WHERE active=1",
        "CREATE INDEX vi ON pi(v) USING IVF WITH(metric=l2,nlist=1,nprobe=1) WHERE active=1",
        "CREATE SPATIAL INDEX sg ON pi(g) WHERE active=1",
        "CREATE FULLTEXT INDEX ft ON pi(body) WITH PARSER ngram WHERE active=1",
    ] {
        e.execute(sql).unwrap();
    }
    for index in ["vh", "vi", "sg", "ft"] {
        assert_physical_members(&e, "pi", index);
    }
}

#[test]
fn partial_index_cosine_constraints_apply_only_to_members_and_generated_flags() {
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    for algo in ["HNSW", "IVF"] {
        let table = format!("cos_{}", algo.to_ascii_lowercase());
        e.execute(&format!("CREATE TABLE {table}(id INT PRIMARY KEY,flag INT,active INT GENERATED ALWAYS AS(flag=1) STORED,v VECTOR(2) NOT NULL,INDEX vc(v) USING {algo} WITH(metric=cosine) WHERE active=1)")).unwrap();
        e.execute(&format!(
            "INSERT INTO {table}(id,flag,v) VALUES(1,0,'[0,0]'),(2,1,'[1,0]')"
        ))
        .unwrap();
        assert_physical_members(&e, &table, "vc");
        assert!(e
            .execute(&format!("UPDATE {table} SET flag=1 WHERE id=1"))
            .is_err());
        assert_eq!(
            rows(&e, &format!("SELECT flag FROM {table} WHERE id=1")),
            vec![vec![Value::Int(0)]]
        );
        e.execute(&format!("UPDATE {table} SET flag=1,v='[0,1]' WHERE id=1"))
            .unwrap();
        assert_physical_members(&e, &table, "vc");
    }
    e.execute("CREATE TABLE build_cos(id INT,active INT,v VECTOR(2) NOT NULL)")
        .unwrap();
    e.execute("INSERT INTO build_cos VALUES(1,0,'[0,0]'),(2,1,'[1,0]')")
        .unwrap();
    e.execute(
        "CREATE INDEX valid_cos ON build_cos(v) USING HNSW WITH(metric=cosine) WHERE active=1",
    )
    .unwrap();
    assert!(e.execute("CREATE INDEX invalid_cos ON build_cos(v) USING HNSW WITH(metric=cosine) WHERE active=0").is_err());
    assert_physical_members(&e, "build_cos", "valid_cos");
}

#[test]
fn partial_index_sibling_does_not_disable_complete_point_cache() {
    use crate::engine::api::{PreparedSelectMemoState, QueryEngine};
    let dir = tempfile::tempdir().unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE pi(id INT PRIMARY KEY,k INT,active INT,INDEX pp(k) WHERE active=1)")
        .unwrap();
    e.execute("INSERT INTO pi VALUES(1,7,1),(2,8,0)").unwrap();
    let mut session = SessionContext::default();
    let sql = "SELECT id,k FROM pi WHERE id=?";
    let parsed = e.prepare_cached(&mut session, sql, 1).unwrap();
    let mut state = PreparedSelectMemoState::default();
    let result = e
        .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(1)])
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        result_rows(result),
        vec![vec![Value::Int(1), Value::Int(7)]]
    );
    let PreparedSelectMemoState::Ready(first) = &state else {
        panic!("expected point memo")
    };
    let first = first.clone();
    assert!(!first.compiled.replan_partial_indexes);
    let result = e
        .execute_prepared_select(&mut session, &mut state, &parsed, sql, &[Value::Int(2)])
        .unwrap()
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(
        result_rows(result),
        vec![vec![Value::Int(2), Value::Int(8)]]
    );
    let PreparedSelectMemoState::Ready(second) = &state else {
        panic!("expected point memo")
    };
    assert!(std::sync::Arc::ptr_eq(&first, second));
}
