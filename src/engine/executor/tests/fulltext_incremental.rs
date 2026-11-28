use super::*;
use crate::storage::{IsolationLevel, TxnView};
use std::collections::{HashMap, HashSet};

fn rows(e: &StorageQueryEngine, sql: &str) -> Vec<Vec<Value>> {
    let QueryResponse::ResultSet(rs) = e
        .execute(sql)
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .materialize()
        .unwrap()
    else {
        panic!("expected rows")
    };
    rs.rows
}

fn current(e: &StorageQueryEngine) -> TxnView {
    TxnView::read_only(
        e.db_for_tests().visible_scn(),
        IsolationLevel::ReadCommitted,
    )
}

// The heap is an independent oracle used only by tests. Every tokenizer and
// corpus is checked, including rows excluded from all partial postings.
pub(super) fn assert_corpora(e: &StorageQueryEngine, name: &str, txn: TxnView) {
    let db = e.db_for_tests();
    let table = db.table(name).unwrap();
    let heap = db.scan_rows(name, txn).unwrap();
    for index in table.indexes.iter().filter(|index| index.is_fulltext()) {
        let tokenizer =
            crate::storage::fulltext::build_tokenizer_from_meta(index.fulltext.as_ref().unwrap());
        let mut expected = HashMap::<String, u64>::new();
        let mut length = 0;
        for row in &heap {
            let mut seen = HashSet::new();
            for column in &index.columns {
                let v = &row.values[table.column_index(column).unwrap()];
                if *v == Value::Null {
                    continue;
                }
                let tokens = tokenizer.tokenize(&v.to_display_string());
                length += tokens.len() as u64;
                seen.extend(tokens);
            }
            for token in seen {
                *expected.entry(token).or_default() += 1;
            }
        }
        expected.entry("absent_token".into()).or_default();
        let terms = expected.keys().cloned().collect::<Vec<_>>();
        let actual = db
            .fulltext_corpus_statistics(txn, &table, index, &terms, None)
            .unwrap();
        assert_eq!(actual.documents, heap.len() as u64, "{} N", index.name);
        assert_eq!(actual.total_tokens, length, "{} length", index.name);
        assert_eq!(actual.frequencies, expected, "{} df", index.name);
    }
}

#[test]
fn fulltext_incremental_chunked_ordered_delete_rollback_and_restart() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let e = open_strict_test_engine(dir.path());
        e.execute("CREATE TABLE ft_delete(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body),FULLTEXT fp(body) WHERE active=1,INDEX ps(active) WHERE active=1)").unwrap();
        let values = (0..600)
            .map(|id| format!("({id},'quartz ruby',{})", id % 2))
            .collect::<Vec<_>>()
            .join(",");
        e.execute(&format!("INSERT INTO ft_delete VALUES {values}"))
            .unwrap();
        let mut session = SessionContext::default();
        for sql in [
            "BEGIN",
            "SAVEPOINT keep",
            "DELETE FROM ft_delete ORDER BY id LIMIT 300",
            "ROLLBACK TO keep",
            "DELETE FROM ft_delete ORDER BY id DESC LIMIT 300",
            "ROLLBACK",
            "DELETE FROM ft_delete ORDER BY id LIMIT 300",
        ] {
            let response = e.execute_with_session(&mut session, sql).unwrap();
            if sql.starts_with("DELETE") {
                assert!(matches!(
                    response,
                    QueryResponse::OkAffected { affected_rows: 300 }
                ));
            }
            let view = session.current_txn.map_or_else(
                || current(&e),
                |txn| {
                    e.db_for_tests().session_statement_view(
                        txn,
                        IsolationLevel::ReadCommitted,
                        None,
                    )
                },
            );
            assert_corpora(&e, "test.ft_delete", view);
            assert_corpora(&e, "test.ft_delete", current(&e));
        }
        assert_eq!(
            rows(&e, "SELECT COUNT(*) FROM ft_delete"),
            vec![vec![Value::Int(300)]]
        );
        e.shutdown().unwrap();
    }
    let e = open_strict_test_engine(dir.path());
    assert_corpora(&e, "test.ft_delete", current(&e));
    assert_eq!(
        rows(
            &e,
            "SELECT COUNT(*) FROM ft_delete WHERE active=1 AND MATCH(body) AGAINST('quartz')"
        ),
        vec![vec![Value::Int(150)]]
    );
}

#[test]
fn fulltext_incremental_cold_statistics_do_not_scan_heap() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY, body TEXT, active INT, FULLTEXT ft(body) WHERE active=1)").unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz quartz ruby',1),(2,'quartz sapphire',0),(3,'sapphire diamond',NULL)").unwrap();
    let db = e.db_for_tests();
    let table = db.table("test.ft_inc").unwrap();
    for round in 0..3 {
        if round != 0 {
            e.execute("UPDATE ft_inc SET body=CONCAT(body,' garnet') WHERE id=2")
                .unwrap();
        }
        crate::engine::perf_events::clear_statement_rows_examined();
        crate::engine::perf_events::start_statement_rows_examined();
        let stats = db
            .fulltext_corpus_statistics(
                current(&e),
                &table,
                &table.indexes[1],
                &["quartz".into()],
                None,
            )
            .unwrap();
        assert_eq!(stats.documents, 3);
        assert_eq!(stats.total_tokens, 7 + round);
        assert_eq!(stats.frequencies["quartz"], 2);
        assert_eq!(
            crate::engine::perf_events::take_scan_rows_examined(),
            0,
            "cold/write-first scoring scanned a heap"
        );
        crate::engine::perf_events::clear_statement_rows_examined();
        assert_eq!(
            rows(
                &e,
                "SELECT id FROM ft_inc WHERE active=1 AND MATCH(body) AGAINST('quartz')"
            ),
            vec![vec![Value::Int(1)]]
        );
    }
    let mut prefix = rows(
        &e,
        "SELECT id FROM ft_inc WHERE MATCH(body) AGAINST('quar*' IN BOOLEAN MODE) LIMIT 10",
    )
    .into_iter()
    .map(|row| row[0].to_display_string())
    .collect::<Vec<_>>();
    prefix.sort();
    assert_eq!(prefix, vec!["1", "2"]);
    assert_corpora(&e, "test.ft_inc", current(&e));
}

#[test]
fn fulltext_incremental_membership_dml_savepoints_and_statement_atomicity() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY, body TEXT, title TEXT, active INT, g TEXT GENERATED ALWAYS AS (CONCAT(body,' ruby')) STORED, FULLTEXT f1(body,title), FULLTEXT f2(title,body) WHERE active=1, FULLTEXT fg(g) WHERE active IS NOT NULL)").unwrap();
    for sql in [
        "INSERT INTO ft_inc(id,body,title,active) VALUES(1,'quartz quartz','ruby',1),(2,'quartz','sapphire',0),(3,NULL,'',NULL),(4,'diamond','quartz',1)",
        "UPDATE ft_inc SET active=CASE id WHEN 1 THEN NULL WHEN 2 THEN 1 ELSE 0 END",
        "UPDATE ft_inc SET body='ruby sapphire',title=NULL WHERE id=2",
        "REPLACE INTO ft_inc(id,body,title,active) VALUES(4,'emerald','quartz',0)",
        "INSERT INTO ft_inc(id,body,title,active) VALUES(1,'zircon','garnet',1) ON DUPLICATE KEY UPDATE body=VALUES(body),title=VALUES(title),active=VALUES(active)",
        "DELETE FROM ft_inc WHERE id=3",
    ] { e.execute(sql).unwrap_or_else(|err|panic!("{sql}: {err}")); assert_corpora(&e,"test.ft_inc",current(&e)); }
    let table = e.db_for_tests().table("test.ft_inc").unwrap();
    assert_eq!(
        table.indexes[1].fulltext.as_ref().unwrap().corpus_id,
        table.indexes[2].fulltext.as_ref().unwrap().corpus_id
    );
    let before = rows(&e, "SELECT * FROM ft_inc ORDER BY id");
    let mut s = SessionContext::default();
    for sql in [
        "BEGIN",
        "UPDATE ft_inc SET body='diamond diamond diamond' WHERE id=1",
        "SAVEPOINT keep",
        "DELETE FROM ft_inc WHERE id=2",
        "INSERT INTO ft_inc(id,body,active) VALUES(8,'zircon',1)",
        "ROLLBACK TO keep",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    let own = e.db_for_tests().session_statement_view(
        s.current_txn.unwrap(),
        IsolationLevel::ReadCommitted,
        None,
    );
    assert_corpora(&e, "test.ft_inc", own);
    assert_corpora(&e, "test.ft_inc", current(&e));
    assert!(e
        .execute_with_session(
            &mut s,
            "INSERT INTO ft_inc(id,body,active) VALUES(9,'zircon',1),(1,'quartz',1)"
        )
        .is_err());
    assert_corpora(&e, "test.ft_inc", own);
    e.execute_with_session(&mut s, "ROLLBACK").unwrap();
    assert_eq!(rows(&e, "SELECT * FROM ft_inc ORDER BY id"), before);
    assert_corpora(&e, "test.ft_inc", current(&e));
}

#[test]
fn fulltext_incremental_ddl_shared_corpus_rebuild_drop_readd_and_copy() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT)")
        .unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(2,'quartz sapphire',0),(3,NULL,NULL)")
        .unwrap();
    for sql in [
        "CREATE FULLTEXT INDEX fp ON ft_inc(body) WHERE active=1",
        "ALTER TABLE ft_inc ADD FULLTEXT INDEX f(body)",
        "DROP INDEX fp ON ft_inc",
        "UPDATE ft_inc SET body='diamond diamond' WHERE id=2",
        "DROP INDEX f ON ft_inc",
        "INSERT INTO ft_inc VALUES(4,'emerald emerald emerald',0)",
        "CREATE FULLTEXT INDEX fn ON ft_inc(body) WHERE active=1",
        "ALTER TABLE ft_inc RENAME COLUMN body TO content",
        "ALTER TABLE ft_inc ADD COLUMN extra INT, ALGORITHM=COPY",
        "OPTIMIZE TABLE ft_inc",
        "ANALYZE TABLE ft_inc",
    ] {
        e.execute(sql).unwrap_or_else(|err| panic!("{sql}: {err}"));
        assert_corpora(&e, "test.ft_inc", current(&e));
    }
    e.execute("TRUNCATE TABLE ft_inc").unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    e.execute("INSERT INTO ft_inc VALUES(10,'zircon zircon',1,3)")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    e.execute("CREATE TABLE ft_copy LIKE ft_inc").unwrap();
    e.execute("INSERT INTO ft_copy SELECT * FROM ft_inc")
        .unwrap();
    assert_corpora(&e, "test.ft_copy", current(&e));
}

#[test]
fn fulltext_incremental_ngram_multicolumn_scores_match_complete_corpus() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    for (table, predicate) in [("ft_inc", " WHERE active=1"), ("ft_oracle", "")] {
        e.execute(&format!("CREATE TABLE {table}(id INT PRIMARY KEY,body TEXT,title TEXT,active INT,FULLTEXT f(body,title) WITH PARSER ngram{predicate})")).unwrap();
        e.execute(&format!("INSERT INTO {table} VALUES(1,'数据库数据库','全文检索',1),(2,'全文数据库','数据库',0),(3,'数据库检索','全文全文',1),(4,NULL,NULL,NULL)")).unwrap();
    }
    for round in 0..3 {
        for query in [
            "'数据库'",
            "'+数据库 全文*' IN BOOLEAN MODE",
            "'\"数据库\"' IN BOOLEAN MODE",
        ] {
            let sql = format!("SELECT id,MATCH(body,title) AGAINST({query}) s FROM ft_inc WHERE active=1 ORDER BY id");
            assert_eq!(
                rows(&e, &sql),
                rows(&e, &sql.replace("FROM ft_inc", "FROM ft_oracle"))
            );
        }
        for table in ["ft_inc", "ft_oracle"] {
            e.execute(&format!(
                "UPDATE {table} SET body=CONCAT(body,'数据库'),active=1-active WHERE id=2"
            ))
            .unwrap();
            if round == 1 {
                e.execute(&format!("DELETE FROM {table} WHERE id=4"))
                    .unwrap();
            }
            assert_corpora(&e, &format!("test.{table}"), current(&e));
        }
    }
}

#[test]
fn fulltext_incremental_old_snapshot_concurrent_commit_and_own_writes() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = std::sync::Arc::new(open_test_engine(dir.path()));
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(2,'sapphire diamond',0)")
        .unwrap();
    let db = e.db_for_tests();
    let old = db.begin_read_only_txn().unwrap();
    assert_corpora(&e, "test.ft_inc", old);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let e = e.clone();
            scope.spawn(move || {
                for round in 0..8 {
                    let id = 10 + worker * 8 + round;
                    e.execute(&format!(
                        "INSERT INTO ft_inc VALUES({id},'quartz garnet',{})",
                        id % 2
                    ))
                    .unwrap();
                }
            });
        }
    });
    e.execute("UPDATE ft_inc SET body='zircon zircon',active=1 WHERE id=2")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", old);
    assert_corpora(&e, "test.ft_inc", current(&e));
    db.rollback_txn(old.txn_id).unwrap();
}

#[test]
fn fulltext_incremental_seeded_mixed_transactions_differential() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    let mut seed = 0x1ab5_709d_u64;
    let mut s = SessionContext::default();
    for round in 0..32 {
        e.execute_with_session(&mut s, "BEGIN").unwrap();
        for step in 0..6 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = (seed >> 32) % 17;
            let body = [
                "quartz quartz ruby",
                "sapphire diamond",
                "emerald zircon",
                "",
            ][(seed as usize >> 8) % 4];
            let active = ["1", "0", "NULL"][(seed as usize >> 16) % 3];
            let sql = match seed % 4 {
                0 => format!("DELETE FROM ft_inc WHERE id={id}"),
                1 => format!("UPDATE ft_inc SET body='{body}',active={active} WHERE id={id}"),
                _ => format!("REPLACE INTO ft_inc VALUES({id},'{body}',{active})"),
            };
            e.execute_with_session(&mut s, &sql).unwrap();
            if step == 2 {
                e.execute_with_session(&mut s, "SAVEPOINT middle").unwrap();
            }
        }
        if round % 3 == 0 {
            e.execute_with_session(&mut s, "ROLLBACK TO middle")
                .unwrap();
        }
        let own = e.db_for_tests().session_statement_view(
            s.current_txn.unwrap(),
            IsolationLevel::ReadCommitted,
            None,
        );
        assert_corpora(&e, "test.ft_inc", own);
        e.execute_with_session(&mut s, if round % 5 == 0 { "ROLLBACK" } else { "COMMIT" })
            .unwrap();
        assert_corpora(&e, "test.ft_inc", current(&e));
    }
}

#[test]
fn fulltext_incremental_strict_restart_preserves_statistics_without_rebuild() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let expected;
    {
        let e = open_strict_test_engine(dir.path());
        e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
        e.execute("INSERT INTO ft_inc VALUES(1,'quartz quartz ruby',1),(2,'quartz sapphire',0),(3,NULL,NULL)").unwrap();
        let mut s = SessionContext::default();
        for sql in [
            "BEGIN",
            "UPDATE ft_inc SET body='diamond diamond' WHERE id=2",
            "SAVEPOINT s",
            "DELETE FROM ft_inc WHERE id=1",
            "ROLLBACK TO s",
            "COMMIT",
            "BEGIN",
            "INSERT INTO ft_inc VALUES(4,'quartz',1)",
            "ROLLBACK",
        ] {
            e.execute_with_session(&mut s, sql).unwrap();
        }
        expected = rows(
            &e,
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 ORDER BY id",
        );
        assert_corpora(&e, "test.ft_inc", current(&e));
        e.shutdown().unwrap();
    }
    let e = open_strict_test_engine(dir.path());
    assert_corpora(&e, "test.ft_inc", current(&e));
    assert_eq!(
        rows(
            &e,
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 ORDER BY id"
        ),
        expected
    );
}

#[test]
fn fulltext_incremental_crash_child() {
    let Some(path) = std::env::var_os("MYDB_FT_COUNTER_CRASH_DIR") else {
        return;
    };
    let e = open_strict_test_engine(std::path::Path::new(&path));
    if std::env::var_os("MYDB_FT_STATISTICS_TEST_CRASH_AFTER_BUILD").is_some() {
        e.execute("CREATE FULLTEXT INDEX f ON ft_inc(body) WHERE active=1")
            .unwrap();
        panic!("expected build crash");
    }
    let mut s = SessionContext::default();
    for sql in [
        "BEGIN",
        "UPDATE ft_inc SET body='emerald emerald',active=0 WHERE id=1",
        "SAVEPOINT s",
        "DELETE FROM ft_inc WHERE id=2",
        "ROLLBACK TO s",
        "INSERT INTO ft_inc VALUES(4,'quartz ruby',1)",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    if std::env::var("MYDB_FT_COUNTER_CRASH_COMMIT").as_deref() == Ok("1") {
        e.execute_with_session(&mut s, "COMMIT").unwrap();
        e.db_for_tests().flush_dirty_data().unwrap();
    } else {
        // Make both data and counter before-images durable while uncommitted.
        e.db_for_tests().checkpoint().unwrap();
    }
    std::process::exit(86);
}

#[test]
fn fulltext_incremental_crash_recovers_commit_and_rolls_back_uncommitted_counters() {
    for committed in [false, true] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        {
            let e = open_strict_test_engine(dir.path());
            e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
            e.execute("INSERT INTO ft_inc VALUES(1,'quartz quartz ruby',1),(2,'quartz sapphire',0),(3,NULL,NULL)").unwrap();
            e.shutdown().unwrap();
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::fulltext_incremental::fulltext_incremental_crash_child",
                "--nocapture",
            ])
            .env("MYDB_FT_COUNTER_CRASH_DIR", dir.path())
            .env(
                "MYDB_FT_COUNTER_CRASH_COMMIT",
                if committed { "1" } else { "0" },
            )
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let e = open_strict_test_engine(dir.path());
        assert_corpora(&e, "test.ft_inc", current(&e));
        assert_eq!(rows(&e,"SELECT id FROM ft_inc WHERE active=1 AND MATCH(body) AGAINST('quartz') ORDER BY id"),vec![vec![Value::Int(if committed {4}else{1})]]);
        assert_eq!(
            rows(&e, "SELECT COUNT(*) FROM ft_inc")[0][0].to_display_string(),
            if committed { "4" } else { "3" }
        );
    }
}

#[test]
fn fulltext_incremental_physical_replication_commit_rollback_and_old_snapshot() {
    fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
        std::fs::create_dir_all(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let target = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let master_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let replica_dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let master = open_strict_test_engine(master_dir.path());
    master.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    master.execute("INSERT INTO ft_inc VALUES(1,'quartz quartz ruby',1),(2,'quartz sapphire',0),(3,NULL,NULL)").unwrap();
    let source = master.db_for_tests();
    source.checkpoint().unwrap();
    copy_tree(master_dir.path(), replica_dir.path());
    let mut role = crate::repl::role::RoleState::new_standalone();
    role.role = crate::repl::role::Role::Replica;
    crate::repl::role::save(replica_dir.path(), &role).unwrap();
    let replica = open_strict_test_engine(replica_dir.path());
    let target = replica.db_for_tests().clone();
    let old = target.begin_read_only_txn().unwrap();
    let original = rows(
        &replica,
        "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 ORDER BY id",
    );
    let log = source.wal_log();
    let mut start = log.lock().next_lsn();
    let mut applier = crate::repl::replica::applier::Applier::with_database(
        replica_dir.path(),
        target.wal_log(),
        target.clone(),
        crate::repl::replica::progress::ReplicaProgress::default(),
    );
    let mut s = SessionContext::default();
    for sql in [
        "BEGIN",
        "UPDATE ft_inc SET body='emerald emerald',active=1 WHERE id=2",
        "SAVEPOINT s",
        "DELETE FROM ft_inc WHERE id=1",
        "ROLLBACK TO s",
        "INSERT INTO ft_inc VALUES(4,'quartz ruby',1)",
        "COMMIT",
        "BEGIN",
        "UPDATE ft_inc SET body='diamond' WHERE id=1",
        "ROLLBACK",
    ] {
        master.execute_with_session(&mut s, sql).unwrap();
    }
    let end = log.lock().last_lsn();
    crate::wal::LogManager::sync_durable_to(&log, end).unwrap();
    let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
    while let Some(chunk) = reader.next_chunk(end, 1 << 20).unwrap() {
        applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
    }
    start = end + 1;
    assert_corpora(&replica, "test.ft_inc", old);
    assert_corpora(&replica, "test.ft_inc", current(&replica));
    assert_eq!(
        rows(
            &replica,
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 ORDER BY id"
        ),
        rows(
            &master,
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 ORDER BY id"
        )
    );
    assert_ne!(
        rows(
            &replica,
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 ORDER BY id"
        ),
        original
    );
    target.rollback_txn(old.txn_id).unwrap();
    master
        .execute("CREATE FULLTEXT INDEX f2 ON ft_inc(body)")
        .unwrap();
    let end = log.lock().last_lsn();
    crate::wal::LogManager::sync_durable_to(&log, end).unwrap();
    let mut reader = crate::wal::WalReader::open(&log, start).unwrap();
    while let Some(chunk) = reader.next_chunk(end, 1 << 20).unwrap() {
        applier.apply_chunk(&chunk, &role.node_uuid, 1).unwrap();
    }
    assert_corpora(&replica, "test.ft_inc", current(&replica));
    drop(applier);
    drop(target);
    replica.shutdown().unwrap();
    drop(replica);
    let replica = open_strict_test_engine(replica_dir.path());
    assert_corpora(&replica, "test.ft_inc", current(&replica));
}

#[test]
fn fulltext_incremental_load_cascade_and_xa_rollback() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_relaxed_test_engine(dir.path());
    e.execute("CREATE TABLE parent_ft(id INT PRIMARY KEY)")
        .unwrap();
    e.execute("INSERT INTO parent_ft VALUES(1),(2)").unwrap();
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,pid INT,body TEXT,active INT,FULLTEXT f(body) WHERE active=1,FOREIGN KEY(pid) REFERENCES parent_ft(id) ON DELETE CASCADE ON UPDATE CASCADE)").unwrap();
    let file = dir.path().join("rows.tsv");
    std::fs::write(
        &file,
        "1\t1\tquartz quartz ruby\t1\n2\t2\tquartz sapphire\t0\n3\t2\tdiamond emerald\t1\n",
    )
    .unwrap();
    e.execute(&format!(
        "LOAD DATA INFILE '{}' INTO TABLE ft_inc",
        file.display()
    ))
    .unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    e.execute("DELETE FROM parent_ft WHERE id=2").unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    let mut s = SessionContext::default();
    for sql in [
        "XA START 'ft_counters'",
        "INSERT INTO ft_inc VALUES(4,1,'garnet zircon',1)",
        "UPDATE ft_inc SET body='diamond' WHERE id=1",
        "XA END 'ft_counters'",
        "XA PREPARE 'ft_counters'",
    ] {
        e.execute_with_session(&mut s, sql).unwrap();
    }
    assert_corpora(&e, "test.ft_inc", current(&e));
    e.execute_with_session(&mut s, "XA ROLLBACK 'ft_counters'")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM ft_inc WHERE active=1 AND MATCH(body) AGAINST('quartz')"
        ),
        vec![vec![Value::Int(1)]]
    );
}

#[test]
fn fulltext_incremental_partition_moves_and_explicit_partition_corpus() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_relaxed_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT,body TEXT,active INT,FULLTEXT f(body) WHERE active=1) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    e.execute(
        "INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(11,'quartz sapphire',0),(12,NULL,NULL)",
    )
    .unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    e.execute("UPDATE ft_inc SET id=15,body='diamond diamond' WHERE id=1")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    let db = e.db_for_tests();
    let table = db.table("test.ft_inc").unwrap();
    let stats = db
        .fulltext_corpus_statistics(
            current(&e),
            &table,
            &table.indexes[0],
            &["quartz".into()],
            Some(&table.storage_ids()[..1]),
        )
        .unwrap();
    assert_eq!(
        (
            stats.documents,
            stats.total_tokens,
            stats.frequencies["quartz"]
        ),
        (0, 0, 0)
    );
    e.execute("ALTER TABLE ft_inc REORGANIZE PARTITION p1 INTO(PARTITION p1a VALUES LESS THAN(13),PARTITION p1b VALUES LESS THAN MAXVALUE)").unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    e.execute("ALTER TABLE ft_inc TRUNCATE PARTITION p1a")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
}

#[test]
fn fulltext_incremental_failed_build_identity_reuse_reinitializes_corpus() {
    for retry in [true, false] {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        {
            let e = open_strict_test_engine(dir.path());
            e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT)")
                .unwrap();
            e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1)")
                .unwrap();
            e.shutdown().unwrap();
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::executor::tests::fulltext_incremental::fulltext_incremental_crash_child",
                "--nocapture",
            ])
            .env("MYDB_FT_COUNTER_CRASH_DIR", dir.path())
            .env("MYDB_FT_STATISTICS_TEST_CRASH_AFTER_BUILD", "1")
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(87),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let e = open_strict_test_engine(dir.path());
        if !retry {
            let db = e.db_for_tests();
            let id = db.table("test.ft_inc").unwrap().id;
            db.set_recyclebin_variable("mydb_recyclebin", 0).unwrap();
            e.execute("DROP TABLE ft_inc").unwrap();
            let counters = crate::storage::data_dictionary::dd_table_schema(
                crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
            )
            .unwrap();
            // Even an unpublished build's orphan corpus must be retired
            // without requiring a public FULLTEXT descriptor at DROP time.
            assert!(!db
                .prefix_index_entries_visible(
                    &counters,
                    &counters.indexes[0],
                    crate::storage::IndexKey::from(vec![crate::storage::KeyPart::UInt(id)]),
                    current(&e),
                )
                .unwrap()
                .is_empty());
            for _ in 0..16 {
                if db.catalog_retirement_count_for_test() == 0 {
                    break;
                }
                db.maintain_catalog_retirements_until(|| false).unwrap();
            }
            assert!(db
                .prefix_index_entries_visible(
                    &counters,
                    &counters.indexes[0],
                    crate::storage::IndexKey::from(vec![crate::storage::KeyPart::UInt(id)]),
                    current(&e)
                )
                .unwrap()
                .is_empty());
            continue;
        }
        e.execute("INSERT INTO ft_inc VALUES(2,'quartz sapphire',0)")
            .unwrap();
        e.execute("CREATE FULLTEXT INDEX f ON ft_inc(body) WHERE active=1")
            .unwrap();
        assert_corpora(&e, "test.ft_inc", current(&e));
    }
}

#[test]
fn fulltext_incremental_exchange_transfers_counters_with_catalog_publication() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_relaxed_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT,body TEXT,active INT,FULLTEXT f(body) WHERE active=1) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    e.execute("CREATE TABLE ft_stage(id INT,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)")
        .unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(11,'garnet ruby',1)")
        .unwrap();
    e.execute("INSERT INTO ft_stage VALUES(2,'quartz sapphire',1),(3,'diamond emerald',0)")
        .unwrap();
    e.execute("CREATE TABLE anchor_ft(id INT PRIMARY KEY)")
        .unwrap();
    e.execute("INSERT INTO anchor_ft VALUES(1)").unwrap();
    for _ in 0..2 {
        let mut old = SessionContext::default();
        for sql in [
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN",
            "SELECT * FROM anchor_ft",
        ] {
            e.execute_with_session(&mut old, sql).unwrap();
        }
        e.execute("ALTER TABLE ft_inc EXCHANGE PARTITION p0 WITH TABLE ft_stage")
            .unwrap();
        assert_corpora(&e, "test.ft_inc", current(&e));
        assert_corpora(&e, "test.ft_stage", current(&e));
        let indexed="SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_stage WHERE active=1 AND MATCH(body) AGAINST('quartz') ORDER BY id";
        let heap = indexed.replace("active=1", "COALESCE(active,0)=1");
        // Ownership changed after this view. Both access paths must reject
        // it instead of combining the new heap binding with an old corpus.
        for query in [indexed, heap.as_str()] {
            assert_eq!(
                e.execute_with_session(&mut old, query)
                    .and_then(QueryResponse::materialize)
                    .unwrap_err()
                    .code(),
                1412
            );
        }
        e.execute_with_session(&mut old, "COMMIT").unwrap();

        e.execute(
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_stage WHERE active=1 ORDER BY id",
        )
        .unwrap();
    }
}

#[test]
fn fulltext_incremental_statistics_probe_work_is_bounded_as_corpus_grows() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    for batch in 0..16 {
        let values = (batch * 256..(batch + 1) * 256)
            .map(|id| {
                format!(
                    "({id},'{} sapphire diamond',{})",
                    if id == 0 { "quartz" } else { "cobalt" },
                    id % 2
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        e.execute(&format!("INSERT INTO ft_inc VALUES{values}"))
            .unwrap();
        let db = e.db_for_tests();
        let table = db.table("test.ft_inc").unwrap();
        crate::engine::perf_events::clear_statement_rows_examined();
        crate::engine::perf_events::start_statement_rows_examined();
        let stats = db
            .fulltext_corpus_statistics(
                current(&e),
                &table,
                &table.indexes[1],
                &["quartz".into()],
                None,
            )
            .unwrap();
        assert_eq!(stats.documents, (batch + 1) * 256);
        assert_eq!(stats.frequencies["quartz"], 1);
        assert!(
            stats.counter_rows_read <= 128,
            "{} counter rows for {} documents",
            stats.counter_rows_read,
            stats.documents
        );
        assert_eq!(crate::engine::perf_events::take_scan_rows_examined(), 0);
        crate::engine::perf_events::clear_statement_rows_examined();
    }
}

#[test]
fn fulltext_incremental_parallel_storage_writers_share_transaction_shard_safely() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    let db = e.db_for_tests();
    let txn = db.begin_txn().unwrap();
    std::thread::scope(|scope| {
        for worker in 0..4 {
            scope.spawn(move || {
                let rows = (0..64)
                    .map(|n| {
                        vec![
                            Value::Int(worker * 64 + n),
                            Value::String("quartz quartz sapphire".into()),
                            Value::Int(n % 2),
                        ]
                    })
                    .collect();
                db.insert_rows(txn, "test.ft_inc", rows).unwrap();
            });
        }
    });
    assert_corpora(&e, "test.ft_inc", txn);
    db.commit_txn(txn.txn_id).unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
}

#[test]
fn fulltext_incremental_ddl_retires_counters_without_breaking_old_snapshots() {
    use crate::storage::{IndexKey, KeyPart};
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_relaxed_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body),FULLTEXT fp(body) WHERE active=1)").unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(2,'quartz sapphire',0)")
        .unwrap();
    let db = e.db_for_tests();
    db.set_recyclebin_variable("mydb_recyclebin", 0).unwrap();
    let counters = crate::storage::data_dictionary::dd_table_schema(
        crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
    )
    .unwrap();
    let count = |prefix: Vec<u64>| {
        db.prefix_index_entries_visible(
            &counters,
            &counters.indexes[0],
            IndexKey::from(prefix.into_iter().map(KeyPart::UInt).collect::<Vec<_>>()),
            current(&e),
        )
        .unwrap()
        .len()
    };
    e.execute("DROP INDEX f ON ft_inc").unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    let old_table = db.table("test.ft_inc").unwrap();
    let old_index = &old_table.indexes[1];
    let old = db.begin_read_only_txn().unwrap();
    let old_prefix = vec![
        old_table.id,
        old_table.active_generation,
        old_index.fulltext.as_ref().unwrap().corpus_id,
    ];
    assert!(count(old_prefix.clone()) > 0);
    e.execute("DROP INDEX fp ON ft_inc").unwrap();
    for _ in 0..16 {
        if count(old_prefix.clone()) == 0 {
            break;
        }
        db.maintain_catalog_retirements_until(|| false).unwrap();
    }
    assert_eq!(count(old_prefix), 0);
    let frozen = db
        .fulltext_corpus_statistics(old, &old_table, old_index, &["quartz".into()], None)
        .unwrap();
    assert_eq!((frozen.documents, frozen.frequencies["quartz"]), (2, 2));
    db.rollback_txn(old.txn_id).unwrap();
    e.execute("CREATE FULLTEXT INDEX fn ON ft_inc(body) WHERE active=1")
        .unwrap();
    let before_copy = db.table("test.ft_inc").unwrap();
    let copy_snapshot = db.begin_read_only_txn().unwrap();
    let copy_statistics = db
        .fulltext_corpus_statistics(
            copy_snapshot,
            &before_copy,
            &before_copy.indexes[1],
            &["quartz".into()],
            None,
        )
        .unwrap();
    e.execute("OPTIMIZE TABLE ft_inc").unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
    let after_copy = db.table("test.ft_inc").unwrap();
    if before_copy.active_generation != after_copy.active_generation {
        for _ in 0..16 {
            if count(vec![before_copy.id, before_copy.active_generation]) == 0 {
                break;
            }
            db.maintain_catalog_retirements_until(|| false).unwrap();
        }
        assert_eq!(
            count(vec![before_copy.id, before_copy.active_generation]),
            0
        );
    }
    let frozen = db
        .fulltext_corpus_statistics(
            copy_snapshot,
            &before_copy,
            &before_copy.indexes[1],
            &["quartz".into()],
            None,
        )
        .unwrap();
    assert_eq!(frozen.documents, copy_statistics.documents);
    assert_eq!(frozen.total_tokens, copy_statistics.total_tokens);
    assert_eq!(frozen.frequencies, copy_statistics.frequencies);
    db.rollback_txn(copy_snapshot.txn_id).unwrap();
    e.execute("TRUNCATE TABLE ft_inc").unwrap();
    for _ in 0..16 {
        if count(vec![after_copy.id, after_copy.active_generation]) == 0 {
            break;
        }
        db.maintain_catalog_retirements_until(|| false).unwrap();
    }
    assert_eq!(count(vec![after_copy.id, after_copy.active_generation]), 0);
    e.execute("INSERT INTO ft_inc VALUES(3,'quartz',1)")
        .unwrap();
    e.execute("DROP TABLE ft_inc").unwrap();
    for _ in 0..16 {
        if count(vec![old_table.id]) == 0 {
            break;
        }
        db.maintain_catalog_retirements_until(|| false).unwrap();
    }
    assert_eq!(count(vec![old_table.id]), 0);
}

#[test]
fn fulltext_incremental_temporary_statistics_use_volatile_storage_and_owner_quota() {
    use crate::storage::{IndexKey, KeyPart};
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    for reopen in 0..2 {
        let e = open_strict_test_engine(dir.path());
        let mut session = SessionContext::default();
        session.temp_quota = Arc::new(crate::storage::temp_quota::SessionTempQuota::new(
            1,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
        ));
        e.execute_with_session(&mut session,"CREATE TEMPORARY TABLE ft_temp(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
        let before_dml = e.db_for_tests().wal_log().lock().last_lsn();
        e.execute_with_session(&mut session,"INSERT INTO ft_temp VALUES(1,'quartz quartz ruby',1),(2,'quartz sapphire',0),(3,NULL,NULL)").unwrap();
        assert_eq!(
            e.db_for_tests().wal_log().lock().last_lsn(),
            before_dml,
            "temporary statistics emitted persistent WAL"
        );
        let key = session.resolve_temp_table_name("ft_temp").unwrap();
        let db = e.db_for_tests();
        assert_corpora(&e, &key, current(&e));
        assert_eq!(
            session.temp_quota.usage().0,
            1,
            "internal counters consumed a user table slot"
        );
        let table = db.table(&key).unwrap();
        let dictionary = crate::storage::data_dictionary::dd_table_schema(
            crate::storage::data_dictionary::DD_FULLTEXT_COUNTERS,
        )
        .unwrap();
        assert!(
            db.prefix_index_entries_visible(
                &dictionary,
                &dictionary.indexes[0],
                IndexKey::from(vec![KeyPart::UInt(table.id)]),
                current(&e)
            )
            .unwrap()
            .is_empty(),
            "temporary counters leaked into persistent DD"
        );
        let QueryResponse::ResultSet(result) = e
            .execute_with_session(
                &mut session,
                "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_temp WHERE active=1 ORDER BY id",
            )
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(result.rows.len(), 1);
        for sql in [
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_temp WHERE active=0 AND MATCH(body) AGAINST('quartz')",
            "SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_temp WHERE active=1 FOR UPDATE",
        ] {
            let QueryResponse::ResultSet(result)=e.execute_with_session(&mut session,sql).unwrap().materialize().unwrap() else {panic!("rows")};
            assert_eq!(result.rows.len(),1);
        }
        for sql in [
            "BEGIN",
            "UPDATE ft_temp SET body='emerald',active=1 WHERE id=2",
            "SAVEPOINT s",
            "DELETE FROM ft_temp WHERE id=1",
            "ROLLBACK TO s",
        ] {
            e.execute_with_session(&mut session, sql).unwrap();
        }
        let own = db.session_statement_view(
            session.current_txn.unwrap(),
            IsolationLevel::ReadCommitted,
            None,
        );
        assert_corpora(&e, &key, own);
        e.execute_with_session(&mut session, "ROLLBACK").unwrap();
        assert_corpora(&e, &key, current(&e));
        let snapshot = db.snapshot_temp_table(&key).unwrap();
        e.execute_with_session(
            &mut session,
            "ALTER TABLE ft_temp MODIFY COLUMN body VARCHAR(100)",
        )
        .unwrap();
        assert_corpora(&e, &key, current(&e));
        db.restore_temp_table_snapshot(snapshot).unwrap();
        assert_corpora(&e, &key, current(&e));
        e.execute_with_session(&mut session, "TRUNCATE TABLE ft_temp")
            .unwrap();
        assert_corpora(&e, &key, current(&e));
        e.execute_with_session(
            &mut session,
            &format!(
                "INSERT INTO ft_temp VALUES({},'zircon zircon',1)",
                10 + reopen
            ),
        )
        .unwrap();
        assert_corpora(&e, &key, current(&e));
        e.execute_with_session(&mut session, "DROP TEMPORARY TABLE ft_temp")
            .unwrap();
        assert_eq!(
            (session.temp_quota.usage().0, session.temp_quota.usage().1),
            (0, 0)
        );
        e.shutdown().unwrap();
    }
}

#[test]
fn fulltext_incremental_rr_own_writes_reconcile_snapshot_and_current_updates() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1)")
        .unwrap();
    let mut session = SessionContext::default();
    for sql in [
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "BEGIN",
        "SELECT COUNT(*) FROM ft_inc",
    ] {
        e.execute_with_session(&mut session, sql).unwrap();
    }
    let db = e.db_for_tests();
    // Cover every counter shard with commits newer than the pinned view.
    for id in 2..66 {
        let txn = db.begin_txn().unwrap();
        db.insert_row(
            txn,
            "test.ft_inc",
            vec![
                Value::Int(id),
                Value::String("quartz emerald".into()),
                Value::Int(0),
            ],
        )
        .unwrap();
        db.commit_txn(txn.txn_id).unwrap();
    }
    e.execute_with_session(
        &mut session,
        "UPDATE ft_inc SET body='quartz diamond diamond' WHERE id=1",
    )
    .unwrap();
    let own = db.session_statement_view(
        session.current_txn.unwrap(),
        IsolationLevel::RepeatableRead,
        session.txn_snapshot_scn,
    );
    assert_corpora(&e, "test.ft_inc", own);
    e.execute_with_session(&mut session, "SAVEPOINT s").unwrap();
    e.execute_with_session(
        &mut session,
        "UPDATE ft_inc SET body='quartz garnet',active=1 WHERE id=2",
    )
    .unwrap();
    e.execute_with_session(&mut session, "DELETE FROM ft_inc WHERE id=3")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", own);
    e.execute_with_session(&mut session, "ROLLBACK TO s")
        .unwrap();
    assert_corpora(&e, "test.ft_inc", own);
    e.execute_with_session(&mut session, "COMMIT").unwrap();
    assert_corpora(&e, "test.ft_inc", current(&e));
}

#[test]
fn fulltext_incremental_rr_scoring_cache_isolates_writers_and_savepoint_rollback() {
    fn session_rows(e: &StorageQueryEngine, s: &mut SessionContext, sql: &str) -> Vec<Vec<Value>> {
        let QueryResponse::ResultSet(rs) = e
            .execute_with_session(s, sql)
            .unwrap()
            .materialize()
            .unwrap()
        else {
            panic!("rows")
        };
        rs.rows
    }
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let e = open_test_engine(dir.path());
    e.execute("CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT,FULLTEXT f(body) WHERE active=1)").unwrap();
    e.execute("INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(2,'quartz sapphire',1)")
        .unwrap();
    let mut a = SessionContext::default();
    let mut b = SessionContext::default();
    for session in [&mut a, &mut b] {
        for sql in [
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN",
            "SELECT COUNT(*) FROM ft_inc",
        ] {
            e.execute_with_session(session, sql).unwrap();
        }
    }
    assert_eq!(a.txn_snapshot_scn, b.txn_snapshot_scn);
    e.execute_with_session(
        &mut a,
        "UPDATE ft_inc SET body='quartz diamond diamond' WHERE id=1",
    )
    .unwrap();
    e.execute_with_session(
        &mut b,
        "UPDATE ft_inc SET body='quartz garnet garnet garnet' WHERE id=2",
    )
    .unwrap();
    let indexed="SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 AND MATCH(body) AGAINST('quartz') ORDER BY id";
    // COALESCE has the same result on this data but intentionally cannot
    // prove the partial predicate, selecting the independent heap oracle.
    let heap = indexed.replace("active=1", "COALESCE(active,0)=1");
    for session in [&mut a, &mut b] {
        assert_eq!(
            session_rows(&e, session, indexed),
            session_rows(&e, session, &heap)
        );
    }
    e.execute_with_session(&mut a, "SAVEPOINT s").unwrap();
    e.execute_with_session(
        &mut a,
        "UPDATE ft_inc SET body='quartz zircon zircon zircon zircon' WHERE id=1",
    )
    .unwrap();
    assert_eq!(
        session_rows(&e, &mut a, indexed),
        session_rows(&e, &mut a, &heap)
    );
    e.execute_with_session(&mut a, "ROLLBACK TO s").unwrap();
    assert_eq!(
        session_rows(&e, &mut a, indexed),
        session_rows(&e, &mut a, &heap)
    );
    for session in [&mut a, &mut b] {
        e.execute_with_session(session, "ROLLBACK").unwrap();
    }
    assert_corpora(&e, "test.ft_inc", current(&e));
}

#[test]
fn fulltext_incremental_rr_before_index_publication_uses_complete_visible_corpus() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    {
        let e = open_strict_test_engine(dir.path());
        for sql in ["CREATE TABLE anchor_ft(id INT PRIMARY KEY)","INSERT INTO anchor_ft VALUES(1)","CREATE TABLE ft_inc(id INT PRIMARY KEY,body TEXT,active INT)","INSERT INTO ft_inc VALUES(1,'quartz ruby',1),(2,'quartz sapphire',0),(3,'quartz cobalt',1)"] {e.execute(sql).unwrap();}
        let mut old = SessionContext::default();
        for sql in [
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN",
            "SELECT * FROM anchor_ft",
        ] {
            e.execute_with_session(&mut old, sql).unwrap();
        }
        for sql in [
            "DELETE FROM ft_inc WHERE id=3",
            "UPDATE ft_inc SET body='diamond' WHERE id=1",
            "INSERT INTO ft_inc VALUES(4,'quartz opal',1)",
            "CREATE FULLTEXT INDEX fp ON ft_inc(body) WHERE active=1",
            "CREATE FULLTEXT INDEX fc ON ft_inc(body)",
            "CREATE FULLTEXT INDEX ng ON ft_inc(body) WITH PARSER ngram WHERE active=1",
        ] {
            e.execute(sql).unwrap();
        }
        let table = e.db_for_tests().table("test.ft_inc").unwrap();
        for index in table.indexes.iter().filter(|index| index.is_fulltext()) {
            let origin = index.fulltext.as_ref().unwrap().snapshot_origin_txn_id;
            assert_ne!(origin, 0);
            assert_ne!(origin, u64::MAX);
        }
        let QueryResponse::ResultSet(rs)=e.execute_with_session(&mut old,"SELECT id,MATCH(body) AGAINST('quartz') s FROM ft_inc WHERE active=1 AND MATCH(body) AGAINST('quartz') ORDER BY id").unwrap().materialize().unwrap() else {panic!("rows")};
        assert_eq!(
            rs.rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
            vec![Value::Int(1), Value::Int(3)]
        );
        let expected = crate::storage::fulltext::bm25::idf(3., 3.);
        for row in &rs.rows {
            assert!((row[1].to_display_string().parse::<f64>().unwrap() - expected).abs() < 1e-12);
        }
        for sql in [
            "SELECT id FROM ft_inc WHERE MATCH(body) AGAINST('cob*' IN BOOLEAN MODE) LIMIT 1",
            "SELECT id FROM ft_inc WHERE active=1 AND body LIKE '%cob%' ORDER BY id",
        ] {
            let QueryResponse::ResultSet(rs) = e
                .execute_with_session(&mut old, sql)
                .unwrap()
                .materialize()
                .unwrap()
            else {
                panic!("rows")
            };
            assert_eq!(rs.rows, vec![vec![Value::Int(3)]]);
        }
        e.execute_with_session(&mut old, "COMMIT").unwrap();
        assert_corpora(&e, "test.ft_inc", current(&e));
        e.shutdown().unwrap();
    }
    let e = open_strict_test_engine(dir.path());
    assert_corpora(&e, "test.ft_inc", current(&e));
    assert_eq!(
        rows(
            &e,
            "SELECT id FROM ft_inc WHERE active=1 AND MATCH(body) AGAINST('quartz') ORDER BY id"
        ),
        vec![vec![Value::Int(4)]]
    );
}
