use super::support::query_rows;
use super::*;

fn fixture() -> (tempfile::TempDir, StorageQueryEngine) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_test_engine(dir.path());
    engine.execute("CREATE TABLE sampled (id INT PRIMARY KEY, k INT, s VARCHAR(256), d DECIMAL(18,3), j JSON)").unwrap();
    engine.execute("INSERT INTO sampled SELECT value, value%7, REPEAT('a', 100), value/1000, JSON_OBJECT('id',value) FROM generate_series(1,2048) AS g(value)").unwrap();
    (dir, engine)
}

#[test]
fn tablesample_exact_counts_filters_expressions_and_repeatability() {
    let (_dir, engine) = fixture();
    for vectorized in ["off", "on"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(vectorized))], || {
            for method in ["SYSTEM", "BERNOULLI"] {
                for n in [0, 1, 37, 2048, 3000] {
                    let sql = format!("SELECT /*+ NO_PARALLEL */ id,k FROM sampled TABLESAMPLE {method} ({n} ROWS) REPEATABLE (40+2) ORDER BY id");
                    let rows = query_rows(&engine, &sql);
                    assert_eq!(rows.len(), n.min(2048));
                    assert_eq!(rows, query_rows(&engine, &sql));
                }
                let sample =
                    format!("FROM sampled TABLESAMPLE {method} (100 ROWS) REPEATABLE (42)");
                let all = query_rows(&engine, &format!("SELECT id,k {sample} ORDER BY id"));
                let expected: Vec<_> = all.into_iter().filter(|r| r[1] == Value::Int(3)).collect();
                assert_eq!(
                    query_rows(
                        &engine,
                        &format!("SELECT id,k {sample} WHERE k=3 ORDER BY id")
                    ),
                    expected
                );
                let count = query_rows(
                    &engine,
                    &format!("SELECT COUNT(*), MIN(id), MAX(id) {sample}"),
                );
                assert_eq!(count[0][0].to_display_string(), "100");
                let all_columns = query_rows(&engine, &format!("SELECT * {sample} ORDER BY id"));
                assert_eq!(all_columns.len(), 100);
                assert!(all_columns.iter().all(|r| r.len() == 5));
            }
            assert_eq!(
                query_rows(&engine, "SELECT COUNT(*) FROM sampled TABLESAMPLE (100)")[0][0]
                    .to_display_string(),
                "2048"
            );
            assert_eq!(
                query_rows(&engine, "SELECT COUNT(*) FROM sampled TABLESAMPLE (0)")[0][0]
                    .to_display_string(),
                "0"
            );
            let rows = query_rows(
                &engine,
                "SELECT id FROM sampled TABLESAMPLE ROW (10 PERCENT) REPEATABLE (42) ORDER BY id",
            );
            assert!((100..320).contains(&rows.len()), "{}", rows.len());
        });
    }
}

#[test]
fn tablesample_parallel_partitions_and_plan_diagnostics() {
    let (_dir, engine) = fixture();
    engine.execute("CREATE TABLE sampled_p (id INT, k INT, s VARCHAR(256)) PARTITION BY HASH(k) PARTITIONS 3").unwrap();
    engine
        .execute("INSERT INTO sampled_p SELECT id,k,s FROM sampled")
        .unwrap();
    for vectorized in ["off", "on"] {
        with_test_env(
            &[
                ("MYDB_VECTORIZED", Some(vectorized)),
                ("MYDB_PARALLEL_DISABLE", None),
            ],
            || {
                for table in ["sampled", "sampled_p"] {
                    for method in ["SYSTEM", "BERNOULLI"] {
                        let sql = format!("SELECT /*+ PARALLEL(4) */ id FROM {table} TABLESAMPLE {method} (123 ROWS) REPEATABLE (42) ORDER BY id");
                        let before = crate::engine::executor::select::sampling::SAMPLE_WORKER_TASKS
                            .load(AtomicOrdering::Relaxed);
                        let rows = query_rows(&engine, &sql);
                        assert_eq!(rows.len(), 123);
                        assert_eq!(rows, query_rows(&engine, &sql));
                        assert!(
                            crate::engine::executor::select::sampling::SAMPLE_WORKER_TASKS
                                .load(AtomicOrdering::Relaxed)
                                > before
                        );
                        let tree = explain_analyze_tree(&engine, &sql);
                        assert!(tree.contains("sample scan"), "{tree}");
                        assert!(tree.contains("TABLESAMPLE"), "{tree}");
                        engine
                            .execute(&format!("EXPLAIN FORMAT=JSON {sql}"))
                            .unwrap();
                    }
                }
                let explicit = query_rows(&engine,"SELECT /*+ PARALLEL(4) */ id FROM sampled_p PARTITION(p0) TABLESAMPLE (40 ROWS) REPEATABLE (4)");
                assert_eq!(explicit.len(), 40);
                let expected = query_rows(&engine, "SELECT id FROM sampled_p PARTITION(p0)");
                assert!(explicit.iter().all(|r| expected.contains(r)));
            },
        );
    }
}

#[test]
fn tablesample_prepared_parameters_and_invalid_arguments() {
    let (_dir, engine) = fixture();
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,"PREPARE s FROM 'SELECT id FROM sampled TABLESAMPLE BERNOULLI (? ROWS) REPEATABLE (? + 1) ORDER BY id'").unwrap();
    for n in [0, 1, 37, 257] {
        engine
            .execute_with_session(&mut session, &format!("SET @n={n}, @seed=41"))
            .unwrap();
        let result =
            support::execute_materialized(&engine, &mut session, "EXECUTE s USING @n,@seed")
                .unwrap();
        let rows = support::result_rows(result);
        assert_eq!(rows.len(), n);
        let again =
            support::execute_materialized(&engine, &mut session, "EXECUTE s USING @n,@seed")
                .unwrap();
        assert_eq!(rows, support::result_rows(again));
    }
    for clause in [
        "(-1)",
        "(101)",
        "(100.000000000000000000000000000001)",
        "(NULL)",
        "('10')",
        "(1.5 ROWS)",
        "(-1 ROWS)",
        "(18446744073709551616 ROWS)",
        "(id)",
        "(RAND())",
        "((SELECT 1))",
        "(10) REPEATABLE (NULL)",
        "(10) REPEATABLE ('42')",
        "(10) REPEATABLE (id)",
        "('10'+1)",
        "(CAST(10 AS CHAR)+1)",
    ] {
        let sql = format!("SELECT id FROM sampled TABLESAMPLE {clause} LIMIT 0");
        let error = engine.execute(&sql).unwrap_err();
        assert!(error.message().contains("TABLESAMPLE"), "{sql}: {error}");
    }
    engine
        .execute("CREATE VIEW sample_view AS SELECT * FROM sampled")
        .unwrap();
    for sql in [
        "SELECT * FROM sample_view TABLESAMPLE (10)",
        "WITH q AS (SELECT * FROM sampled) SELECT * FROM q TABLESAMPLE (10)",
        "SELECT * FROM information_schema.tables TABLESAMPLE (10)",
        "SELECT * FROM sampled USE INDEX(PRIMARY) TABLESAMPLE (10)",
    ] {
        assert!(engine.execute(sql).is_err(), "{sql}");
    }
}

#[test]
fn tablesample_fresh_samples_numeric_casts_and_large_row_limits() {
    let (_dir, engine) = fixture();
    let sql = "WITH q AS (SELECT id FROM sampled TABLESAMPLE BERNOULLI (37 ROWS)) SELECT id FROM q ORDER BY id";
    let first = query_rows(&engine, sql);
    assert!(query_rows(&engine, sql) != first || query_rows(&engine, sql) != first);
    for clause in [
        "(CAST('100' AS UNSIGNED) PERCENT) REPEATABLE(CAST(42 AS DOUBLE))",
        "BERNOULLI (18446744073709551615 ROWS) REPEATABLE (42)",
    ] {
        assert_eq!(
            query_rows(
                &engine,
                &format!("SELECT COUNT(*) FROM sampled TABLESAMPLE {clause}")
            )[0][0]
                .to_display_string(),
            "2048"
        );
    }
    let mut session = SessionContext::default();
    engine.execute_with_session(&mut session,"PREPARE l FROM 'SELECT id FROM sampled TABLESAMPLE BERNOULLI (0 ROWS) WHERE id=? FOR UPDATE'").unwrap();
    engine
        .execute_with_session(&mut session, "SET @id=1")
        .unwrap();
    assert!(support::result_rows(
        support::execute_materialized(&engine, &mut session, "EXECUTE l USING @id").unwrap()
    )
    .is_empty());
}

#[test]
fn tablesample_joins_nested_sources_rowid_and_cache_invalidation() {
    let (_dir, engine) = fixture();
    for method in ["SYSTEM", "BERNOULLI"] {
        let source = format!("sampled TABLESAMPLE {method} (73 ROWS) REPEATABLE (42)");
        let expected = query_rows(&engine, &format!("SELECT id FROM {source} ORDER BY id"));
        let joined = query_rows(&engine, &format!("SELECT a.id FROM sampled a JOIN sampled b TABLESAMPLE {method} (73 ROWS) REPEATABLE (42) ON a.id=b.id ORDER BY a.id"));
        assert_eq!(joined, expected);
        let derived = query_rows(
            &engine,
            &format!("SELECT id FROM (SELECT id FROM {source}) q ORDER BY id"),
        );
        assert_eq!(derived, expected);
        let cte = query_rows(
            &engine,
            &format!("WITH q AS (SELECT id FROM {source}) SELECT id FROM q ORDER BY id"),
        );
        assert_eq!(cte, expected);
        let addresses = query_rows(
            &engine,
            &format!("SELECT ROWID,id FROM {source} ORDER BY id"),
        );
        assert_eq!(addresses.len(), 73);
        assert_eq!(
            addresses
                .into_iter()
                .map(|r| vec![r[1].clone()])
                .collect::<Vec<_>>(),
            expected
        );
    }
    engine.execute("CREATE VIEW internal_sample AS SELECT id FROM sampled TABLESAMPLE BERNOULLI (73 ROWS) REPEATABLE (42)").unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT COUNT(*) FROM internal_sample")[0][0].to_display_string(),
        "73"
    );
    let sql = "SELECT id FROM sampled TABLESAMPLE BERNOULLI (100 PERCENT) ORDER BY id";
    assert_eq!(query_rows(&engine, sql).len(), 2048);
    engine
        .execute("ALTER TABLE sampled ADD extra INT DEFAULT 7")
        .unwrap();
    assert_eq!(query_rows(&engine, sql).len(), 2048);
    assert!(query_rows(
        &engine,
        "SELECT extra FROM sampled TABLESAMPLE SYSTEM (13 ROWS)"
    )
    .iter()
    .all(|r| r[0] == Value::Int(7)));
}

#[test]
fn tablesample_mvcc_keeps_visible_root_rows_and_transaction_writes() {
    let (_dir, engine) = fixture();
    let mut reader = SessionContext::default();
    let mut writer = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION")
        .unwrap();
    let sql =
        "SELECT id,k FROM sampled TABLESAMPLE BERNOULLI (71 ROWS) REPEATABLE (42) ORDER BY id";
    let before =
        support::result_rows(support::execute_materialized(&engine, &mut reader, sql).unwrap());
    engine
        .execute_with_session(&mut writer, "START TRANSACTION")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "UPDATE sampled SET k=99 WHERE id<=1024")
        .unwrap();
    engine
        .execute_with_session(&mut writer, "DELETE FROM sampled WHERE id>1024")
        .unwrap();
    for method in ["SYSTEM", "BERNOULLI"] {
        let own = format!("SELECT COUNT(*),MIN(k),MAX(k) FROM sampled TABLESAMPLE {method} (10000 ROWS) REPEATABLE (42)");
        let row = support::result_rows(
            support::execute_materialized(&engine, &mut writer, &own).unwrap(),
        );
        assert_eq!(
            row[0]
                .iter()
                .map(Value::to_display_string)
                .collect::<Vec<_>>(),
            ["1024", "99", "99"]
        );
    }
    let during =
        support::result_rows(support::execute_materialized(&engine, &mut reader, sql).unwrap());
    assert_eq!(before, during);
    engine.execute_with_session(&mut writer, "COMMIT").unwrap();
    let after =
        support::result_rows(support::execute_materialized(&engine, &mut reader, sql).unwrap());
    assert_eq!(before, after);
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert_eq!(
        query_rows(&engine, "SELECT COUNT(*) FROM sampled TABLESAMPLE (100)")[0][0]
            .to_display_string(),
        "1024"
    );
}

#[test]
fn tablesample_zero_io_and_block_percentage_preserves_pages() {
    let (_dir, engine) = fixture();
    use crate::engine::executor::select::sampling::SAMPLE_PAGES;
    let before = SAMPLE_PAGES.load(AtomicOrdering::Relaxed);
    for clause in [
        "SYSTEM (0)",
        "BERNOULLI (0)",
        "SYSTEM (0 ROWS)",
        "BERNOULLI (0 ROWS)",
    ] {
        assert!(query_rows(
            &engine,
            &format!("SELECT * FROM sampled TABLESAMPLE {clause}")
        )
        .is_empty());
    }
    assert_eq!(before, SAMPLE_PAGES.load(AtomicOrdering::Relaxed));
    let all = query_rows(&engine, "SELECT ROWID,id FROM sampled");
    let sample = query_rows(
        &engine,
        "SELECT ROWID,id FROM sampled TABLESAMPLE SYSTEM (25) REPEATABLE (42)",
    );
    assert!(!sample.is_empty() && sample.len() < all.len());
    let page = |r: &Vec<Value>| r[0].to_display_string().parse::<u64>().unwrap() >> 16;
    let pages: std::collections::HashSet<_> = sample.iter().map(page).collect();
    assert_eq!(
        sample.len(),
        all.iter().filter(|r| pages.contains(&page(r))).count()
    );
}

#[test]
fn tablesample_spills_exact_row_candidates_and_releases_resources() {
    let (_dir, engine) = fixture();
    engine.execute("INSERT INTO sampled SELECT value,value%7,'b',value/1000,JSON_OBJECT('id',value) FROM generate_series(2049,30000) g(value)").unwrap();
    let before = spill_quota::sort_spill_count();
    let previous = mem_budget::query_mem_limit_bytes().div_ceil(1024 * 1024);
    mem_budget::set_query_mem_limit_mb(1).unwrap();
    for hint in ["NO_PARALLEL", "PARALLEL(4)"] {
        let sql = format!("SELECT /*+ {hint} */ COUNT(*) FROM sampled TABLESAMPLE BERNOULLI (25000 ROWS) REPEATABLE(42)");
        assert_eq!(query_rows(&engine, &sql)[0][0].to_display_string(), "25000");
    }
    let prior_disk_limit = spill_quota::per_query_limit_bytes();
    let prior_spilled = spill_quota::global_spilled_bytes();
    spill_quota::set_per_query_limit_bytes(1);
    let failure = engine
        .execute("SELECT COUNT(*) FROM sampled TABLESAMPLE BERNOULLI (25000 ROWS) REPEATABLE(42)");
    spill_quota::set_per_query_limit_bytes(prior_disk_limit);
    assert_eq!(failure.unwrap_err().code(), 1041);
    assert_eq!(spill_quota::global_spilled_bytes(), prior_spilled);
    mem_budget::set_query_mem_limit_mb(previous).unwrap();
    assert!(spill_quota::sort_spill_count() > before);
    for _ in 0..8 {
        assert_eq!(
            query_rows(
                &engine,
                "SELECT /*+ PARALLEL(4) */ id FROM sampled TABLESAMPLE SYSTEM (25000 ROWS) LIMIT 1"
            )
            .len(),
            1
        );
    }
    assert_eq!(
        query_rows(&engine, "SELECT COUNT(*) FROM sampled")[0][0].to_display_string(),
        "30000"
    );
}

#[test]
fn tablesample_locking_reads_preserve_the_sample() {
    let (_dir, engine) = fixture();
    let mut session = SessionContext::default();
    for method in ["SYSTEM", "BERNOULLI"] {
        let source =
            format!("FROM sampled TABLESAMPLE {method} (31 ROWS) REPEATABLE(42) ORDER BY id");
        let expected = query_rows(&engine, &format!("SELECT id {source}"));
        engine
            .execute_with_session(&mut session, "START TRANSACTION")
            .unwrap();
        let actual = support::execute_materialized(
            &engine,
            &mut session,
            &format!("SELECT id {source} FOR UPDATE"),
        )
        .unwrap();
        assert_eq!(support::result_rows(actual), expected);
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
        engine
            .execute_with_session(&mut session, "START TRANSACTION")
            .unwrap();
        let actual = support::execute_materialized(&engine, &mut session, &format!("SELECT a.id FROM sampled a JOIN sampled b TABLESAMPLE {method} (31 ROWS) REPEATABLE(42) ON a.id=b.id ORDER BY a.id FOR UPDATE")).unwrap();
        assert_eq!(support::result_rows(actual), expected);
        engine
            .execute_with_session(&mut session, "ROLLBACK")
            .unwrap();
    }
}

#[test]
fn tablesample_argument_warnings_are_evaluated_once_on_cold_and_warm_plans() {
    let (_dir, engine) = fixture();
    let mut session = SessionContext::default();
    for _ in 0..3 {
        support::execute_materialized(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM sampled TABLESAMPLE (CAST('bad' AS UNSIGNED))",
        )
        .unwrap();
        let warnings = support::result_rows(
            support::execute_materialized(&engine, &mut session, "SHOW WARNINGS").unwrap(),
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert_eq!(warnings[0][1].to_display_string(), "1292");
    }
}

#[test]
fn tablesample_release_audit_unqualified_ddl_and_missing_relation() {
    let (_dir, engine) = fixture();
    let mut session = SessionContext::default();
    for sql in [
        "CREATE DATABASE sample_audit",
        "USE sample_audit",
        "CREATE TABLE sampled(id INT PRIMARY KEY)",
        "INSERT INTO sampled VALUES(1),(2),(3)",
        "CREATE TABLE dst AS SELECT id FROM sampled TABLESAMPLE SYSTEM(2 ROWS) REPEATABLE(1)",
        "CREATE TEMPORARY TABLE tmp AS SELECT id FROM sampled TABLESAMPLE SYSTEM(2 ROWS)",
        "CREATE VIEW v AS SELECT id FROM sampled TABLESAMPLE SYSTEM(2 ROWS)",
        "CREATE OR REPLACE VIEW v AS SELECT id FROM sampled TABLESAMPLE SYSTEM(2 ROWS)",
        "ALTER VIEW v AS SELECT id FROM sampled TABLESAMPLE SYSTEM(1 ROWS)",
        "DO (SELECT COUNT(*) FROM sampled TABLESAMPLE SYSTEM(2 ROWS))",
    ] {
        engine
            .execute_with_session(&mut session, sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for sql in [
        "CREATE VIEW badtmp AS SELECT id FROM tmp TABLESAMPLE(100)",
        "CREATE VIEW badtmp AS SELECT id FROM sampled WHERE id IN(SELECT id FROM tmp)",
        "CREATE OR REPLACE VIEW v AS SELECT id FROM tmp TABLESAMPLE(100)",
        "ALTER VIEW v AS SELECT id FROM tmp TABLESAMPLE(100)",
    ] {
        let err = engine.execute_with_session(&mut session, sql).unwrap_err();
        assert!(
            err.is(crate::error::catalog::ER_VIEW_SELECT_TMPTABLE),
            "{sql}: {err:?}"
        );
    }
    for (table, count) in [("dst", 2), ("tmp", 2), ("v", 1)] {
        let rs = engine
            .execute_with_session(&mut session, &format!("SELECT COUNT(*) FROM {table}"))
            .unwrap()
            .materialize()
            .unwrap();
        let QueryResponse::ResultSet(rs) = rs else {
            panic!()
        };
        assert_eq!(rs.rows[0][0].to_display_string(), count.to_string());
    }
    let err = engine
        .execute_with_session(&mut session, "SELECT * FROM absent TABLESAMPLE(10)")
        .unwrap_err();
    assert!(err.is(crate::error::catalog::ER_NO_SUCH_TABLE), "{err:?}");
    for sql in [
        "WITH z AS(SELECT * FROM sampled) SELECT * FROM z TABLESAMPLE(10)",
        "WITH z AS(SELECT * FROM sampled) SELECT sampled.id FROM sampled JOIN z TABLESAMPLE(10) ON sampled.id=z.id",
        "SELECT * FROM v TABLESAMPLE(10)",
        "SELECT * FROM information_schema.tables TABLESAMPLE(10)",
    ] {
        let err = engine.execute_with_session(&mut session, sql).unwrap_err();
        assert!(err.is(crate::error::catalog::ER_NOT_SUPPORTED_YET), "{sql}: {err:?}");
    }
}

#[test]
fn tablesample_row_small_samples_and_limits_only_probe_block_prefixes() {
    let (_dir, engine) = fixture();
    use crate::engine::executor::select::sampling::SAMPLE_PAGES;
    for vectorized in ["off", "on"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(vectorized))], || {
            for clause in ["ROW(2 ROWS)", "BERNOULLI(2 ROWS)"] {
                let before = SAMPLE_PAGES.load(AtomicOrdering::Relaxed);
                let rows = query_rows(&engine,&format!("SELECT /*+ NO_PARALLEL */ ROWID,id,s,d,j FROM sampled TABLESAMPLE {clause} REPEATABLE(42)"));
                assert_eq!(rows.len(), 2);
                let page = |r: &Vec<Value>| r[0].to_display_string().parse::<u64>().unwrap() >> 16;
                assert_ne!(page(&rows[0]), page(&rows[1]));
                assert!(SAMPLE_PAGES.load(AtomicOrdering::Relaxed) - before <= 4);
            }
            for percent in [1, 100] {
                let sql = format!("SELECT /*+ NO_PARALLEL */ id FROM sampled TABLESAMPLE ROW({percent} PERCENT) REPEATABLE(42)");
                let full = query_rows(&engine, &sql);
                assert!(!full.is_empty());
                let before = SAMPLE_PAGES.load(AtomicOrdering::Relaxed);
                let prefix = query_rows(&engine, &format!("{sql} LIMIT 1"));
                assert_eq!(prefix, full[..1]);
                assert!(SAMPLE_PAGES.load(AtomicOrdering::Relaxed) - before <= 8);
            }
            let tree = explain_analyze_tree(
                &engine,
                "SELECT /*+ NO_PARALLEL */ * FROM sampled TABLESAMPLE ROW(2 ROWS) REPEATABLE(42)",
            );
            assert!(tree.contains("sampler=block_round_row"), "{tree}");
            assert!(tree.contains("sample_decoded_rows=2"), "{tree}");
        });
    }
}

#[test]
fn tablesample_full_percent_consumers_keep_membership_limits_and_history() {
    let (_dir, engine) = fixture();
    for vectorized in ["off", "on"] {
        with_test_env(&[("MYDB_VECTORIZED", Some(vectorized))], || {
            let source = "FROM sampled TABLESAMPLE ROW(25 PERCENT) REPEATABLE(42)";
            let raw = query_rows(&engine, &format!("SELECT id,k {source}"));
            let sum = raw
                .iter()
                .map(|r| r[0].to_display_string().parse::<u64>().unwrap())
                .sum::<u64>();
            let filtered = raw.iter().filter(|r| r[1] == Value::Int(3)).count();
            for hint in ["NO_PARALLEL", "PARALLEL(4)"] {
                let aggregated = query_rows(
                    &engine,
                    &format!("SELECT /*+ {hint} */ COUNT(*),SUM(id) {source}"),
                );
                assert_eq!(aggregated[0][0].to_display_string(), raw.len().to_string());
                assert_eq!(aggregated[0][1].to_display_string(), sum.to_string());
                assert_eq!(
                    query_rows(
                        &engine,
                        &format!("SELECT /*+ {hint} */ COUNT(*) {source} WHERE k=3")
                    )[0][0]
                        .to_display_string(),
                    filtered.to_string()
                );
            }
            let prefix_sum = raw
                .iter()
                .take(3)
                .map(|r| r[0].to_display_string().parse::<u64>().unwrap())
                .sum::<u64>();
            assert_eq!(
                query_rows(
                    &engine,
                    &format!("SELECT SUM(id) FROM (SELECT id {source} LIMIT 3) q")
                )[0][0]
                    .to_display_string(),
                prefix_sum.to_string()
            );
            let tree = explain_analyze_tree(&engine, &format!("SELECT COUNT(*) {source}"));
            assert!(tree.contains("sampler=row_percent_scan"), "{tree}");
        });
    }
    let mut reader = SessionContext::default();
    engine
        .execute_with_session(
            &mut reader,
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        )
        .unwrap();
    engine
        .execute_with_session(&mut reader, "START TRANSACTION")
        .unwrap();
    let sql = "SELECT COUNT(*) FROM sampled TABLESAMPLE ROW(100 PERCENT) REPEATABLE(42)";
    let count = |reader: &mut SessionContext| {
        support::result_rows(support::execute_materialized(&engine, reader, sql).unwrap())[0][0]
            .to_display_string()
    };
    assert_eq!(count(&mut reader), "2048");
    engine.execute("DELETE FROM sampled WHERE id>1024").unwrap();
    assert_eq!(count(&mut reader), "2048");
    engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    assert_eq!(query_rows(&engine, sql)[0][0].to_display_string(), "1024");
}
