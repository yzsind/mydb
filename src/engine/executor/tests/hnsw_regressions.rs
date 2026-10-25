//! Regression coverage for HNSW semantics, snapshots and lifecycle boundaries.
use super::*;

fn environment(backend: &str, cold: bool) -> HnswEnv {
    HnswEnv::set(&[
        ("MYDB_VECTOR_STORAGE", backend),
        ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
        ("MYDB_HNSW_COMPACT_DISABLE", "1"),
        ("MYDB_HNSW_BUILD_ON_DDL", if cold { "0" } else { "1" }),
    ])
}

#[test]
fn relational_topk_never_truncates_aggregate_distinct_join_or_window() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY, emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[0,0]'),(2,'[0,0]'),(3,'[1,0]'),(4,'[2,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
            "CREATE TABLE rhs(id INT PRIMARY KEY)",
            "INSERT INTO rhs VALUES(4)",
        ] {
            engine.execute(sql).unwrap();
        }
        let distinct = rows(
            &engine,
            "SELECT DISTINCT DISTANCE(emb,'[0,0]') d FROM items ORDER BY d LIMIT 3",
        );
        assert_eq!(distinct.len(), 3, "{backend}: {distinct:?}");
        assert_eq!(
            cell(
                &engine,
                "SELECT COUNT(*) FROM items GROUP BY emb ORDER BY DISTANCE(emb,'[0,0]') LIMIT 1"
            ),
            "2"
        );
        assert_eq!(id_list(&engine,"SELECT items.id FROM items JOIN rhs ON items.id=rhs.id ORDER BY DISTANCE(items.emb,'[0,0]') LIMIT 1"),[4]);
        assert_eq!(
            cell(
                &engine,
                "SELECT COUNT(*) OVER() FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 1"
            ),
            "4"
        );
    }
}

#[test]
fn inplace_update_preserves_old_snapshot_nearest_neighbor() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[0,0]'),(2,'[10,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
        ] {
            engine.execute(sql).unwrap();
        }
        let query = "SELECT id FROM items ORDER BY DISTANCE(emb,'[4.9,0]') LIMIT 1";
        let mut reader = SessionContext::default();
        engine
            .execute_with_session(
                &mut reader,
                "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            )
            .unwrap();
        engine.execute_with_session(&mut reader, "BEGIN").unwrap();
        assert_eq!(id_list_session(&engine, &mut reader, query), [1]);
        engine
            .execute("UPDATE items SET emb='[-4,0]' WHERE id=1")
            .unwrap();
        assert_eq!(id_list(&engine, query), [2]);
        assert_eq!(
            id_list_session(&engine, &mut reader, query),
            [1],
            "{backend}"
        );
        engine.execute_with_session(&mut reader, "COMMIT").unwrap();
    }
}

#[test]
fn cold_build_resolves_uncommitted_delete_and_update_undo() {
    for backend in ["memory", "paged"] {
        for mutation in [
            "DELETE FROM items WHERE id=1",
            "UPDATE items SET emb='[1000,0]' WHERE id=1",
        ] {
            let _env = environment(backend, true);
            let dir = tempfile::tempdir().unwrap();
            let engine = open_strict_test_engine(dir.path());
            for sql in [
                "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
                "INSERT INTO items VALUES(1,'[0,0]'),(2,'[10,0]')",
                "CREATE INDEX hv ON items(emb) USING HNSW",
            ] {
                engine.execute(sql).unwrap();
            }
            let mut writer = SessionContext::default();
            engine.execute_with_session(&mut writer, "BEGIN").unwrap();
            engine.execute_with_session(&mut writer, mutation).unwrap();
            let query = "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 2";
            assert_eq!(id_list(&engine, query), [1, 2], "{backend}/{mutation}");
            engine
                .execute_with_session(&mut writer, "ROLLBACK")
                .unwrap();
            assert_eq!(id_list(&engine, query), [1, 2]);
        }
    }
}

#[test]
fn duplicate_vectors_keep_full_effort_coverage_and_filtered_row_targets() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)")
            .unwrap();
        let values = (1..=256)
            .map(|id| format!("({id},'[1,1]')"))
            .collect::<Vec<_>>()
            .join(",");
        engine
            .execute(&format!("INSERT INTO items VALUES{values}"))
            .unwrap();
        engine
            .execute("CREATE INDEX hv ON items(emb) USING HNSW")
            .unwrap();
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM items ORDER BY DISTANCE(emb,'[1,1]') LIMIT 256"
            )
            .len(),
            256,
            "{backend}"
        );
        let found = id_list(
            &engine,
            "SELECT id FROM items WHERE id>200 ORDER BY DISTANCE(emb,'[1,1]') LIMIT 5",
        );
        assert_eq!(found.len(), 5, "{backend}");
        assert!(found.iter().all(|&id| id > 200));
        // Exercise incremental duplicate insertion after the cold topology.
        engine
            .execute("INSERT INTO items VALUES(999,'[1,1]')")
            .unwrap();
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM items WHERE id=999 ORDER BY DISTANCE(emb,'[1,1]') LIMIT 1"
            ),
            [999]
        );
    }
}

#[test]
fn finite_extremes_have_valid_distances_and_index_ordering() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        let l2: f64 = cell(&engine, "SELECT L2_DISTANCE('[3e30,0]','[0,0]')")
            .parse()
            .unwrap();
        assert!((l2 / 3e30 - 1.0).abs() < 1e-6);
        let cosine: f64 = cell(&engine, "SELECT COSINE_DISTANCE('[1e30,1e30]','[1e30,0]')")
            .parse()
            .unwrap();
        assert!((cosine - (1.0 - 2f64.sqrt().recip())).abs() < 1e-6);
        assert_eq!(
            cell(&engine, "SELECT COSINE_DISTANCE('[1e-30,0]','[1e-30,0]')"),
            "0"
        );
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[3e30,0]'),(2,'[2e30,0]'),(3,'[1e30,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
        ] {
            engine.execute(sql).unwrap();
        }
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 3"
            ),
            [3, 2, 1],
            "{backend}"
        );
        assert_eq!(id_list(&engine,"SELECT /*+ HNSW_EF(18446744073709551615) */ id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 1"),[3]);
        engine
            .execute("CREATE INDEX hd ON items(emb) USING HNSW WITH (metric=dot)")
            .unwrap();
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM items ORDER BY DISTANCE(emb,'[1e30,0]','DOT') LIMIT 3"
            ),
            [1, 2, 3]
        );
        for sql in [
            "CREATE TABLE directions(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO directions VALUES(1,'[1e30,0]'),(2,'[0,1e-30]'),(3,'[-1e30,0]')",
            "CREATE INDEX hc ON directions(emb) USING HNSW WITH (metric=cosine)",
        ] {
            engine.execute(sql).unwrap();
        }
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM directions ORDER BY COSINE_DISTANCE(emb,'[1e-30,0]') LIMIT 3"
            ),
            [1, 2, 3]
        );
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM directions ORDER BY COSINE_DISTANCE(emb,'[0,1e30]') LIMIT 1"
            ),
            [2]
        );
    }
}

#[test]
fn paged_layout_is_rejected_before_ddl_publication() {
    let _env = environment("paged", false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine
        .execute("CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(3500) NOT NULL)")
        .unwrap();
    let error = engine
        .execute("CREATE INDEX hv ON items(emb) USING HNSW")
        .unwrap_err();
    assert!(error.message().contains("capacity"), "{error:?}");
}

#[test]
fn paged_commit_markers_and_borrowed_handle_invalidation_are_coherent() {
    let _env = environment("paged", false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    for sql in [
        "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
        "INSERT INTO items VALUES(1,'[1,0]')",
        "CREATE INDEX hv ON items(emb) USING HNSW",
    ] {
        engine.execute(sql).unwrap();
    }
    let table = engine.database().table("items").unwrap();
    let index = table.indexes.iter().find(|i| i.is_vector()).unwrap();
    let graph = engine
        .database()
        .vector_graph_get_or_build(&table, index)
        .unwrap();
    let crate::engine::vector::VectorGraph::Paged(paged) = graph.as_ref() else {
        panic!("paged graph required")
    };
    graph.begin_transaction_delta(123).unwrap();
    graph.begin_transaction_delta(456).unwrap();
    assert!(!paged.is_finalized());
    graph.finish_committed_delta(123, 1).unwrap();
    assert!(!paged.is_finalized());
    graph.finish_committed_delta(456, 2).unwrap();
    assert!(paged.is_finalized());
    graph.invalidate();
    assert!(graph
        .search(
            &crate::engine::vector::codec::from_f32(&[0.0, 0.0]),
            crate::engine::vector::VectorSearchRequest {
                candidate_limit: 1,
                rerank_limit: 1,
                effort: crate::engine::vector::VectorSearchEffort::Hnsw { ef: 64 }
            }
        )
        .is_err());
}

#[test]
fn concurrent_cold_readers_share_one_build_owner() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, true);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[1,0]'),(2,'[2,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
        ] {
            engine.execute(sql).unwrap();
        }
        let table = engine.database().table("items").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|i| i.is_vector())
            .unwrap()
            .clone();
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *crate::storage::VECTOR_BUILD_BEFORE_PUBLISH_HOOK.lock() = Some(Box::new(move || {
            reached_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }));
        let db = engine.database().clone();
        let first_table = table.clone();
        let first_index = index.clone();
        let first =
            std::thread::spawn(move || db.vector_graph_get_or_build(&first_table, &first_index));
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let db = engine.database().clone();
        let second = std::thread::spawn(move || {
            let result = db.vector_graph_get_or_build(&table, &index);
            finished_tx.send(()).unwrap();
            result
        });
        let completed_before_owner =
            finished_rx.recv_timeout(std::time::Duration::from_millis(100));
        release_tx.send(()).unwrap();
        let first = first.join().unwrap().unwrap();
        let second = second.join().unwrap().unwrap();
        assert!(
            completed_before_owner.is_err(),
            "{backend}: second builder bypassed owner"
        );
        assert!(std::sync::Arc::ptr_eq(&first, &second));
    }
}

#[test]
fn partitioned_cold_build_resolves_undo_in_the_owning_storage() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, true);
        let dir = tempfile::tempdir().unwrap();
        let engine = open_strict_test_engine(dir.path());
        for sql in ["CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)","INSERT INTO items VALUES(1,'[1000,0]'),(11,'[0,0]')","CREATE INDEX hv ON items(emb) USING HNSW"] {engine.execute(sql).unwrap();}
        let mut writer = SessionContext::default();
        engine.execute_with_session(&mut writer, "BEGIN").unwrap();
        engine
            .execute_with_session(&mut writer, "DELETE FROM items WHERE id=11")
            .unwrap();
        assert_eq!(
            id_list(
                &engine,
                "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 1"
            ),
            [11],
            "{backend}"
        );
        engine
            .execute_with_session(&mut writer, "ROLLBACK")
            .unwrap();
    }
}

#[test]
fn interrupted_paged_commit_rebuilds_after_wal_recovery() {
    use crate::engine::vector::graph::FAIL_PAGED_WRITES_FOR_TEST;
    use std::sync::atomic::Ordering;
    let _env = environment("paged", false);
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[10,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
        ] {
            engine.execute(sql).unwrap();
        }
        FAIL_PAGED_WRITES_FOR_TEST.store(true, Ordering::Release);
        let inserted = engine.execute("INSERT INTO items VALUES(2,'[0,0]')");
        FAIL_PAGED_WRITES_FOR_TEST.store(false, Ordering::Release);
        inserted.unwrap();
        engine.database().wal_log().lock().flush().unwrap();
        std::mem::forget(engine);
    }
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 2"
        ),
        [2, 1]
    );
}

#[test]
fn memory_snapshot_with_only_tombstones_is_not_relinked_as_a_fresh_graph() {
    let _env = environment("memory", false);
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open_strict_test_engine(dir.path());
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[1,0]'),(2,'[2,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
            "DELETE FROM items",
        ] {
            engine.execute(sql).unwrap();
        }
        engine.checkpoint().unwrap();
    }
    let engine = open_strict_test_engine(dir.path());
    engine.database().vector_graph_warm_all_for_test();
    assert!(id_list(
        &engine,
        "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 1"
    )
    .is_empty());
    assert_eq!(
        engine
            .database()
            .hnsw_stats_for_test("items", "hv")
            .unwrap()
            .1,
        0
    );
    engine
        .execute("INSERT INTO items VALUES(3,'[3,0]')")
        .unwrap();
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 1"
        ),
        [3]
    );
}

#[test]
fn partition_fanout_respects_one_global_candidate_budget() {
    let _env = environment("paged", true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.execute("CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL) PARTITION BY RANGE(id)(PARTITION p0 VALUES LESS THAN(10),PARTITION p1 VALUES LESS THAN MAXVALUE)").unwrap();
    let values = (0..20)
        .map(|id| format!("({id},'[{id},0]')"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(&format!("INSERT INTO items VALUES{values}"))
        .unwrap();
    engine
        .execute("CREATE INDEX hv ON items(emb) USING HNSW")
        .unwrap();
    let mut session = SessionContext::default();
    engine
        .execute_with_session(&mut session, "SET mydb_hnsw_max_scan_tuples=16")
        .unwrap();
    let found = id_list_session(
        &engine,
        &mut session,
        "SELECT id FROM items ORDER BY DISTANCE(emb,'[0,0]') LIMIT 10",
    );
    assert_eq!(found, (0..10).collect::<Vec<_>>());
    assert!(
        engine.database().vector_index_runtime_stats().is_empty(),
        "fanout exceeding the global budget should finish exactly before starting cold graphs"
    );
}

#[test]
fn cold_build_commit_tail_cannot_reorder_an_existing_snapshot() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, true);
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
        for sql in [
            "CREATE TABLE items(id INT PRIMARY KEY,emb VECTOR(2) NOT NULL)",
            "INSERT INTO items VALUES(1,'[0,0]'),(2,'[10,0]')",
            "CREATE INDEX hv ON items(emb) USING HNSW",
        ] {
            engine.execute(sql).unwrap();
        }
        let mut reader = SessionContext::default();
        for sql in [
            "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN",
            "SELECT COUNT(*) FROM items",
        ] {
            engine.execute_with_session(&mut reader, sql).unwrap();
        }
        let writer = engine.clone();
        *crate::storage::VECTOR_BUILD_BEFORE_PUBLISH_HOOK.lock() = Some(Box::new(move || {
            writer
                .execute("UPDATE items SET emb='[-4,0]' WHERE id=1")
                .unwrap();
        }));
        let query = "SELECT id FROM items ORDER BY DISTANCE(emb,'[4.9,0]') LIMIT 1";
        assert_eq!(
            id_list_session(&engine, &mut reader, query),
            [1],
            "{backend}: old snapshot after cold replay"
        );
        engine.execute_with_session(&mut reader, "COMMIT").unwrap();
        assert_eq!(id_list(&engine, query), [2]);
    }
}
