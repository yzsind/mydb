//! Regression coverage for HNSW semantics, snapshots and lifecycle boundaries.
use super::*;

#[test]
fn vector_audit_duplicate_cluster_preserves_distinct_directions() {
    for (metric, distance) in [
        ("l2", "L2_DISTANCE"),
        ("cosine", "COSINE_DISTANCE"),
        ("dot", "MAX_INNER_PRODUCT_DISTANCE"),
    ] {
        for (backend, mode, incremental) in [
            ("memory", "auto", false),
            ("memory", "auto", true),
            ("paged", "auto", false),
            ("paged", "chunked", false),
            ("paged", "auto", true),
        ] {
            let _env = HnswEnv::set(&[
                ("MYDB_VECTOR_STORAGE", backend),
                ("MYDB_HNSW_BULK_MODE", mode),
                ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
                ("MYDB_HNSW_BRUTE_FILTER_ROWS", "0"),
            ]);
            let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
            let engine = open_strict_test_engine(dir.path());
            engine
                .execute("CREATE TABLE audit_dup(id INT PRIMARY KEY,v VECTOR(4) NOT NULL)")
                .unwrap();
            let create =
                format!("CREATE INDEX vi ON audit_dup(v) USING HNSW WITH(metric={metric})");
            if incremental {
                engine.execute(&create).unwrap();
            }
            for start in (0..2400).step_by(200) {
                let values = (start..start + 200)
                    .map(|id| {
                        let vector = if id < 2280 {
                            "1,0,0,0".into()
                        } else {
                            format!("0,{},{},{}", 50 + id % 40, 51 + id % 30, 52 + id % 20)
                        };
                        format!("({id},'[{vector}]')")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                engine
                    .execute(&format!("INSERT INTO audit_dup VALUES{values}"))
                    .unwrap();
            }
            if !incremental {
                engine.execute(&create).unwrap();
            }
            let mut session = SessionContext::default();
            engine
                .execute_with_session(&mut session, "SET mydb_hnsw_ef_search=256")
                .unwrap();
            engine
                .execute_with_session(&mut session, "SET mydb_hnsw_ef_dynamic_factor=0")
                .unwrap();
            let query =
                format!("SELECT id FROM audit_dup ORDER BY {distance}(v,'[0,60,60,60]') LIMIT 100");
            for rebuilt in [false, true] {
                if rebuilt {
                    engine.execute("OPTIMIZE TABLE audit_dup").unwrap();
                }
                let found = id_list_session(&engine, &mut session, &query);
                assert_eq!(found.len(), 100);
                assert!(
                    found.iter().all(|&id| id >= 2280),
                    "{backend}/{mode}/{incremental}/{rebuilt}: {found:?}"
                );
            }
            engine.checkpoint().unwrap();
            drop(engine);
            let engine = open_strict_test_engine(dir.path());
            assert!(id_list_session(&engine, &mut session, &query)
                .iter()
                .all(|&id| id >= 2280));
        }
    }
}

#[test]
fn vector_audit_query_text_and_null_order_follow_scalar_semantics() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE audit_query(id INT PRIMARY KEY,v VECTOR(4))")
            .unwrap();
        engine.execute("INSERT INTO audit_query VALUES(1,'[1,0,0,0]'),(2,'[0,1,0,0]'),(3,NULL),(4,'[0,0,0,0]')").unwrap();
        for indexed in [false, true] {
            if indexed {
                engine
                    .execute("DELETE FROM audit_query WHERE v IS NULL")
                    .unwrap();
                engine
                    .execute("ALTER TABLE audit_query MODIFY v VECTOR(4) NOT NULL")
                    .unwrap();
                engine
                    .execute("CREATE INDEX vi ON audit_query(v) USING HNSW")
                    .unwrap();
            }
            for hint in ["", "/*+ NO_VECTOR_INDEX() */"] {
                assert!(engine.execute(&format!("SELECT {hint} id FROM audit_query ORDER BY L2_DISTANCE(v,'abcdefghijklmnop') LIMIT 2")).is_err(), "{backend}/{indexed}/{hint}: invalid text accepted");
                for distance in ["L2_DISTANCE", "COSINE_DISTANCE"] {
                    for nulls in ["FIRST", "LAST"] {
                        let order = format!("{distance}(v,'[1,0,0,0]') NULLS {nulls}");
                        let actual = id_list(
                            &engine,
                            &format!("SELECT {hint} id FROM audit_query ORDER BY {order} LIMIT 2"),
                        );
                        let expected = id_list(
                            &engine,
                            &format!(
                                "SELECT id FROM audit_query WHERE id>0 ORDER BY {order} LIMIT 2"
                            ),
                        );
                        assert_eq!(actual, expected, "{backend}/{indexed}/{hint}/{order}");
                    }
                }
            }
        }
        for sql in [
            "DROP INDEX vi ON audit_query",
            "DELETE FROM audit_query WHERE id=4",
            "CREATE INDEX vi ON audit_query(v) USING HNSW WITH(metric=cosine)",
        ] {
            engine.execute(sql).unwrap();
        }
        let cosine =
            "SELECT id FROM audit_query ORDER BY COSINE_DISTANCE(v,'[1,0,0,0]') NULLS LAST LIMIT 2";
        assert!(explain_mentions_vector_index(&engine, cosine));
        assert_eq!(id_list(&engine, cosine), [1, 2]);
    }
}

#[test]
fn vector_audit_rebuild_keeps_existing_reader_handle_valid() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_strict_test_engine(dir.path());
        engine
            .execute("CREATE TABLE audit_rebuild(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO audit_rebuild VALUES(1,'[1,0]'),(2,'[2,0]')")
            .unwrap();
        engine
            .execute("CREATE INDEX vi ON audit_rebuild(v) USING HNSW")
            .unwrap();
        let table = engine.database().table("audit_rebuild").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_vector())
            .unwrap();
        let old = engine
            .database()
            .vector_graph_get_or_build(&table, index)
            .unwrap();
        engine
            .database()
            .rebuild_vector_for_test(&table, index, table.id)
            .unwrap();
        old.ensure_valid().unwrap();
        engine.checkpoint().unwrap();
        let found = old
            .search(
                &crate::engine::vector::codec::from_f32(&[1.0, 0.0]),
                crate::engine::vector::VectorSearchRequest {
                    candidate_limit: 1,
                    rerank_limit: 1,
                    effort: crate::engine::vector::VectorSearchEffort::Hnsw { ef: 64 },
                },
            )
            .unwrap();
        assert_eq!(found.candidates.len(), 1);
        assert_eq!(found.candidates[0].1, 0.0);
    }
}

#[test]
fn vector_audit_cancelled_cold_load_preserves_finalized_file() {
    let _env = environment("paged", false);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
    engine
        .execute("CREATE TABLE audit_cold(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("INSERT INTO audit_cold VALUES(1,'[1,0]'),(2,'[2,0]')")
        .unwrap();
    engine
        .execute("CREATE INDEX vi ON audit_cold(v) USING HNSW")
        .unwrap();
    let table = engine.database().table("audit_cold").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.is_vector())
        .unwrap();
    engine.database().evict_table_vector_graphs(table.id);
    let db = engine.database().clone();
    let table_id = table.id;
    *crate::storage::VECTOR_BUILD_BEFORE_PUBLISH_HOOK.lock() = Some(Box::new(move || {
        db.evict_table_vector_graphs(table_id);
    }));
    assert!(engine
        .database()
        .vector_graph_get_or_build(&table, index)
        .is_err());
    engine.checkpoint().unwrap();
    engine
        .database()
        .vector_graph_get_or_build(&table, index)
        .unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(
        id_list(
            &engine,
            "SELECT id FROM audit_cold ORDER BY L2_DISTANCE(v,'[1,0]') LIMIT 1"
        ),
        [1]
    );
}

#[test]
fn vector_audit_replica_replaces_retirement_snapshots() {
    let _env = environment("paged", false);
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let engine = open_strict_test_engine(dir.path());
    engine.database().vector_reader_timeout_for_test();
    engine
        .execute("CREATE TABLE audit_retire(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
        .unwrap();
    engine
        .execute("CREATE INDEX vi ON audit_retire(v) USING HNSW")
        .unwrap();
    let table = engine.database().table("audit_retire").unwrap();
    let index = table
        .indexes
        .iter()
        .find(|index| index.is_vector())
        .unwrap();
    engine.database().set_physical_replica_mode(true).unwrap();
    for scn in 1..=64 {
        engine
            .database()
            .install_vector_retirement_snapshot_for_test(&table, index, scn / 8 + 1, Some(scn))
            .unwrap();
        assert_eq!(engine.database().pending_vector_retirements_for_test(), 1);
    }
    engine
        .database()
        .install_vector_retirement_snapshot_for_test(&table, index, 1, None)
        .unwrap();
    assert_eq!(engine.database().pending_vector_retirements_for_test(), 0);
    engine.checkpoint().unwrap();
    drop(engine);
    let engine = open_strict_test_engine(dir.path());
    assert_eq!(engine.database().pending_vector_retirements_for_test(), 0);
}

#[test]
fn vector_audit_checkpoint_skips_index_dropped_after_capture() {
    for backend in ["memory", "paged"] {
        let _env = environment(backend, false);
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = std::sync::Arc::new(open_strict_test_engine(dir.path()));
        engine
            .execute("CREATE TABLE audit_checkpoint(id INT PRIMARY KEY,v VECTOR(2) NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO audit_checkpoint VALUES(1,'[1,0]')")
            .unwrap();
        engine
            .execute("CREATE INDEX vi ON audit_checkpoint(v) USING HNSW")
            .unwrap();
        let ddl = engine.clone();
        *crate::storage::VECTOR_CHECKPOINT_AFTER_CAPTURE_HOOK.lock() = Some(Box::new(move || {
            ddl.execute("DROP INDEX vi ON audit_checkpoint").unwrap();
        }));
        engine.database().vector_graph_persist_all_dirty(0).unwrap();
        engine.checkpoint().unwrap();
    }
}

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
        engine.database().evict_table_vector_graphs(table.id);
        let scans = crate::storage::Database::vector_build_heap_scans_for_test();
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
        assert_eq!(
            crate::storage::Database::vector_build_heap_scans_for_test(),
            scans
        );
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
fn failed_paged_commit_returns_error_and_keeps_its_durable_fence() {
    use crate::engine::vector::graph::FAIL_PAGED_WRITES_FOR_TEST;
    use std::sync::atomic::Ordering;
    let _env = environment("paged", false);
    let dir = tempfile::tempdir().unwrap();
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
    let err = inserted.unwrap_err();
    assert!(err.message().contains("committed WAL decision"), "{err:?}");
    assert!(engine.database().ensure_operational().is_err());
    assert!(engine.checkpoint().is_err());
    let files = find_hvec_files(dir.path());
    assert_eq!(files.len(), 1);
    let evidence = std::fs::read(&files[0]).unwrap();
    drop(engine);
    let mut config = memory_test_storage_config();
    config.durability = crate::storage::DurabilityMode::Strict;
    assert!(
        StorageQueryEngine::open_with_storage_config_without_background_workers(dir.path(), config)
            .is_err()
    );
    assert_eq!(std::fs::read(&files[0]).unwrap(), evidence);
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
    let table = engine.database().table("items").unwrap();
    engine.database().evict_table_vector_graphs(table.id);
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
        let table = engine.database().table("items").unwrap();
        let index = table
            .indexes
            .iter()
            .find(|index| index.is_vector())
            .unwrap();
        engine
            .database()
            .rebuild_vector_for_test(&table, index, table.id)
            .unwrap();
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
