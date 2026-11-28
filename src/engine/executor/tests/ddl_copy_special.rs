use super::vector_type::HnswEnv;
use super::*;

#[test]
fn mixed_copy_adds_ann_with_committed_tail_on_all_storage_shapes() {
    for backend in ["memory", "paged"] {
        let _env = HnswEnv::set(&[
            ("MYDB_VECTOR_STORAGE", backend),
            ("MYDB_HNSW_BRUTE_THRESHOLD", "0"),
        ]);
        for partition in ["", " PARTITION BY HASH(id) PARTITIONS 2"] {
            for algo in ["HNSW", "IVF WITH(nlist=1)"] {
                for memory in [false, true] {
                    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
                    let e = Arc::new(if memory {
                        open_complete_memory_test_engine(dir.path())
                    } else {
                        open_strict_test_engine(dir.path())
                    });
                    e.execute(&format!(
                        "CREATE TABLE t(id INT PRIMARY KEY,n INT,v VECTOR(2) NOT NULL){partition}"
                    ))
                    .unwrap();
                    e.execute(
                        "INSERT INTO t VALUES(1,10,'[1,1]'),(2,20,'[20,20]'),(3,30,'[30,30]')",
                    )
                    .unwrap();
                    let hook = e.database().install_ddl_backfill_test_hook();
                    let worker = e.clone();
                    let sql=format!("ALTER TABLE t MODIFY n BIGINT,ADD INDEX ix(v) USING {algo},ALGORITHM=COPY,LOCK=NONE");
                    let ddl = std::thread::spawn(move || worker.execute(&sql));
                    let reached = hook.wait_until_reached(Duration::from_secs(10));
                    let mut s = SessionContext::default();
                    s.connection_id = 38901;
                    s.sysvars.lock_wait_timeout = Some(0);
                    let writes = [
                        "BEGIN",
                        "UPDATE t SET v='[9,9]',n=40 WHERE id=1",
                        "DELETE FROM t WHERE id=3",
                        "INSERT INTO t VALUES(4,50,'[1,1]')",
                        "SAVEPOINT s",
                        "UPDATE t SET v='[99,99]' WHERE id=4",
                        "ROLLBACK TO s",
                        "COMMIT",
                    ]
                    .map(|sql| e.execute_with_session(&mut s, sql));
                    hook.release();
                    let result = ddl.join().unwrap();
                    assert!(reached, "{backend} {partition} {algo} {memory}: {result:?}");
                    for write in writes {
                        write.unwrap();
                    }
                    result.unwrap();
                    let query =
                        "SELECT id FROM t FORCE INDEX(ix) ORDER BY L2_DISTANCE(v,'[1,1]') LIMIT 1";
                    assert_eq!(
                        super::support::query_rows(&e, query),
                        vec![vec![Value::Int(4)]]
                    );
                    if !memory {
                        drop(e);
                        let e = open_strict_test_engine(dir.path());
                        assert_eq!(
                            super::support::query_rows(&e, query),
                            vec![vec![Value::Int(4)]]
                        );
                        e.execute("UPDATE t SET v='[0,0]' WHERE id=4").unwrap();
                    }
                }
            }
        }
    }
}
