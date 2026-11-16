//! Parallel clone/open coverage for the dictionary bootstrap authority.
use super::*;
use crate::engine::executor::vec::config::{with_vec_mode, VecMode};

#[test]
fn catalog_seed_parallel_reopen_preserves_authority_and_window_results() {
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..16 {
                    let mut previous = None;
                    for mode in [VecMode::On, VecMode::Off] {
                        let rows = with_vec_mode(mode, || {
                            let dir = tempfile::tempdir().unwrap();
                            let engine = match StorageQueryEngine::open_with_storage_config_without_background_workers(
                                dir.path(), memory_test_storage_config(),
                            ) {
                                Ok(engine) => engine,
                                Err(error) => {
                                    let preserved = dir.keep();
                                    panic!("catalog open failed: {error:?}; fixture retained at {}", preserved.display());
                                }
                            };
                            engine.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
                            engine.execute("INSERT INTO t VALUES (1,-3),(2,NULL),(3,7),(4,-2)").unwrap();
                            let QueryResponse::ResultSet(result) = engine.execute(
                                "SELECT id, SUM(v) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM t ORDER BY id",
                            ).unwrap() else { panic!("expected rows"); };
                            result.rows
                        });
                        if let Some(expected) = previous.as_ref() {
                            assert_eq!(&rows, expected);
                        }
                        previous = Some(rows);
                    }
                }
            });
        }
    });
}
