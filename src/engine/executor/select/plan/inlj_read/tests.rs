use super::*;

fn rows() -> Vec<RowId> {
    (0..4)
        .flat_map(|page_id| (0..170).map(move |slot_id| RowId { page_id, slot_id }))
        .collect()
}

#[test]
fn inlj_read_tasks_preserve_pages_order_parameters_and_filtering() {
    let rows = rows();
    assert_eq!(
        page_aligned_row_tasks(&rows, 128),
        vec![(0, 170), (170, 340), (340, 510), (510, 680)]
    );
    assert!(page_aligned_row_tasks(&[], 128).is_empty());
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let txn = TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted);
    for degree in [2, 4] {
        crate::util::env::with_vars(&[("MYDB_INLJ_PROBE_TASK_ROWS", Some("128"))], || {
            crate::engine::params::with_auto_params(vec![Value::Int(37)], || {
                let caller = std::thread::current().id();
                let output = read_inlj_row_tasks(&db, txn, &rows, degree, "test worker", |chunk| {
                    assert_ne!(std::thread::current().id(), caller);
                    if degree > 1 {
                        assert!(chunk.iter().all(|id| id.page_id == chunk[0].page_id));
                        if chunk[0].page_id == 0 {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                    }
                    assert!(crate::engine::params::auto_parameterized_active());
                    Ok(chunk
                        .iter()
                        .filter(|id| id.slot_id % 3 == 0)
                        .map(|id| (*id, vec![crate::engine::params::current_param(0).unwrap()]))
                        .collect())
                })
                .unwrap();
                assert_eq!(
                    output,
                    rows.iter()
                        .filter(|id| id.slot_id % 3 == 0)
                        .map(|id| (*id, vec![Value::Int(37)]))
                        .collect::<Vec<_>>()
                );
            });
        });
    }
}

#[test]
fn inlj_read_workers_join_on_error_and_observe_cancellation() {
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    let txn = TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted);
    crate::util::env::with_vars(
        &[
            ("MYDB_INLJ_PROBE_PARALLELISM", Some("4")),
            ("MYDB_INLJ_PROBE_TASK_ROWS", Some("128")),
        ],
        || {
            let active = std::sync::atomic::AtomicUsize::new(0);
            let barrier = std::sync::Barrier::new(4);
            let result = read_inlj_row_tasks(&db, txn, &rows(), 4, "test worker", |chunk| {
                active.fetch_add(1, Ordering::SeqCst);
                barrier.wait();
                if chunk[0].page_id != 0 {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                active.fetch_sub(1, Ordering::SeqCst);
                Err(crate::error::catalog::ER_BAD_FIELD_ERROR.error("read callback failed"))
            });
            assert!(result
                .unwrap_err()
                .is(crate::error::catalog::ER_BAD_FIELD_ERROR));
            assert_eq!(
                active.load(Ordering::SeqCst),
                0,
                "all scoped workers joined before returning"
            );
            let token = crate::engine::cancel::QueryCancelToken::noop();
            token.cancel_local();
            let _scope = crate::engine::cancel::enter(token);
            let result = read_inlj_row_tasks(&db, txn, &rows(), 4, "test worker", |_| {
                panic!("cancelled worker must not read");
            });
            assert!(result
                .unwrap_err()
                .is(crate::error::catalog::ER_QUERY_INTERRUPTED));
        },
    );
}
