use super::*;
use std::collections::VecDeque;
use std::sync::Mutex;

enum Step {
    Batch(Batch),
    Barrier(Arc<std::sync::Barrier>),
    Error,
    Panic,
}
struct Input {
    schema: Arc<BatchSchema>,
    steps: VecDeque<Step>,
    threads: Arc<Mutex<Vec<std::thread::ThreadId>>>,
    dropped: Arc<AtomicUsize>,
}
impl BatchOperator for Input {
    fn schema(&self) -> &Arc<BatchSchema> {
        &self.schema
    }
    fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        let Some(step) = self.steps.pop_front() else {
            return Ok(None);
        };
        self.threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        match step {
            Step::Batch(batch) => Ok(Some(batch)),
            Step::Barrier(barrier) => {
                barrier.wait();
                self.next_batch()
            }
            Step::Error => {
                Err(crate::error::catalog::ER_OUT_OF_RESOURCES.error("source test failure"))
            }
            Step::Panic => panic!("source test panic"),
        }
    }
}
impl Drop for Input {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}
struct Worker {
    input: Option<Input>,
    pull: bool,
}
impl ParallelBatchWorker for Worker {
    fn take_coordinator_pull(&mut self, _: Arc<AtomicBool>) -> Option<Box<dyn BatchOperator>> {
        if self.pull {
            self.input
                .take()
                .map(|i| Box::new(i) as Box<dyn BatchOperator>)
        } else {
            None
        }
    }
    fn run(
        mut self: Box<Self>,
        consume: &mut dyn FnMut(Batch) -> Result<(), QueryError>,
        _: &AtomicBool,
    ) -> Result<(), QueryError> {
        let input = self.input.as_mut().expect("untransferred test input");
        while let Some(batch) = input.next_batch()? {
            consume(batch)?;
        }
        Ok(())
    }
}

fn schema() -> Arc<BatchSchema> {
    Arc::new(BatchSchema::new(vec![BatchField {
        table: String::new(),
        alias: None,
        name: "v".into(),
        column_type: ColumnType::LongLong,
        source: Default::default(),
    }]))
}
fn batch(schema: &Arc<BatchSchema>, base: i64) -> Batch {
    Batch::new(
        schema.clone(),
        vec![ColumnVector::I64 {
            data: (base..base + 4).collect(),
            nulls: None,
        }],
    )
    .with_selection(Selection::Indices(vec![2, 0, 2, 1]))
}
fn worker(
    schema: &Arc<BatchSchema>,
    steps: Vec<Step>,
    pull: bool,
    threads: &Arc<Mutex<Vec<std::thread::ThreadId>>>,
    dropped: &Arc<AtomicUsize>,
) -> Box<dyn ParallelBatchWorker> {
    Box::new(Worker {
        pull,
        input: Some(Input {
            schema: schema.clone(),
            steps: steps.into(),
            threads: threads.clone(),
            dropped: dropped.clone(),
        }),
    })
}
fn projection(
    db: Arc<Database>,
    schema: Arc<BatchSchema>,
    workers: Vec<Box<dyn ParallelBatchWorker>>,
    budget: Arc<mem_budget::MemBudget>,
) -> ParallelProjection {
    use crate::engine::expr_program::{ExprCompiler, InputColumn, InputSchema};
    let input = InputSchema::new(vec![InputColumn {
        table: String::new(),
        alias: None,
        name: "v".into(),
        column_type: ColumnType::LongLong,
        unsigned: false,
        collation_id: None,
    }]);
    let expr = crate::parser::parse_expr_fragment("v+1").unwrap();
    let program = ExprCompiler::compile(&expr, &input).unwrap();
    let context = super::super::super::execution_context::ExecutionContext::capture_with_budget(
        TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
        budget,
    )
    .fork_worker_cancellation();
    let charge = mem_budget::ControlledMemoryCharge::try_new_optional(
        mem_budget::ControlledMemoryCategory::StorageScratch,
        workers.len() * 4 * 1024 * 1024,
    )
    .expect("test worker stacks admitted");
    ParallelProjection {
        db,
        schema,
        projection: Arc::new(vec![VecResultProjection::Expr(Arc::new(program))]),
        pending: Some(workers),
        execution: ScanWorkers::new(context, "parallel projection worker panicked"),
        abort: Arc::new(AtomicBool::new(false)),
        active: Arc::new(AtomicUsize::new(0)),
        local: None,
        local_observed: false,
        prefer_peer: false,
        key: None,
        _charge: Arc::new(charge),
    }
}
fn database() -> (tempfile::TempDir, Arc<Database>) {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
    let db = Arc::new(Database::open(dir.path()).unwrap());
    (dir, db)
}

#[test]
fn coordinator_and_peers_each_consume_their_input_once_preserving_selection() {
    let (_dir, db) = database();
    let schema = schema();
    for local in [None, Some(0), Some(1), Some(2)] {
        let threads: Vec<_> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let dropped = Arc::new(AtomicUsize::new(0));
        let workers = (0..3)
            .map(|i| {
                worker(
                    &schema,
                    vec![
                        Step::Batch(batch(&schema, i as i64 * 10)),
                        Step::Batch(batch(&schema, i as i64 * 10 + 4)),
                    ],
                    local == Some(i),
                    &threads[i],
                    &dropped,
                )
            })
            .collect();
        let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024 * 1024));
        let mut projection = projection(db.clone(), schema.clone(), workers, budget.clone());
        let mut actual = Vec::new();
        while let Some(batch) = projection.next_batch().unwrap() {
            for row in batch.selection().unwrap().iter() {
                actual.push(batch.columns()[0].value_at(row));
            }
        }
        assert!(projection.next_batch().unwrap().is_none());
        let mut want = Vec::new();
        for base in [0, 4, 10, 14, 20, 24] {
            want.extend([base + 3, base + 1, base + 3, base + 2].map(Value::Int));
        }
        actual.sort_by_key(|v| match v {
            Value::Int(n) => *n,
            _ => panic!("integer"),
        });
        want.sort_by_key(|v| match v {
            Value::Int(n) => *n,
            _ => panic!("integer"),
        });
        assert_eq!(actual, want);
        assert_eq!(projection.active.load(Ordering::Relaxed), 3);
        assert_eq!(projection.execution.workers.len(), 0);
        let current = std::thread::current().id();
        for (i, threads) in threads.iter().enumerate() {
            let threads = threads.lock().unwrap();
            assert_eq!(threads.len(), 2);
            assert!(threads
                .iter()
                .all(|id| (*id == current) == (local == Some(i))));
        }
        drop(projection);
        assert_eq!(dropped.load(Ordering::Relaxed), 3);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn coordinator_error_panic_and_early_drop_disconnect_and_join_peers() {
    let (_dir, db) = database();
    let schema = schema();
    for failure in [0, 1, 2] {
        let dropped = Arc::new(AtomicUsize::new(0));
        let threads = Arc::new(Mutex::new(Vec::new()));
        let steps = match failure {
            0 => vec![Step::Error],
            1 => vec![Step::Panic],
            _ => vec![
                Step::Batch(batch(&schema, 0)),
                Step::Batch(batch(&schema, 4)),
            ],
        };
        let workers = vec![
            worker(&schema, steps, true, &threads, &dropped),
            worker(
                &schema,
                (0..64)
                    .map(|i| Step::Batch(batch(&schema, i * 10)))
                    .collect(),
                false,
                &threads,
                &dropped,
            ),
        ];
        let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024 * 1024));
        let mut projection = projection(db.clone(), schema.clone(), workers, budget.clone());
        if failure < 2 {
            let err = projection.next_batch().unwrap_err();
            if failure == 0 {
                assert!(err.to_string().contains("source test failure"));
            } else {
                assert!(err
                    .to_string()
                    .contains("parallel projection worker panicked"));
            }
            assert!(projection.next_batch().unwrap().is_none());
        } else {
            drop(projection.next_batch().unwrap().unwrap());
        }
        drop(projection);
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn coordinator_only_partition_empty_eof_and_cancellation_release_owners() {
    let (_dir, db) = database();
    let schema = schema();
    for scenario in 0..3 {
        let dropped = Arc::new(AtomicUsize::new(0));
        let threads = Arc::new(Mutex::new(Vec::new()));
        let steps = if scenario == 0 {
            vec![]
        } else {
            vec![Step::Batch(batch(&schema, 0))]
        };
        let workers = vec![worker(&schema, steps, true, &threads, &dropped)];
        let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024 * 1024));
        let mut projection = projection(db.clone(), schema.clone(), workers, budget.clone());
        if scenario == 2 {
            projection
                .execution
                .worker_context
                .cancellation()
                .cancel_local();
            assert_eq!(projection.next_batch().unwrap_err().code(), 1317);
        } else {
            assert_eq!(projection.next_batch().unwrap().is_some(), scenario == 1);
            assert!(projection.next_batch().unwrap().is_none());
            assert!(projection.next_batch().unwrap().is_none());
            assert!(projection.execution.workers.is_empty());
        }
        drop(projection);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn empty_local_progress_yields_to_an_already_ready_peer() {
    let (_dir, db) = database();
    let schema = schema();
    let dropped = Arc::new(AtomicUsize::new(0));
    let threads = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let empty = Batch::new(
        schema.clone(),
        vec![ColumnVector::I64 {
            data: vec![],
            nulls: None,
        }],
    );
    let workers = vec![
        worker(
            &schema,
            vec![
                Step::Barrier(barrier.clone()),
                Step::Batch(empty),
                Step::Error,
            ],
            true,
            &threads,
            &dropped,
        ),
        worker(
            &schema,
            vec![Step::Batch(batch(&schema, 10)), Step::Barrier(barrier)],
            false,
            &threads,
            &dropped,
        ),
    ];
    let budget = Arc::new(mem_budget::MemBudget::new(64 * 1024 * 1024));
    let mut projection = projection(db, schema, workers, budget.clone());
    let first = projection
        .next_batch()
        .expect("ready peer precedes later local failure")
        .unwrap();
    assert_eq!(first.columns()[0].value_at(2), Value::Int(13));
    drop(first);
    assert!(projection
        .next_batch()
        .unwrap_err()
        .to_string()
        .contains("source test failure"));
    drop(projection);
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
    assert_eq!(budget.used(), 0);
}
