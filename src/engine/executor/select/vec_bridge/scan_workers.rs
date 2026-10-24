//! Shared lifecycle for channel-backed native scan workers.
use super::*;

pub(super) struct ScanWorkers {
    pub(super) receiver: Option<std::sync::mpsc::Receiver<Result<QueuedScanBatch, QueryError>>>,
    pub(super) workers: Vec<std::thread::JoinHandle<()>>,
    pub(super) txn_pressure: Option<crate::util::exec_permit::ParallelReadPressureGuard>,
    pub(super) worker_context: super::super::execution_context::ExecutionContext,
    pub(super) finished: bool,
    panic_message: &'static str,
}

impl ScanWorkers {
    pub(super) fn new(
        worker_context: super::super::execution_context::ExecutionContext,
        panic_message: &'static str,
    ) -> Self {
        Self {
            receiver: None,
            workers: Vec::new(),
            txn_pressure: None,
            worker_context,
            finished: false,
            panic_message,
        }
    }

    pub(super) fn with_channel(
        mut self,
        receiver: std::sync::mpsc::Receiver<Result<QueuedScanBatch, QueryError>>,
        workers: Vec<std::thread::JoinHandle<()>>,
    ) -> Self {
        self.receiver = Some(receiver);
        self.workers = workers;
        self
    }

    fn finish(&mut self) -> Result<(), QueryError> {
        self.finished = true;
        // Disconnect before joining: a queued batch owns its memory charge,
        // and a producer must never wait for a consumer that has exited.
        self.receiver.take();
        let panicked = self
            .workers
            .drain(..)
            .fold(false, |panicked, worker| panicked | worker.join().is_err());
        self.txn_pressure.take();
        if panicked {
            Err(crate::storage::error(
                crate::error::catalog::ER_UNKNOWN_ERROR,
                self.panic_message,
            ))
        } else {
            Ok(())
        }
    }

    pub(super) fn next_batch(&mut self) -> Result<Option<Batch>, QueryError> {
        if self.finished {
            return Ok(None);
        }
        match self.receiver.as_ref().expect("scan workers started").recv() {
            Ok(Ok(batch)) => Ok(Some(batch.batch)),
            Ok(Err(error)) => {
                self.worker_context.cancellation().cancel_local();
                let _ = self.finish();
                Err(error)
            }
            Err(_) => {
                self.finish()?;
                Ok(None)
            }
        }
    }
}

impl Drop for ScanWorkers {
    fn drop(&mut self) {
        if !self.finished {
            self.worker_context.cancellation().cancel_local();
            let _ = self.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn context(
        budget: Arc<mem_budget::MemBudget>,
    ) -> super::super::super::execution_context::ExecutionContext {
        super::super::super::execution_context::ExecutionContext::capture_with_budget(
            TxnView::read_only(0, crate::storage::IsolationLevel::ReadCommitted),
            budget,
        )
        .fork_worker_cancellation()
    }

    #[test]
    fn drop_disconnects_full_queue_joins_worker_and_releases_charges() {
        let budget = Arc::new(mem_budget::MemBudget::new(4096));
        let ctx = context(budget.clone());
        let parent = ctx.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let schema = Arc::new(BatchSchema::new(Vec::new()));
        mem_budget::with_query_budget(budget.clone(), || {
            tx.send(Ok(QueuedScanBatch {
                batch: Batch::with_row_count(schema, Vec::new(), 0),
                _charge: mem_budget::force_charge_scoped(128).unwrap(),
            }))
            .unwrap_or_else(|_| panic!("queue unexpectedly closed"));
        });
        let joined = Arc::new(AtomicBool::new(false));
        let flag = joined.clone();
        let worker = std::thread::spawn(move || {
            assert!(tx
                .send(Err(crate::engine::cancel::interrupted_error()))
                .is_err());
            flag.store(true, Ordering::Release);
        });
        let group = ScanWorkers::new(ctx, "test worker panicked").with_channel(rx, vec![worker]);
        assert_eq!(budget.used(), 128);
        drop(group);
        assert!(joined.load(Ordering::Acquire));
        assert_eq!(budget.used(), 0);
        assert!(parent.cancellation().check().is_err());
    }

    #[test]
    fn reports_worker_panic_but_preserves_first_query_error() {
        for query_error in [false, true] {
            let budget = Arc::new(mem_budget::MemBudget::new(4096));
            let (tx, rx) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                if query_error {
                    tx.send(Err(crate::engine::cancel::interrupted_error()))
                        .unwrap_or_else(|_| panic!("queue unexpectedly closed"));
                }
                panic!("injected worker failure");
            });
            let mut group = ScanWorkers::new(context(budget), "test worker panicked")
                .with_channel(rx, vec![worker]);
            let error = group.next_batch().unwrap_err();
            assert_eq!(error.code(), if query_error { 1317 } else { 1105 });
            assert!(group.workers.is_empty());
            assert!(group.receiver.is_none());
            assert!(group.next_batch().unwrap().is_none());
        }
    }
}
