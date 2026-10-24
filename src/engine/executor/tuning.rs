//! Statement-stable executor tuning values.

use crate::util::env::is_disabled as boolean_is_disabled;
use std::cell::RefCell;
#[cfg(not(test))]
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub(crate) struct ExecutorTuningSnapshot {
    pub(crate) stream_batch_rows: usize,
    pub(crate) stream_channel_batches: usize,
    pub(crate) sort_run_bytes: Option<usize>,
    pub(crate) sort_merge_fanin: usize,
    pub(crate) hash_join_bloom_adaptive: bool,
    pub(crate) scheduler_parallelism: usize,
}

impl ExecutorTuningSnapshot {
    pub(crate) fn capture() -> Self {
        fn load() -> ExecutorTuningSnapshot {
            ExecutorTuningSnapshot {
                stream_batch_rows: positive_usize("MYDB_STREAM_BATCH_ROWS").unwrap_or(64),
                stream_channel_batches: positive_usize("MYDB_STREAM_CHANNEL_BATCHES").unwrap_or(4),
                sort_run_bytes: positive_usize("MYDB_SORT_RUN_BYTES")
                    .filter(|value| *value >= 1024 * 1024),
                sort_merge_fanin: positive_usize("MYDB_SORT_MERGE_FANIN")
                    .unwrap_or(32)
                    .clamp(2, 128),
                hash_join_bloom_adaptive: !boolean_is_disabled("MYDB_HASH_JOIN_BLOOM_ADAPTIVE"),
                scheduler_parallelism: crate::planner::default_parallel_degree() as usize,
            }
        }
        #[cfg(not(test))]
        {
            // These are process-start configuration knobs. Parsing them for
            // every statement serializes concurrent executors on libc's
            // environment lock even when no knob is set.
            static SNAPSHOT: OnceLock<ExecutorTuningSnapshot> = OnceLock::new();
            SNAPSHOT.get_or_init(load).clone()
        }
        #[cfg(test)]
        {
            // Unit tests deliberately scope environment overrides inside one
            // process, so retain dynamic capture for their semantic oracle.
            load()
        }
    }

    pub(crate) fn capture_for_session(parallelism: u64) -> Self {
        let mut snapshot = Self::capture();
        snapshot.scheduler_parallelism = parallelism.max(1) as usize;
        snapshot
    }
}

fn positive_usize(name: &str) -> Option<usize> {
    crate::util::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

thread_local! {
    static CURRENT: RefCell<Option<ExecutorTuningSnapshot>> = const { RefCell::new(None) };
}

pub(crate) struct ExecutorTuningScope {
    previous: Option<ExecutorTuningSnapshot>,
}

impl Drop for ExecutorTuningScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| *slot.borrow_mut() = self.previous.take());
    }
}

pub(crate) fn enter(snapshot: ExecutorTuningSnapshot) -> ExecutorTuningScope {
    let previous = CURRENT.with(|slot| slot.borrow_mut().replace(snapshot));
    ExecutorTuningScope { previous }
}

pub(crate) fn enter_if_absent() -> Option<ExecutorTuningScope> {
    let present = CURRENT.with(|slot| slot.borrow().is_some());
    (!present).then(|| enter(ExecutorTuningSnapshot::capture()))
}

pub(crate) fn current_or_capture() -> ExecutorTuningSnapshot {
    CURRENT
        .with(|slot| slot.borrow().clone())
        .unwrap_or_else(ExecutorTuningSnapshot::capture)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_scope_keeps_statement_snapshot_stable() {
        let first = ExecutorTuningSnapshot {
            stream_batch_rows: 17,
            stream_channel_batches: 3,
            sort_run_bytes: Some(4096),
            sort_merge_fanin: 7,
            hash_join_bloom_adaptive: false,
            scheduler_parallelism: 5,
        };
        let _scope = enter(first.clone());
        assert_eq!(current_or_capture().stream_batch_rows, 17);
        assert!(enter_if_absent().is_none());
        assert_eq!(current_or_capture().sort_merge_fanin, 7);
        assert_eq!(current_or_capture().scheduler_parallelism, 5);
    }
}
